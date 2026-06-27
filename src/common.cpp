#include "common.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <cctype>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <iterator>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace remote {
namespace {

constexpr char kBase64Table[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

std::string ssl_error_string(SSL *ssl, int rc) {
  int err = SSL_get_error(ssl, rc);
  switch (err) {
    case SSL_ERROR_ZERO_RETURN:
      return "TLS connection closed";
    case SSL_ERROR_WANT_READ:
    case SSL_ERROR_WANT_WRITE:
      return "TLS operation would block";
    default: {
      unsigned long code = ERR_get_error();
      if (code == 0) {
        return "TLS failure";
      }
      char buf[256];
      ERR_error_string_n(code, buf, sizeof(buf));
      return buf;
    }
  }
}

void throw_ssl(const std::string &what) {
  unsigned long code = ERR_get_error();
  if (code == 0) {
    throw std::runtime_error(what);
  }
  char buf[256];
  ERR_error_string_n(code, buf, sizeof(buf));
  throw std::runtime_error(what + ": " + buf);
}

int create_server_socket(uint16_t port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error("socket() failed");
  }
  int yes = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port);
  if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    ::close(fd);
    throw std::runtime_error("bind() failed");
  }
  if (::listen(fd, 64) < 0) {
    ::close(fd);
    throw std::runtime_error("listen() failed");
  }
  return fd;
}

int connect_socket(const std::string &host, uint16_t port) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  addrinfo *result = nullptr;
  std::string port_str = std::to_string(port);
  if (::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &result) != 0) {
    throw std::runtime_error("getaddrinfo() failed");
  }

  int fd = -1;
  for (addrinfo *ai = result; ai != nullptr; ai = ai->ai_next) {
    fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) {
      continue;
    }
    if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) {
      ::freeaddrinfo(result);
      return fd;
    }
    ::close(fd);
    fd = -1;
  }
  ::freeaddrinfo(result);
  throw std::runtime_error("connect() failed");
}

bool write_full_ssl(SSL *ssl, const char *buf, std::size_t len) {
  std::size_t offset = 0;
  while (offset < len) {
    int rc = SSL_write(ssl, buf + offset, static_cast<int>(len - offset));
    if (rc <= 0) {
      if (SSL_get_error(ssl, rc) == SSL_ERROR_WANT_READ ||
          SSL_get_error(ssl, rc) == SSL_ERROR_WANT_WRITE) {
        continue;
      }
      throw std::runtime_error(ssl_error_string(ssl, rc));
    }
    offset += static_cast<std::size_t>(rc);
  }
  return true;
}

bool write_full_fd(int fd, const char *buf, std::size_t len) {
  std::size_t offset = 0;
  while (offset < len) {
    ssize_t rc = ::write(fd, buf + offset, len - offset);
    if (rc < 0) {
      if (errno == EINTR) {
        continue;
      }
      throw std::runtime_error("write() failed");
    }
    offset += static_cast<std::size_t>(rc);
  }
  return true;
}

std::string trim_newline(std::string s) {
  while (!s.empty() && (s.back() == '\n' || s.back() == '\r')) {
    s.pop_back();
  }
  return s;
}

}  // namespace

std::string now_string() {
  using namespace std::chrono;
  auto now = system_clock::now();
  auto tt = system_clock::to_time_t(now);
  std::tm tm{};
  localtime_r(&tt, &tm);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
  return buf;
}

std::string shell_join(const std::vector<std::string> &args) {
  std::ostringstream out;
  for (std::size_t i = 0; i < args.size(); ++i) {
    if (i > 0) {
      out << ' ';
    }
    bool needs_quote = args[i].find_first_of(" \t\"'\\") != std::string::npos;
    if (!needs_quote) {
      out << args[i];
      continue;
    }
    out << '"';
    for (char ch : args[i]) {
      if (ch == '"' || ch == '\\') {
        out << '\\';
      }
      out << ch;
    }
    out << '"';
  }
  return out.str();
}

std::vector<std::string> split_args(const std::string &line) {
  std::vector<std::string> out;
  std::string current;
  bool in_quotes = false;
  bool escape = false;
  for (char ch : line) {
    if (escape) {
      current.push_back(ch);
      escape = false;
      continue;
    }
    if (ch == '\\') {
      escape = true;
      continue;
    }
    if (ch == '"') {
      in_quotes = !in_quotes;
      continue;
    }
    if (!in_quotes && std::isspace(static_cast<unsigned char>(ch))) {
      if (!current.empty()) {
        out.push_back(current);
        current.clear();
      }
      continue;
    }
    current.push_back(ch);
  }
  if (!current.empty()) {
    out.push_back(current);
  }
  return out;
}

std::vector<std::string> split_fields(const std::string &line) {
  std::vector<std::string> out;
  std::string current;
  for (char ch : line) {
    if (ch == '\t') {
      out.push_back(current);
      current.clear();
      continue;
    }
    current.push_back(ch);
  }
  out.push_back(current);
  return out;
}

std::string escape_field(const std::string &value) {
  std::ostringstream out;
  for (unsigned char c : value) {
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9') || c == '.' || c == '-' || c == '_' ||
        c == '/' || c == ':' || c == '@' || c == '+' || c == '=') {
      out << static_cast<char>(c);
    } else {
      out << '%';
      out << std::hex << std::uppercase;
      out.width(2);
      out.fill('0');
      out << static_cast<int>(c);
      out << std::nouppercase << std::dec;
    }
  }
  return out.str();
}

std::string unescape_field(const std::string &value) {
  std::string out;
  for (std::size_t i = 0; i < value.size(); ++i) {
    if (value[i] == '%' && i + 2 < value.size()) {
      int hi = std::isdigit(static_cast<unsigned char>(value[i + 1]))
                   ? value[i + 1] - '0'
                   : std::toupper(static_cast<unsigned char>(value[i + 1])) - 'A' + 10;
      int lo = std::isdigit(static_cast<unsigned char>(value[i + 2]))
                   ? value[i + 2] - '0'
                   : std::toupper(static_cast<unsigned char>(value[i + 2])) - 'A' + 10;
      out.push_back(static_cast<char>((hi << 4) | lo));
      i += 2;
      continue;
    }
    out.push_back(value[i]);
  }
  return out;
}

std::string base64_encode(const std::vector<unsigned char> &data) {
  std::string out;
  out.reserve(((data.size() + 2) / 3) * 4);
  int val = 0;
  int valb = -6;
  for (unsigned char c : data) {
    val = (val << 8) | c;
    valb += 8;
    while (valb >= 0) {
      out.push_back(kBase64Table[(val >> valb) & 0x3F]);
      valb -= 6;
    }
  }
  if (valb > -6) {
    out.push_back(kBase64Table[((val << 8) >> (valb + 8)) & 0x3F]);
  }
  while (out.size() % 4 != 0) {
    out.push_back('=');
  }
  return out;
}

std::vector<unsigned char> base64_decode(const std::string &text) {
  std::vector<int> T(256, -1);
  for (int i = 0; i < 64; i++) {
    T[static_cast<unsigned char>(kBase64Table[i])] = i;
  }
  std::vector<unsigned char> out;
  int val = 0;
  int valb = -8;
  for (unsigned char c : text) {
    if (T[c] == -1) {
      if (c == '=') {
        break;
      }
      continue;
    }
    val = (val << 6) + T[c];
    valb += 6;
    if (valb >= 0) {
      out.push_back(static_cast<unsigned char>((val >> valb) & 0xFF));
      valb -= 8;
    }
  }
  return out;
}

std::string read_text_file(const std::filesystem::path &path) {
  std::ifstream in(path);
  if (!in) {
    throw std::runtime_error("unable to open text file: " + path.string());
  }
  std::ostringstream ss;
  ss << in.rdbuf();
  return ss.str();
}

std::vector<unsigned char> read_binary_file(const std::filesystem::path &path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    throw std::runtime_error("unable to open file: " + path.string());
  }
  std::vector<unsigned char> data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  return data;
}

void write_binary_file(const std::filesystem::path &path, const std::vector<unsigned char> &data) {
  ensure_parent_dir(path);
  std::ofstream out(path, std::ios::binary);
  if (!out) {
    throw std::runtime_error("unable to write file: " + path.string());
  }
  out.write(reinterpret_cast<const char *>(data.data()), static_cast<std::streamsize>(data.size()));
}

void ensure_parent_dir(const std::filesystem::path &path) {
  auto parent = path.parent_path();
  if (!parent.empty()) {
    std::filesystem::create_directories(parent);
  }
}

std::string basename_string(const std::filesystem::path &path) {
  return path.filename().string();
}

std::string sanitize_name(const std::string &value) {
  std::string out;
  for (char c : value) {
    if (std::isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_' || c == '.') {
      out.push_back(c);
    } else {
      out.push_back('_');
    }
  }
  if (out.empty()) {
    out = "node";
  }
  return out;
}

LineStream::LineStream(SocketHandle handle) : handle_(handle) {}

LineStream::~LineStream() {
  if (handle_.ssl != nullptr) {
    SSL_shutdown(handle_.ssl);
    SSL_free(handle_.ssl);
  }
  if (handle_.fd >= 0) {
    ::close(handle_.fd);
  }
}

std::optional<std::string> LineStream::read_line() {
  for (;;) {
    auto pos = read_buffer_.find('\n');
    if (pos != std::string::npos) {
      std::string line = read_buffer_.substr(0, pos);
      read_buffer_.erase(0, pos + 1);
      return trim_newline(line);
    }
    char buf[4096];
    if (handle_.ssl != nullptr) {
      int rc = SSL_read(handle_.ssl, buf, sizeof(buf));
      if (rc <= 0) {
        int err = SSL_get_error(handle_.ssl, rc);
        if (err == SSL_ERROR_ZERO_RETURN) {
          return std::nullopt;
        }
        if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
          continue;
        }
        throw std::runtime_error(ssl_error_string(handle_.ssl, rc));
      }
      read_buffer_.append(buf, rc);
    } else {
      ssize_t rc = ::read(handle_.fd, buf, sizeof(buf));
      if (rc < 0) {
        if (errno == EINTR) {
          continue;
        }
        throw std::runtime_error("read() failed");
      }
      if (rc == 0) {
        return std::nullopt;
      }
      read_buffer_.append(buf, static_cast<std::size_t>(rc));
    }
  }
}

bool LineStream::write_line(const std::string &line) {
  std::lock_guard<std::mutex> lock(write_mutex_);
  std::string payload = line;
  payload.push_back('\n');
  if (handle_.ssl != nullptr) {
    return write_full_ssl(handle_.ssl, payload.data(), payload.size());
  }
  return write_full_fd(handle_.fd, payload.data(), payload.size());
}

bool LineStream::write_fields(const std::string &type, const std::vector<std::string> &fields) {
  std::ostringstream line;
  line << type;
  for (const auto &field : fields) {
    line << '\t' << escape_field(field);
  }
  return write_line(line.str());
}

SocketHandle LineStream::release() {
  SocketHandle out = handle_;
  handle_.fd = -1;
  handle_.ssl = nullptr;
  return out;
}

bool LineStream::valid() const {
  return handle_.ssl != nullptr && handle_.fd >= 0;
}

TlsServer::TlsServer(TlsConfig cfg) : cfg_(std::move(cfg)) {
  SSL_library_init();
  SSL_load_error_strings();
  OpenSSL_add_all_algorithms();

  if (!cfg_.no_tls) {
    ctx_ = SSL_CTX_new(TLS_server_method());
    if (!ctx_) {
      throw_ssl("SSL_CTX_new");
    }
    if (SSL_CTX_use_certificate_file(ctx_, cfg_.cert_file.c_str(), SSL_FILETYPE_PEM) != 1) {
      throw_ssl("loading server certificate");
    }
    if (SSL_CTX_use_PrivateKey_file(ctx_, cfg_.key_file.c_str(), SSL_FILETYPE_PEM) != 1) {
      throw_ssl("loading server private key");
    }
    if (SSL_CTX_check_private_key(ctx_) != 1) {
      throw std::runtime_error("server certificate and private key do not match");
    }
    if (!cfg_.ca_file.empty()) {
      if (SSL_CTX_load_verify_locations(ctx_, cfg_.ca_file.c_str(), nullptr) != 1) {
        throw_ssl("loading client CA file");
      }
      SSL_CTX_set_verify(ctx_, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
    }
  }

  listen_fd_ = create_server_socket(cfg_.listen_port);
}

TlsServer::~TlsServer() {
  if (listen_fd_ >= 0) {
    ::close(listen_fd_);
  }
  if (ctx_) {
    SSL_CTX_free(ctx_);
  }
}

int TlsServer::listen_fd() const { return listen_fd_; }

SocketHandle TlsServer::accept_client() {
  sockaddr_storage addr{};
  socklen_t len = sizeof(addr);
  int fd = ::accept(listen_fd_, reinterpret_cast<sockaddr *>(&addr), &len);
  if (fd < 0) {
    throw std::runtime_error("accept() failed");
  }
  SSL *ssl = nullptr;
  if (!cfg_.no_tls) {
    ssl = SSL_new(ctx_);
    if (!ssl) {
      ::close(fd);
      throw_ssl("SSL_new");
    }
    SSL_set_fd(ssl, fd);
    if (SSL_accept(ssl) != 1) {
      std::string error = ssl_error_string(ssl, 0);
      SSL_free(ssl);
      ::close(fd);
      throw std::runtime_error("TLS handshake failed: " + error);
    }
  }
  SocketHandle handle;
  handle.fd = fd;
  handle.ssl = ssl;
  return handle;
}

TlsClient::TlsClient(TlsConfig cfg) : cfg_(std::move(cfg)) {
  SSL_library_init();
  SSL_load_error_strings();
  OpenSSL_add_all_algorithms();

  if (!cfg_.no_tls) {
    ctx_ = SSL_CTX_new(TLS_client_method());
    if (!ctx_) {
      throw_ssl("SSL_CTX_new");
    }
    if (!cfg_.ca_file.empty()) {
      if (SSL_CTX_load_verify_locations(ctx_, cfg_.ca_file.c_str(), nullptr) != 1) {
        throw_ssl("loading CA file");
      }
      SSL_CTX_set_verify(ctx_, SSL_VERIFY_PEER, nullptr);
    }
    if (!cfg_.client_cert_file.empty()) {
      if (SSL_CTX_use_certificate_file(ctx_, cfg_.client_cert_file.c_str(), SSL_FILETYPE_PEM) != 1) {
        throw_ssl("loading client certificate");
      }
    }
    if (!cfg_.client_key_file.empty()) {
      if (SSL_CTX_use_PrivateKey_file(ctx_, cfg_.client_key_file.c_str(), SSL_FILETYPE_PEM) != 1) {
        throw_ssl("loading client private key");
      }
    }
    if (!cfg_.client_cert_file.empty() || !cfg_.client_key_file.empty()) {
      if (SSL_CTX_check_private_key(ctx_) != 1) {
        throw std::runtime_error("client certificate and private key do not match");
      }
    }
  }
}

TlsClient::~TlsClient() {
  if (ctx_) {
    SSL_CTX_free(ctx_);
  }
}

SocketHandle TlsClient::connect_to(const std::string &host, uint16_t port) {
  int fd = connect_socket(host, port);
  SSL *ssl = nullptr;
  if (!cfg_.no_tls) {
    ssl = SSL_new(ctx_);
    if (!ssl) {
      ::close(fd);
      throw_ssl("SSL_new");
    }
    SSL_set_fd(ssl, fd);
    if (!cfg_.server_name.empty()) {
      SSL_set_tlsext_host_name(ssl, cfg_.server_name.c_str());
#if OPENSSL_VERSION_NUMBER >= 0x10002000L
      SSL_set1_host(ssl, cfg_.server_name.c_str());
#endif
    }
    if (SSL_connect(ssl) != 1) {
      std::string error = ssl_error_string(ssl, 0);
      SSL_free(ssl);
      ::close(fd);
      throw std::runtime_error("TLS connection failed: " + error);
    }
    long verify_result = SSL_get_verify_result(ssl);
    if (verify_result != X509_V_OK) {
      std::string msg = X509_verify_cert_error_string(verify_result);
      SSL_free(ssl);
      ::close(fd);
      throw std::runtime_error("server certificate verification failed: " + msg);
    }
  }
  SocketHandle handle;
  handle.fd = fd;
  handle.ssl = ssl;
  return handle;
}

}  // namespace remote
