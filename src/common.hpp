#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <openssl/ssl.h>

namespace remote {

struct TlsConfig {
  bool no_tls = false;
  std::string cert_file;
  std::string key_file;
  std::string ca_file;
  std::string client_cert_file;
  std::string client_key_file;
  std::string server_name;
  uint16_t listen_port = 0;
};

struct SocketHandle {
  int fd = -1;
  SSL *ssl = nullptr;
};

std::string now_string();
std::string shell_join(const std::vector<std::string> &args);
std::vector<std::string> split_args(const std::string &line);
std::vector<std::string> split_fields(const std::string &line);
std::string escape_field(const std::string &value);
std::string unescape_field(const std::string &value);
std::string base64_encode(const std::vector<unsigned char> &data);
std::vector<unsigned char> base64_decode(const std::string &text);
std::string read_text_file(const std::filesystem::path &path);
std::vector<unsigned char> read_binary_file(const std::filesystem::path &path);
void write_binary_file(const std::filesystem::path &path, const std::vector<unsigned char> &data);
void ensure_parent_dir(const std::filesystem::path &path);
std::string basename_string(const std::filesystem::path &path);
std::string sanitize_name(const std::string &value);

class LineStream {
 public:
  explicit LineStream(SocketHandle handle);
  ~LineStream();

  LineStream(const LineStream &) = delete;
  LineStream &operator=(const LineStream &) = delete;

  std::optional<std::string> read_line();
  bool write_line(const std::string &line);
  bool write_fields(const std::string &type, const std::vector<std::string> &fields);
  SocketHandle release();
  bool valid() const;

 private:
  SocketHandle handle_;
  std::string read_buffer_;
  std::mutex write_mutex_;
};

class TlsServer {
 public:
  explicit TlsServer(TlsConfig cfg);
  ~TlsServer();

  TlsServer(const TlsServer &) = delete;
  TlsServer &operator=(const TlsServer &) = delete;

  int listen_fd() const;
  SocketHandle accept_client();

 private:
  TlsConfig cfg_;
  SSL_CTX *ctx_ = nullptr;
  int listen_fd_ = -1;
};

class TlsClient {
 public:
  explicit TlsClient(TlsConfig cfg);
  ~TlsClient();

  TlsClient(const TlsClient &) = delete;
  TlsClient &operator=(const TlsClient &) = delete;

  SocketHandle connect_to(const std::string &host, uint16_t port);

 private:
  TlsConfig cfg_;
  SSL_CTX *ctx_ = nullptr;
};

}  // namespace remote
