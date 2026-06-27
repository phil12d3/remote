#include "common.hpp"

#include <cerrno>
#include <csignal>
#include <deque>
#include <fstream>
#include <iostream>
#include <poll.h>
#include <thread>
#include <sys/wait.h>
#include <unistd.h>

namespace remote {
namespace {

struct UploadedFile {
  std::string relpath;
  std::vector<unsigned char> content;
};

struct TaskRuntime {
  std::string stage;
  std::string task;
  std::filesystem::path workdir;
  std::vector<std::string> argv;
  std::map<std::string, std::vector<unsigned char>> uploads;
  pid_t pid = -1;
};

std::vector<std::string> parse_message(const std::string &line) {
  auto raw = split_fields(line);
  std::vector<std::string> out;
  out.reserve(raw.size());
  for (const auto &field : raw) {
    out.push_back(unescape_field(field));
  }
  return out;
}

class AgentServer {
 public:
  AgentServer(std::string node_name,
              std::string token,
              std::filesystem::path root_dir,
              TlsConfig cfg)
      : node_name_(std::move(node_name)),
        token_(std::move(token)),
        root_dir_(std::move(root_dir)),
        server_(std::move(cfg)) {}

  void run() {
    for (;;) {
      auto sock = server_.accept_client();
      try {
        auto stream = std::make_unique<LineStream>(sock);
        auto hello = stream->read_line();
        if (!hello) {
          continue;
        }
        auto fields = parse_message(*hello);
        if (fields.size() < 3 || fields[0] != "HELLO") {
          throw std::runtime_error("unexpected handshake message");
        }
        if (fields[2] != token_) {
          throw std::runtime_error("authentication failed");
        }
        stream->write_fields("WELCOME", {node_name_});
        receive_loop(std::move(stream));
      } catch (const std::exception &) {
        continue;
      }
    }
  }

 private:
  void send(LineStream &stream, const std::string &type, const std::vector<std::string> &fields) {
    stream.write_fields(type, fields);
  }

  void send_log(LineStream &stream, const std::string &stage, const std::string &task, const std::string &chan, const std::string &text) {
    send(stream, "LOG", {stage, task, chan, text});
  }

  void send_progress(LineStream &stream, const std::string &stage, const std::string &task, int percent, const std::string &text) {
    send(stream, "PROGRESS", {stage, task, std::to_string(percent), text});
  }

  void send_state(LineStream &stream, const std::string &stage, const std::string &task, const std::string &status, const std::string &detail) {
    send(stream, "STATE", {stage, task, status, detail});
  }

  void send_exit(LineStream &stream, const std::string &stage, const std::string &task, int code, const std::string &detail) {
    send(stream, "EXIT", {stage, task, std::to_string(code), detail});
  }

  void apply_upload(const std::string &stage, const std::string &task, const std::string &relpath, const std::string &payload_b64) {
    pending_uploads_[stage + "/" + task].push_back({relpath, base64_decode(payload_b64)});
  }

  void prepare_workdir(TaskRuntime &task) {
    task.workdir = root_dir_ / task.stage / task.task;
    std::filesystem::create_directories(task.workdir);
    for (const auto &[relpath, data] : task.uploads) {
      write_binary_file(task.workdir / relpath, data);
    }
  }

  void flush_lines(LineStream &stream, const std::string &stage, const std::string &task, const std::string &stream_name) {
    auto &buffer = buffers_[stream_name];
    for (;;) {
      auto pos = buffer.find('\n');
      if (pos == std::string::npos) {
        break;
      }
      std::string line = buffer.substr(0, pos);
      buffer.erase(0, pos + 1);
      if (line.rfind("::progress::", 0) == 0) {
        std::string rest = line.substr(std::string("::progress::").size());
        auto sep = rest.find("::");
        if (sep != std::string::npos) {
          int pct = std::stoi(rest.substr(0, sep));
          std::string text = rest.substr(sep + 2);
          send_progress(stream, stage, task, pct, text);
          continue;
        }
      }
      send_log(stream, stage, task, stream_name, line);
    }
  }

  void run_command(LineStream &stream, TaskRuntime &task) {
    int stdout_pipe[2];
    int stderr_pipe[2];
    if (::pipe(stdout_pipe) < 0 || ::pipe(stderr_pipe) < 0) {
      throw std::runtime_error("pipe failed");
    }

    pid_t pid = ::fork();
    if (pid < 0) {
      throw std::runtime_error("fork failed");
    }
    if (pid == 0) {
      ::setsid();
      ::dup2(stdout_pipe[1], STDOUT_FILENO);
      ::dup2(stderr_pipe[1], STDERR_FILENO);
      ::close(stdout_pipe[0]);
      ::close(stdout_pipe[1]);
      ::close(stderr_pipe[0]);
      ::close(stderr_pipe[1]);
      ::chdir(task.workdir.c_str());

      std::vector<char *> argv;
      argv.reserve(task.argv.size() + 1);
      for (auto &arg : task.argv) {
        argv.push_back(const_cast<char *>(arg.c_str()));
      }
      argv.push_back(nullptr);
      ::execvp(argv[0], argv.data());
      _exit(127);
    }

    task.pid = pid;
    {
      std::lock_guard<std::mutex> lock(task_mutex_);
      current_pid_ = pid;
    }
    ::close(stdout_pipe[1]);
    ::close(stderr_pipe[1]);
    send_state(stream, task.stage, task.task, "running", "pid=" + std::to_string(pid));

    auto pump_fd = [&](int fd, const std::string &chan) {
      char buf[4096];
      ssize_t rc = ::read(fd, buf, sizeof(buf));
      if (rc <= 0) {
        return false;
      }
      buffers_[chan].append(buf, static_cast<std::size_t>(rc));
      flush_lines(stream, task.stage, task.task, chan);
      return true;
    };

    int status = 0;
    for (;;) {
      struct pollfd pfds[2];
      pfds[0].fd = stdout_pipe[0];
      pfds[0].events = POLLIN;
      pfds[1].fd = stderr_pipe[0];
      pfds[1].events = POLLIN;

      int rc = ::poll(pfds, 2, 200);
      if (rc < 0) {
        if (errno == EINTR) {
          continue;
        }
        throw std::runtime_error("poll failed");
      }
      if (pfds[0].revents & POLLIN) {
        pump_fd(stdout_pipe[0], "stdout");
      }
      if (pfds[1].revents & POLLIN) {
        pump_fd(stderr_pipe[0], "stderr");
      }

      pid_t wait_rc = ::waitpid(pid, &status, WNOHANG);
      if (wait_rc == pid) {
        break;
      }
      if (wait_rc < 0) {
        throw std::runtime_error("waitpid failed");
      }
    }

    pump_fd(stdout_pipe[0], "stdout");
    pump_fd(stderr_pipe[0], "stderr");
    ::close(stdout_pipe[0]);
    ::close(stderr_pipe[0]);

    int exit_code = 0;
    if (WIFEXITED(status)) {
      exit_code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
      exit_code = 128 + WTERMSIG(status);
    }
    send_exit(stream, task.stage, task.task, exit_code, exit_code == 0 ? "completed" : "failed");
    {
      std::lock_guard<std::mutex> lock(task_mutex_);
      current_pid_.reset();
    }
  }

  void execute_task(LineStream &stream, TaskRuntime task) {
    prepare_workdir(task);
    send_state(stream, task.stage, task.task, "preparing", task.workdir.string());
    run_command(stream, task);
  }

  void receive_loop(std::unique_ptr<LineStream> stream) {
    std::thread worker;
    try {
      for (;;) {
        auto line = stream->read_line();
        if (!line) {
          break;
        }
        auto fields = parse_message(*line);
        if (fields.empty()) {
          continue;
        }
        const std::string type = fields[0];
        if (type == "UPLOAD") {
          if (fields.size() < 5) {
            throw std::runtime_error("malformed UPLOAD");
          }
          apply_upload(fields[1], fields[2], fields[3], fields[4]);
          continue;
        }
        if (type == "CONTROL") {
          if (fields.size() < 4) {
            continue;
          }
          std::lock_guard<std::mutex> lock(task_mutex_);
          if (!current_pid_) {
            continue;
          }
          if (fields[3] == "pause") {
            ::kill(-*current_pid_, SIGSTOP);
          } else if (fields[3] == "resume") {
            ::kill(-*current_pid_, SIGCONT);
          } else if (fields[3] == "kill") {
            ::kill(-*current_pid_, SIGTERM);
          }
          continue;
        }
        if (type == "START") {
          if (fields.size() < 5) {
            throw std::runtime_error("malformed START");
          }
          TaskRuntime task;
          task.stage = fields[1];
          task.task = fields[2];
          task.workdir = root_dir_ / fields[3];
          int argc = std::stoi(fields[4]);
          if (fields.size() < 5 + static_cast<std::size_t>(argc)) {
            throw std::runtime_error("START missing argv");
          }
          for (int i = 0; i < argc; ++i) {
            task.argv.push_back(fields[5 + i]);
          }
          {
            auto key = task.stage + "/" + task.task;
            auto it = pending_uploads_.find(key);
            if (it != pending_uploads_.end()) {
              for (auto &file : it->second) {
                task.uploads[file.relpath] = file.content;
              }
              pending_uploads_.erase(it);
            }
          }
          if (worker.joinable()) {
            worker.join();
          }
          worker = std::thread([this, stream_ptr = stream.get(), task = std::move(task)]() mutable {
            try {
              execute_task(*stream_ptr, std::move(task));
            } catch (const std::exception &ex) {
              send(*stream_ptr, "ERROR", {ex.what()});
            }
          });
          continue;
        }
      }
    } catch (const std::exception &) {
    }

    if (worker.joinable()) {
      worker.join();
    }
  }

  std::string node_name_;
  std::string token_;
  std::filesystem::path root_dir_;
  TlsServer server_;
  std::mutex task_mutex_;
  std::map<std::string, std::vector<UploadedFile>> pending_uploads_;
  std::map<std::string, std::string> buffers_;
  std::optional<pid_t> current_pid_;
};

}  // namespace
}  // namespace remote

int main(int argc, char **argv) {
  using namespace remote;
  std::string node_name = "node";
  std::string token = "remote-token";
  std::filesystem::path root_dir = "rc-agent-root";
  std::string cert_file;
  std::string key_file;
  std::string ca_file;
  uint16_t port = 8443;
  bool no_cert = false;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    auto next = [&]() -> std::string {
      if (i + 1 >= argc) {
        throw std::runtime_error("missing value for " + arg);
      }
      return argv[++i];
    };
    if (arg == "--name") {
      node_name = next();
    } else if (arg == "--token") {
      token = next();
    } else if (arg == "--root") {
      root_dir = next();
    } else if (arg == "--cert") {
      cert_file = next();
    } else if (arg == "--key") {
      key_file = next();
    } else if (arg == "--ca") {
      ca_file = next();
    } else if (arg == "--port") {
      port = static_cast<uint16_t>(std::stoi(next()));
    } else if (arg == "--no-cert") {
      no_cert = true;
    } else {
      throw std::runtime_error("unknown argument: " + arg);
    }
  }

  if (!no_cert && (cert_file.empty() || key_file.empty())) {
    std::cerr << "usage: rc-agent --port PORT [--no-cert | --cert server.crt --key server.key --ca ca.crt] [--token TOKEN] [--name NODE] [--root DIR]\n";
    return 2;
  }

  std::filesystem::create_directories(root_dir);
  TlsConfig cfg;
  cfg.no_tls = no_cert;
  if (!no_cert) {
    cfg.cert_file = cert_file;
    cfg.key_file = key_file;
    cfg.ca_file = ca_file;
  }
  cfg.listen_port = port;
  AgentServer agent(node_name, token, root_dir, std::move(cfg));
  agent.run();
  return 0;
}
