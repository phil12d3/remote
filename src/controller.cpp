#include "common.hpp"
#include "plan.hpp"

#include <chrono>
#include <deque>
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>

namespace remote {
namespace {

struct AgentEndpoint {
  std::string host;
  uint16_t port = 0;
};

struct NodeSession {
  std::string name;
  std::string endpoint;
  std::unique_ptr<LineStream> stream;
  std::mutex send_mutex;
  std::ofstream log;
  bool busy = false;
  bool connected = true;
  bool authenticated = false;
  std::string current_task;
  std::string current_stage;
  int current_exit = -1;
};

struct ControllerState {
  std::mutex mutex;
  std::condition_variable cv;
  std::map<std::string, std::shared_ptr<NodeSession>> nodes;
  bool running = true;
  bool stage_failed = false;
  std::string token;
  std::string controller_name = "rc";
  std::filesystem::path log_dir;
};

AgentEndpoint parse_endpoint(const std::string &text) {
  auto pos = text.rfind(':');
  if (pos == std::string::npos || pos == 0 || pos + 1 >= text.size()) {
    throw std::runtime_error("agent endpoint must be host:port");
  }
  AgentEndpoint endpoint;
  endpoint.host = text.substr(0, pos);
  endpoint.port = static_cast<uint16_t>(std::stoi(text.substr(pos + 1)));
  return endpoint;
}

std::vector<AgentEndpoint> load_agents_file(const std::filesystem::path &path) {
  std::ifstream in(path);
  if (!in) {
    throw std::runtime_error("unable to open agents file: " + path.string());
  }

  std::vector<AgentEndpoint> endpoints;
  std::string line;
  while (std::getline(in, line)) {
    auto hash = line.find('#');
    if (hash != std::string::npos) {
      line = line.substr(0, hash);
    }
    auto tokens = split_args(line);
    if (tokens.empty()) {
      continue;
    }
    if (tokens.size() != 1) {
      throw std::runtime_error("each agents file line must contain a single host:port entry");
    }
    endpoints.push_back(parse_endpoint(tokens[0]));
  }
  return endpoints;
}

std::vector<std::string> read_text_rows(const std::filesystem::path &path) {
  std::string text = read_text_file(path);
  std::vector<std::string> rows;
  std::string current;
  for (char ch : text) {
    current.push_back(ch);
    if (ch == '\n') {
      rows.push_back(current);
      current.clear();
    }
  }
  if (!current.empty()) {
    rows.push_back(current);
  }
  return rows;
}

std::vector<std::string> parse_message(const std::string &line) {
  auto raw = split_fields(line);
  std::vector<std::string> out;
  out.reserve(raw.size());
  for (const auto &field : raw) {
    out.push_back(unescape_field(field));
  }
  return out;
}

void log_line(NodeSession &node, const std::string &line) {
  if (node.log.is_open()) {
    node.log << now_string() << " " << line << '\n';
    node.log.flush();
  }
}

bool send_message(NodeSession &node, const std::string &type, const std::vector<std::string> &fields) {
  std::lock_guard<std::mutex> lock(node.send_mutex);
  return node.stream->write_fields(type, fields);
}

void dispatch_task(NodeSession &node,
                   const std::string &stage_name,
                   const std::string &task_name,
                   const std::string &workdir,
                   const std::vector<std::string> &argv,
                   const std::vector<std::pair<std::string, std::vector<unsigned char>>> &uploads) {
  for (const auto &upload : uploads) {
    send_message(node, "UPLOAD", {stage_name, task_name, upload.first, base64_encode(upload.second)});
  }
  std::vector<std::string> fields;
  fields.push_back(stage_name);
  fields.push_back(task_name);
  fields.push_back(workdir);
  fields.push_back(std::to_string(argv.size()));
  fields.insert(fields.end(), argv.begin(), argv.end());
  send_message(node, "START", fields);
}

void handle_message(ControllerState &state, const std::shared_ptr<NodeSession> &node, const std::string &line) {
  auto fields = parse_message(line);
  if (fields.empty()) {
    return;
  }

  const std::string type = fields[0];
  if (type == "LOG") {
    if (fields.size() < 5) {
      throw std::runtime_error("malformed LOG");
    }
    std::ostringstream ss;
    ss << '[' << fields[1] << '/' << fields[2] << "] " << fields[3] << ": " << fields[4];
    log_line(*node, ss.str());
    return;
  }
  if (type == "PROGRESS") {
    if (fields.size() < 5) {
      throw std::runtime_error("malformed PROGRESS");
    }
    std::ostringstream ss;
    ss << '[' << fields[1] << '/' << fields[2] << "] progress=" << fields[3] << "% " << fields[4];
    log_line(*node, ss.str());
    std::lock_guard<std::mutex> lock(state.mutex);
    state.cv.notify_all();
    return;
  }
  if (type == "STATE") {
    if (fields.size() < 5) {
      throw std::runtime_error("malformed STATE");
    }
    log_line(*node, "[" + fields[1] + "/" + fields[2] + "] " + fields[3] + " " + fields[4]);
    return;
  }
  if (type == "EXIT") {
    if (fields.size() < 5) {
      throw std::runtime_error("malformed EXIT");
    }
    int code = std::stoi(fields[3]);
    {
      std::lock_guard<std::mutex> lock(state.mutex);
      node->current_exit = code;
      if (code != 0) {
        state.stage_failed = true;
      }
      node->busy = false;
      node->current_task.clear();
      node->current_stage.clear();
      state.cv.notify_all();
    }
    log_line(*node, "[" + fields[1] + "/" + fields[2] + "] exit=" + fields[3] + " " + fields[4]);
    return;
  }
  if (type == "ERROR") {
    std::ostringstream ss;
    for (std::size_t i = 1; i < fields.size(); ++i) {
      if (i > 1) {
        ss << ' ';
      }
      ss << fields[i];
    }
    log_line(*node, "ERROR " + ss.str());
    {
      std::lock_guard<std::mutex> lock(state.mutex);
      state.stage_failed = true;
      state.cv.notify_all();
    }
    return;
  }
  log_line(*node, "unknown message from agent: " + line);
}

void node_reader(ControllerState &state, std::shared_ptr<NodeSession> node) {
  try {
    for (;;) {
      auto line = node->stream->read_line();
      if (!line) {
        break;
      }
      handle_message(state, node, *line);
    }
  } catch (const std::exception &ex) {
    if (node->authenticated) {
      log_line(*node, std::string("connection error: ") + ex.what());
    }
    if (node->busy) {
      std::lock_guard<std::mutex> lock(state.mutex);
      state.stage_failed = true;
      state.cv.notify_all();
    }
  }
  {
    std::lock_guard<std::mutex> lock(state.mutex);
    node->connected = false;
    node->busy = false;
    state.cv.notify_all();
  }
}

void send_task_payloads(NodeSession &node,
                        const StageSpec &stage,
                        const TaskSpec &task,
                        std::size_t task_index) {
  for (const auto &shared_file : stage.shared_files) {
    auto data = read_binary_file(shared_file);
    send_message(node, "UPLOAD", {stage.name, task.name, basename_string(shared_file), base64_encode(data)});
  }
  if (stage.has_split_file) {
    auto rows = read_text_rows(stage.split_file);
    std::size_t total = stage.tasks.size();
    std::size_t base = rows.size() / total;
    std::size_t remainder = rows.size() % total;
    std::size_t begin = task_index * base + std::min(task_index, remainder);
    std::size_t count = base + (task_index < remainder ? 1 : 0);
    std::string shard;
    for (std::size_t i = begin; i < begin + count; ++i) {
      shard += rows[i];
    }
    std::vector<unsigned char> shard_bytes(shard.begin(), shard.end());
    send_message(node,
                 "UPLOAD",
                 {stage.name, task.name, basename_string(stage.split_file) + ".part" + std::to_string(task_index),
                  base64_encode(shard_bytes)});
  }
}

void dispatch_queue_job(NodeSession &node,
                        const std::string &stage_name,
                        const TaskSpec &task,
                        const std::filesystem::path &source_file,
                        std::size_t job_index) {
  std::string job_name = sanitize_name(task.name + "-" + std::to_string(job_index) + "-" + basename_string(source_file));
  std::string remote_name = basename_string(source_file);
  std::vector<std::pair<std::string, std::vector<unsigned char>>> uploads;
  uploads.push_back({remote_name, read_binary_file(source_file)});
  std::vector<std::string> argv = task.argv;
  argv.push_back(remote_name);
  std::string workdir = stage_name + "/" + job_name;
  dispatch_task(node, stage_name, job_name, workdir, argv, uploads);
}

bool wait_for_idle_node(ControllerState &state) {
  std::unique_lock<std::mutex> lock(state.mutex);
  state.cv.wait(lock, [&] {
    if (state.stage_failed) {
      return true;
    }
    for (const auto &entry : state.nodes) {
      if (entry.second->authenticated && entry.second->connected && !entry.second->busy) {
        return true;
      }
    }
    return false;
  });
  return !state.stage_failed;
}

void interactive_loop(ControllerState &state) {
  std::string line;
  while (state.running && std::getline(std::cin, line)) {
    auto tokens = split_args(line);
    if (tokens.empty()) {
      continue;
    }
    if (tokens[0] == "status") {
      std::lock_guard<std::mutex> lock(state.mutex);
      std::cout << "nodes:\n";
      for (const auto &[name, node] : state.nodes) {
        std::cout << "  " << name << " connected=" << node->connected << " busy=" << node->busy
                  << " task=" << node->current_stage << "/" << node->current_task << "\n";
      }
      continue;
    }
    if ((tokens[0] == "pause" || tokens[0] == "resume" || tokens[0] == "kill") && tokens.size() >= 2) {
      std::shared_ptr<NodeSession> node;
      {
        std::lock_guard<std::mutex> lock(state.mutex);
        auto it = state.nodes.find(tokens[1]);
        if (it != state.nodes.end()) {
          node = it->second;
        }
      }
      if (!node) {
        std::cout << "unknown node: " << tokens[1] << "\n";
        continue;
      }
      if (!node->connected || node->current_task.empty()) {
        std::cout << "node is idle: " << tokens[1] << "\n";
        continue;
      }
      send_message(*node, "CONTROL", {node->current_stage, node->current_task, tokens[0]});
      std::cout << tokens[0] << " sent to " << tokens[1] << "\n";
      continue;
    }
    if (tokens[0] == "help") {
      std::cout << "status | pause <node> | resume <node> | kill <node>\n";
      continue;
    }
    std::cout << "unknown command\n";
  }
}

std::shared_ptr<NodeSession> pick_idle_node(ControllerState &state) {
  for (auto &[name, node] : state.nodes) {
    if (node->authenticated && node->connected && !node->busy) {
      return node;
    }
  }
  return nullptr;
}

void run_fixed_parallel_stage(ControllerState &state, const StageSpec &stage) {
  std::deque<std::size_t> pending;
  for (std::size_t i = 0; i < stage.tasks.size(); ++i) {
    pending.push_back(i);
  }

  while (true) {
    if (!wait_for_idle_node(state)) {
      return;
    }

    std::shared_ptr<NodeSession> node;
    std::size_t task_index = 0;
    {
      std::lock_guard<std::mutex> lock(state.mutex);
      node = pick_idle_node(state);
      if (node && !pending.empty()) {
        task_index = pending.front();
        pending.pop_front();
        node->busy = true;
        node->current_stage = stage.name;
        node->current_task = stage.tasks[task_index].name;
        node->current_exit = -1;
      } else {
        node = nullptr;
      }
    }

    if (node && task_index < stage.tasks.size()) {
      const auto &task = stage.tasks[task_index];
      send_task_payloads(*node, stage, task, task_index);
      dispatch_task(*node, stage.name, task.name, task.workdir, task.argv, {});
      log_line(*node, "task dispatched: " + stage.name + "/" + task.name + " => " + shell_join(task.argv));
      continue;
    }

    std::unique_lock<std::mutex> lock(state.mutex);
    if (pending.empty()) {
      bool any_busy = false;
      for (const auto &[name, n] : state.nodes) {
        if (n->authenticated && n->connected && n->busy) {
          any_busy = true;
          break;
        }
      }
      if (!any_busy) {
        break;
      }
    }
    state.cv.wait(lock);
    if (state.stage_failed) {
      return;
    }
  }
}

void run_queue_stage(ControllerState &state, const StageSpec &stage) {
  auto files = stage.queue_files;
  std::deque<std::filesystem::path> pending(files.begin(), files.end());
  std::size_t job_index = 0;

  while (true) {
    {
      std::lock_guard<std::mutex> lock(state.mutex);
      if (pending.empty()) {
        bool any_busy = false;
        for (const auto &[name, n] : state.nodes) {
          if (n->authenticated && n->connected && n->busy) {
            any_busy = true;
            break;
          }
        }
        if (!any_busy) {
          break;
        }
      }
    }

    if (!wait_for_idle_node(state)) {
      return;
    }

    std::shared_ptr<NodeSession> node;
    std::filesystem::path source_file;
    {
      std::lock_guard<std::mutex> lock(state.mutex);
      node = pick_idle_node(state);
      if (node && !pending.empty()) {
        source_file = pending.front();
        pending.pop_front();
        std::string job_name = sanitize_name(stage.tasks[0].name + "-" + std::to_string(job_index) + "-" +
                                             basename_string(source_file));
        node->busy = true;
        node->current_stage = stage.name;
        node->current_task = job_name;
        node->current_exit = -1;
      } else {
        node = nullptr;
      }
    }

    if (node && !source_file.empty()) {
      const auto &task = stage.tasks[0];
      dispatch_queue_job(*node, stage.name, task, source_file, job_index);
      log_line(*node, "queue dispatched: " + stage.name + "/" + task.name + " => " + source_file.string());
      ++job_index;
      continue;
    }

    std::unique_lock<std::mutex> lock(state.mutex);
    if (pending.empty()) {
      bool any_busy = false;
      for (const auto &[name, n] : state.nodes) {
        if (n->authenticated && n->connected && n->busy) {
          any_busy = true;
          break;
        }
      }
      if (!any_busy) {
        break;
      }
    }
    state.cv.wait(lock);
    if (state.stage_failed) {
      return;
    }
  }
}

void agent_connector(ControllerState &state, AgentEndpoint endpoint, TlsConfig client_cfg) {
  TlsClient client(std::move(client_cfg));
  while (state.running) {
    try {
      auto sock = client.connect_to(endpoint.host, endpoint.port);
      auto stream = std::make_unique<LineStream>(sock);

      {
        std::lock_guard<std::mutex> lock(state.mutex);
        state.cv.notify_all();
      }

      stream->write_fields("HELLO", {state.controller_name, state.token});
      auto welcome = stream->read_line();
      if (!welcome) {
        throw std::runtime_error("agent closed during handshake");
      }
      auto fields = parse_message(*welcome);
      if (fields.size() < 2 || fields[0] != "WELCOME") {
        throw std::runtime_error("unexpected handshake response: " + *welcome);
      }

      auto node = std::make_shared<NodeSession>();
      node->name = fields[1];
      node->endpoint = endpoint.host + ":" + std::to_string(endpoint.port);
      node->stream = std::move(stream);
      node->authenticated = true;
      node->connected = true;
      node->log.open(state.log_dir / (node->name + ".log"), std::ios::app);

      {
        std::lock_guard<std::mutex> lock(state.mutex);
        state.nodes[node->name] = node;
        state.cv.notify_all();
      }
      log_line(*node, "connected from " + node->endpoint);
      node_reader(state, node);
    } catch (const std::exception &ex) {
      (void)ex;
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
}

}  // namespace
}  // namespace remote

int main(int argc, char **argv) {
  using namespace remote;
  std::string plan_path;
  std::optional<std::string> cert_file;
  std::optional<std::string> key_file;
  std::optional<std::string> ca_file;
  std::optional<std::string> token;
  std::optional<std::string> controller_name;
  std::optional<std::string> log_dir;
  std::optional<std::string> agents_file;
  bool no_interactive = false;
  std::optional<bool> no_cert;
  std::vector<AgentEndpoint> endpoints;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    auto next = [&]() -> std::string {
      if (i + 1 >= argc) {
        throw std::runtime_error("missing value for " + arg);
      }
      return argv[++i];
    };
    if (arg == "--plan") {
      plan_path = next();
    } else if (arg == "--cert") {
      cert_file = next();
    } else if (arg == "--key") {
      key_file = next();
    } else if (arg == "--ca") {
      ca_file = next();
    } else if (arg == "--token") {
      token = next();
    } else if (arg == "--name") {
      controller_name = next();
    } else if (arg == "--agent") {
      endpoints.push_back(parse_endpoint(next()));
    } else if (arg == "--no-cert") {
      no_cert = true;
    } else if (arg == "--log-dir") {
      log_dir = next();
    } else if (arg == "--agents-file") {
      agents_file = next();
    } else if (arg == "--no-interactive") {
      no_interactive = true;
    } else {
      throw std::runtime_error("unknown argument: " + arg);
    }
  }

  if (plan_path.empty()) {
    std::cerr << "usage: rc --plan plan.txt [--no-cert | --cert client.crt --key client.key --ca ca.crt] [--agent host:port ...] [--agents-file file] [--token TOKEN] [--name rc] [--log-dir dir]\n";
    return 2;
  }

  PlanSpec plan = parse_plan_file(plan_path);

  ControllerState state;
  auto pick_string = [](const std::optional<std::string> &cli,
                        const std::optional<std::string> &plan_value,
                        const std::string &fallback) {
    if (cli) {
      return *cli;
    }
    if (plan_value) {
      return *plan_value;
    }
    return fallback;
  };

  std::string resolved_token = pick_string(token, plan.options.token, "remote-token");
  std::string resolved_name = pick_string(controller_name, plan.options.controller_name, "rc");
  std::string resolved_log_dir = pick_string(log_dir, plan.options.log_dir, "remote-logs");
  bool resolved_no_cert = no_cert.value_or(plan.options.no_cert);

  std::filesystem::create_directories(resolved_log_dir);
  state.token = resolved_token;
  state.controller_name = resolved_name;
  state.log_dir = resolved_log_dir;

  std::vector<AgentEndpoint> resolved_endpoints = endpoints;
  if (resolved_endpoints.empty()) {
    std::optional<std::string> resolved_agents_file = agents_file;
    if (!resolved_agents_file && plan.options.agents_file) {
      resolved_agents_file = *plan.options.agents_file;
    }
    if (resolved_agents_file) {
      resolved_endpoints = load_agents_file(*resolved_agents_file);
    }
  }
  if (resolved_endpoints.empty()) {
    for (const auto &endpoint_text : plan.options.agents) {
      resolved_endpoints.push_back(parse_endpoint(endpoint_text));
    }
  }
  if (resolved_endpoints.empty()) {
    std::cerr << "usage: rc --plan plan.txt [--no-cert | --cert client.crt --key client.key --ca ca.crt] [--agent host:port ...] [--agents-file file] [--token TOKEN] [--name rc] [--log-dir dir]\n";
    return 2;
  }

  TlsConfig client_cfg;
  client_cfg.no_tls = resolved_no_cert;
  if (!resolved_no_cert) {
    std::string resolved_cert = pick_string(cert_file, plan.options.cert_file, "");
    std::string resolved_key = pick_string(key_file, plan.options.key_file, "");
    std::string resolved_ca = pick_string(ca_file, plan.options.ca_file, "");
    if (resolved_cert.empty() || resolved_key.empty()) {
      std::cerr << "secure mode needs certs in the plan or on the CLI: --cert, --key, --ca\n";
      return 2;
    }
    client_cfg.ca_file = resolved_ca;
    client_cfg.client_cert_file = resolved_cert;
    client_cfg.client_key_file = resolved_key;
  }

  std::vector<std::thread> connector_threads;
  connector_threads.reserve(resolved_endpoints.size());
  for (const auto &endpoint : resolved_endpoints) {
    connector_threads.emplace_back([&state, endpoint, client_cfg] { agent_connector(state, endpoint, client_cfg); });
  }

  std::thread stdin_thread;
  if (!no_interactive) {
    stdin_thread = std::thread([&] { interactive_loop(state); });
  }

  std::cout << "controller starting"
            << " waiting for agents";
  for (const auto &endpoint : resolved_endpoints) {
    std::cout << " " << endpoint.host << ":" << endpoint.port;
  }
  std::cout << "\n";
  std::cout << "interactive commands: status | pause <node> | resume <node> | kill <node>\n";

  for (const auto &stage : plan.stages) {
    std::cout << "starting stage " << stage.name << "\n";
    {
      std::lock_guard<std::mutex> lock(state.mutex);
      state.stage_failed = false;
    }

    if (stage.has_queue_files) {
      run_queue_stage(state, stage);
    } else {
      run_fixed_parallel_stage(state, stage);
    }

    if (state.stage_failed) {
      std::cerr << "stage failed: " << stage.name << "\n";
      break;
    }
    std::cout << "stage complete: " << stage.name << "\n";
  }

  state.running = false;
  state.cv.notify_all();
  for (auto &thread : connector_threads) {
    if (thread.joinable()) {
      thread.detach();
    }
  }
  if (stdin_thread.joinable()) {
    stdin_thread.detach();
  }
  return state.stage_failed ? 1 : 0;
}
