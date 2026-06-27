#include "plan.hpp"

#include "common.hpp"

#include <fstream>
#include <sstream>
#include <stdexcept>

namespace remote {

PlanSpec parse_plan_file(const std::filesystem::path &path) {
  PlanSpec plan;
  std::ifstream in(path);
  if (!in) {
    throw std::runtime_error("unable to open plan file: " + path.string());
  }

  StageSpec *current_stage = nullptr;
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
    const std::string &head = tokens[0];
    if (head == "option") {
      if (current_stage != nullptr) {
        throw std::runtime_error("option lines must appear before stages");
      }
      if (tokens.size() < 2) {
        throw std::runtime_error("option line must be: option <key> <value...>");
      }
      const std::string &key = tokens[1];
      if (key == "no-cert") {
        plan.options.no_cert = tokens.size() == 2 || tokens[2] == "true" || tokens[2] == "1" || tokens[2] == "yes";
        continue;
      }
      if (tokens.size() < 3) {
        throw std::runtime_error("option line must be: option <key> <value>");
      }
      std::string value = tokens[2];
      if (key == "name") {
        plan.options.controller_name = value;
      } else if (key == "token") {
        plan.options.token = value;
      } else if (key == "log-dir") {
        plan.options.log_dir = value;
      } else if (key == "cert") {
        plan.options.cert_file = value;
      } else if (key == "key") {
        plan.options.key_file = value;
      } else if (key == "ca") {
        plan.options.ca_file = value;
      } else if (key == "agents-file") {
        plan.options.agents_file = value;
      } else {
        throw std::runtime_error("unknown option key: " + key);
      }
      continue;
    }
    if (head == "agent") {
      if (current_stage != nullptr) {
        throw std::runtime_error("agent lines must appear before stages");
      }
      if (tokens.size() != 2) {
        throw std::runtime_error("agent line must be: agent <host:port>");
      }
      plan.options.agents.push_back(tokens[1]);
      continue;
    }
    if (head == "stage") {
      if (tokens.size() < 3) {
        throw std::runtime_error("stage line must be: stage <name> <single|parallel>");
      }
      plan.stages.push_back(StageSpec{});
      current_stage = &plan.stages.back();
      current_stage->name = tokens[1];
      if (tokens[2] == "single") {
        current_stage->kind = StageKind::kSingle;
      } else if (tokens[2] == "parallel") {
        current_stage->kind = StageKind::kParallel;
      } else {
        throw std::runtime_error("unknown stage kind: " + tokens[2]);
      }
      continue;
    }
    if (head == "end") {
      current_stage = nullptr;
      continue;
    }
    if (current_stage == nullptr) {
      throw std::runtime_error("directive outside of a stage: " + head);
    }
    if (head == "task") {
      if (tokens.size() < 4 || tokens[2] != "--") {
        throw std::runtime_error("task line must be: task <name> -- <argv...>");
      }
      TaskSpec task;
      task.name = tokens[1];
      task.workdir = current_stage->name + "/" + task.name;
      task.argv.assign(tokens.begin() + 3, tokens.end());
      if (task.argv.empty()) {
        throw std::runtime_error("task requires at least one argv element");
      }
      current_stage->tasks.push_back(std::move(task));
      continue;
    }
    if (head == "shared") {
      if (tokens.size() != 3 || tokens[1] != "file") {
        throw std::runtime_error("shared line must be: shared file <path>");
      }
      current_stage->shared_files.push_back(tokens[2]);
      continue;
    }
    if (head == "split") {
      if (tokens.size() != 3 || tokens[1] != "file") {
        throw std::runtime_error("split line must be: split file <path>");
      }
      if (current_stage->has_queue_files) {
        throw std::runtime_error("split file cannot be combined with queue file-list");
      }
      current_stage->split_file = tokens[2];
      current_stage->has_split_file = true;
      continue;
    }
    if (head == "queue") {
      if (tokens.size() < 3 || (tokens[1] != "file" && tokens[1] != "file-list")) {
        throw std::runtime_error("queue line must be: queue file <path>");
      }
      if (current_stage->has_split_file) {
        throw std::runtime_error("queue file-list cannot be combined with split file");
      }
      if (tokens[1] == "file") {
        if (tokens.size() != 3) {
          throw std::runtime_error("queue line must be: queue file <path>");
        }
        current_stage->queue_files.push_back(tokens[2]);
      } else {
        current_stage->queue_files.assign(tokens.begin() + 2, tokens.end());
      }
      current_stage->has_queue_files = true;
      continue;
    }
    throw std::runtime_error("unknown directive: " + head);
  }

  if (plan.stages.empty()) {
    throw std::runtime_error("plan file contains no stages");
  }
  for (const auto &stage : plan.stages) {
    if (stage.tasks.empty()) {
      throw std::runtime_error("stage has no tasks: " + stage.name);
    }
    if (stage.kind == StageKind::kSingle && stage.tasks.size() != 1) {
      throw std::runtime_error("single stage must contain exactly one task: " + stage.name);
    }
    if (stage.has_queue_files) {
      if (stage.kind != StageKind::kParallel) {
        throw std::runtime_error("queue file-list requires a parallel stage: " + stage.name);
      }
      if (stage.tasks.size() != 1) {
        throw std::runtime_error("queue file-list stage must contain exactly one task template: " + stage.name);
      }
      if (stage.queue_files.empty()) {
        throw std::runtime_error("queue file-list stage must name at least one file: " + stage.name);
      }
    }
  }
  return plan;
}

}  // namespace remote
