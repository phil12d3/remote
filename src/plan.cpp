#include "plan.hpp"

#include "common.hpp"

#include <fstream>
#include <stdexcept>

namespace remote {
namespace {

WorkflowKind parse_workflow_kind(const std::string &text) {
  if (text == "sequential") {
    return WorkflowKind::kSequential;
  }
  if (text == "parallel") {
    return WorkflowKind::kParallel;
  }
  throw std::runtime_error("unknown workflow kind: " + text);
}

SubstageKind parse_substage_kind(const std::string &text) {
  if (text == "single") {
    return SubstageKind::kSingle;
  }
  if (text == "parallel") {
    return SubstageKind::kParallel;
  }
  if (text == "queue") {
    return SubstageKind::kQueue;
  }
  throw std::runtime_error("unknown substage kind: " + text);
}

void apply_agent_pool_option(AgentPoolSpec &pool, const std::vector<std::string> &tokens) {
  if (tokens.empty()) {
    return;
  }
  const std::string &head = tokens[0];
  if (head == "agents-file") {
    if (tokens.size() != 2) {
      throw std::runtime_error("agents-file line must be: agents-file <path>");
    }
    pool.agents_file = tokens[1];
    return;
  }
  if (head == "agent") {
    if (tokens.size() != 2) {
      throw std::runtime_error("agent line must be: agent <host:port>");
    }
    pool.agents.push_back(tokens[1]);
    return;
  }
  throw std::runtime_error("unknown agent pool directive: " + head);
}

}  // namespace

PlanSpec parse_plan_file(const std::filesystem::path &path) {
  PlanSpec plan;
  std::ifstream in(path);
  if (!in) {
    throw std::runtime_error("unable to open plan file: " + path.string());
  }

  WorkflowSpec *current_workflow = nullptr;
  SubstageSpec *current_substage = nullptr;
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
      if (current_workflow != nullptr) {
        throw std::runtime_error("option lines must appear before workflows");
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
    if (head == "agent" || head == "agents-file") {
      if (current_substage != nullptr) {
        apply_agent_pool_option(current_substage->agent_pool, tokens);
      } else if (current_workflow != nullptr) {
        apply_agent_pool_option(current_workflow->agent_pool, tokens);
      } else {
        if (head == "agent") {
          if (tokens.size() != 2) {
            throw std::runtime_error("agent line must be: agent <host:port>");
          }
          plan.options.agents.push_back(tokens[1]);
        } else {
          if (tokens.size() != 2) {
            throw std::runtime_error("agents-file line must be: agents-file <path>");
          }
          plan.options.agents_file = tokens[1];
        }
      }
      continue;
    }
    if (head == "workflow" || head == "stage") {
      if (current_workflow != nullptr) {
        throw std::runtime_error("nested workflows are not supported");
      }
      if (tokens.size() < 3) {
        throw std::runtime_error("workflow line must be: workflow <name> <sequential|parallel>");
      }
      plan.workflows.push_back(WorkflowSpec{});
      current_workflow = &plan.workflows.back();
      current_workflow->name = tokens[1];
      current_workflow->kind = parse_workflow_kind(tokens[2]);
      continue;
    }
    if (head == "substage") {
      if (current_workflow == nullptr) {
        throw std::runtime_error("substage lines must appear inside a workflow");
      }
      if (current_substage != nullptr) {
        throw std::runtime_error("nested substages are not supported");
      }
      if (tokens.size() < 3) {
        throw std::runtime_error("substage line must be: substage <name> <single|parallel|queue>");
      }
      current_workflow->substages.push_back(SubstageSpec{});
      current_substage = &current_workflow->substages.back();
      current_substage->name = tokens[1];
      current_substage->kind = parse_substage_kind(tokens[2]);
      current_substage->agent_pool = current_workflow->agent_pool;
      continue;
    }
    if (head == "end") {
      if (current_substage != nullptr) {
        current_substage = nullptr;
        continue;
      }
      if (current_workflow != nullptr) {
        current_workflow = nullptr;
        continue;
      }
      throw std::runtime_error("unexpected end directive");
    }

    if (current_substage == nullptr) {
      throw std::runtime_error("directive outside of a substage: " + head);
    }

    if (head == "task") {
      if (tokens.size() < 4) {
        throw std::runtime_error("task line must be: task <name> [agent <name>] -- <argv...>");
      }
      std::size_t arg_start = 3;
      TaskSpec task;
      task.name = tokens[1];
      if (tokens[2] == "agent") {
        if (tokens.size() < 6 || tokens[4] != "--") {
          throw std::runtime_error("task line must be: task <name> agent <name> -- <argv...>");
        }
        task.agent_name = tokens[3];
        arg_start = 5;
      } else if (tokens[2] != "--") {
        throw std::runtime_error("task line must be: task <name> [agent <name>] -- <argv...>");
      }
      task.workdir = current_workflow->name + "/" + current_substage->name + "/" + task.name;
      task.argv.assign(tokens.begin() + static_cast<std::ptrdiff_t>(arg_start), tokens.end());
      if (task.argv.empty()) {
        throw std::runtime_error("task requires at least one argv element");
      }
      current_substage->tasks.push_back(std::move(task));
      continue;
    }
    if (head == "shared") {
      if (tokens.size() != 3 || tokens[1] != "file") {
        throw std::runtime_error("shared line must be: shared file <path>");
      }
      current_substage->shared_files.push_back(tokens[2]);
      continue;
    }
    if (head == "split") {
      if (tokens.size() != 3 || tokens[1] != "file") {
        throw std::runtime_error("split line must be: split file <path>");
      }
      if (current_substage->kind == SubstageKind::kQueue) {
        throw std::runtime_error("split file cannot be combined with queue substages");
      }
      current_substage->split_file = tokens[2];
      current_substage->has_split_file = true;
      continue;
    }
    if (head == "queue") {
      if (tokens.size() != 3 || tokens[1] != "file") {
        throw std::runtime_error("queue line must be: queue file <path>");
      }
      if (current_substage->kind != SubstageKind::kQueue) {
        throw std::runtime_error("queue files can only appear inside queue substages");
      }
      current_substage->queue_files.push_back(tokens[2]);
      current_substage->has_queue_files = true;
      continue;
    }

    throw std::runtime_error("unknown directive: " + head);
  }

  if (current_substage != nullptr || current_workflow != nullptr) {
    throw std::runtime_error("plan file ended before closing workflow or substage");
  }
  if (plan.workflows.empty()) {
    throw std::runtime_error("plan file contains no workflows");
  }
  for (const auto &workflow : plan.workflows) {
    if (workflow.substages.empty()) {
      throw std::runtime_error("workflow has no substages: " + workflow.name);
    }
    for (const auto &substage : workflow.substages) {
      if (substage.tasks.empty()) {
        throw std::runtime_error("substage has no tasks: " + workflow.name + "/" + substage.name);
      }
      if (substage.kind == SubstageKind::kSingle && substage.tasks.size() != 1) {
        throw std::runtime_error("single substage must contain exactly one task: " + workflow.name + "/" + substage.name);
      }
      if (substage.kind == SubstageKind::kQueue) {
        if (substage.tasks.size() != 1) {
          throw std::runtime_error("queue substage must contain exactly one task template: " + workflow.name + "/" + substage.name);
        }
        if (substage.queue_files.empty()) {
          throw std::runtime_error("queue substage must name at least one file: " + workflow.name + "/" + substage.name);
        }
      }
    }
  }
  return plan;
}

}  // namespace remote
