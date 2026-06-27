#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace remote {

struct TaskSpec {
  std::string name;
  std::vector<std::string> argv;
  std::string workdir;
  std::optional<std::string> agent_name;
};

enum class SubstageKind { kSingle, kParallel, kQueue };
enum class WorkflowKind { kSequential, kParallel };

struct AgentPoolSpec {
  std::optional<std::string> agents_file;
  std::vector<std::string> agents;
};

struct SubstageSpec {
  std::string name;
  SubstageKind kind = SubstageKind::kSingle;
  AgentPoolSpec agent_pool;
  std::vector<TaskSpec> tasks;
  std::vector<std::filesystem::path> shared_files;
  std::filesystem::path split_file;
  bool has_split_file = false;
  std::vector<std::filesystem::path> queue_files;
  bool has_queue_files = false;
};

struct WorkflowSpec {
  std::string name;
  WorkflowKind kind = WorkflowKind::kSequential;
  AgentPoolSpec agent_pool;
  std::vector<SubstageSpec> substages;
};

struct PlanOptions {
  std::optional<std::string> controller_name;
  std::optional<std::string> token;
  std::optional<std::string> log_dir;
  std::optional<std::string> cert_file;
  std::optional<std::string> key_file;
  std::optional<std::string> ca_file;
  std::optional<std::string> agents_file;
  bool no_cert = false;
  std::vector<std::string> agents;
};

struct PlanSpec {
  PlanOptions options;
  std::vector<WorkflowSpec> workflows;
};

PlanSpec parse_plan_file(const std::filesystem::path &path);

}  // namespace remote
