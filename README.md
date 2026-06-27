# remote

`remote` is a two-binary C++ tool for running command batches across remote machines.

## Components

- `rc` is the controller. It connects to agents over TLS by default, schedules workflows and substages, pushes file payloads, streams logs, and exposes live controls on stdin.
- `rc-agent` runs on each remote machine as a TLS listener by default. It accepts a controller connection, receives work, stages files locally, executes commands, and streams stdout/stderr and progress back.

## Execution Model

1. The controller reads a plan file.
2. Each agent starts as a TLS server and waits for the controller to connect.
3. Work is grouped into workflows.
4. Workflows run in sequence unless a workflow is marked `parallel`.
5. Each workflow contains one or more substages.
6. Substages can run `single`, `parallel`, or `queue` work.
7. A `single` substage runs exactly one task template.
8. A `parallel` substage fans tasks out across available agents.
9. A `queue` substage feeds one file at a time to whichever agent becomes free.
10. Shared files are copied into every task working directory.
11. Split files are divided into row-based shards, one shard per task.
12. Each agent writes command output back to a per-node log.
13. The controller waits for a workflow to finish before moving to the next workflow.

## Security

- Transport is encrypted with TLS.
- The agent validates the controller certificate with a CA file.
- A shared token is checked after the TLS handshake so only authorized controllers are accepted.
- The controller presents a certificate/key pair when it connects to agents, so the agent can verify the controller identity with the CA file.
- `--no-cert` switches both binaries into plaintext mode for local development only.

## Plan Format

A plan is a plain text file made of top-level options and workflows. Each workflow contains one or more substages.

Example:

```text
option name rc
option token secret
option log-dir examples/logs
option cert examples/certs/server.crt
option key examples/certs/server.key
option ca examples/certs/ca.crt
option agents-file examples/agents.txt

workflow prepare sequential
  substage prepare single
    task prepare -- /bin/sh -c "echo preparing remote batch; wc -c shared-note.txt"
    shared file examples/data/shared-note.txt
  end
end

workflow process parallel
  substage shard parallel
    task shard-a -- /bin/sh -c "echo shard A; wc -l input.txt.part0; cat input.txt.part0"
    task shard-b -- /bin/sh -c "echo shard B; wc -l input.txt.part1; cat input.txt.part1"
    task shard-c -- /bin/sh -c "echo shard C; wc -l input.txt.part2; cat input.txt.part2"
    split file examples/data/input.txt
  end

  substage queue queue
    task process -- /bin/sh -c "echo queue job: $1; wc -l \"$1\"; cat \"$1\"" sh
    queue file examples/data/queue/job-1.txt
    queue file examples/data/queue/job-2.txt
    queue file examples/data/queue/job-3.txt
    queue file examples/data/queue/job-4.txt
    queue file examples/data/queue/job-5.txt
  end
end

workflow finalize sequential
  substage finalize single
    task finalize -- /bin/sh -c "echo all remote work finished"
  end
end
```

Rules:

- `option <key> <value...>` sets controller defaults
- `agent <host:port>` adds an agent endpoint to the default pool
- `option agents-file <path>` loads default agents from a separate file
- `workflow <name> sequential|parallel`
- `substage <name> single|parallel|queue`
- `agents-file <path>` inside a workflow or substage overrides the agent pool for that scope
- `agent <host:port>` inside a workflow or substage adds one endpoint to that scope
- `task <name> [agent <node>] -- <argv...>` defines a task template and optionally pins it to a named agent
- `shared file <path>` copies the file to every task in the substage
- `split file <path>` splits a text file into per-task row shards
- `queue file <path>` adds one queued file per line to a queue substage
- `end` closes the current substage or workflow

Example of a restricted substage and a pinned task:

```text
workflow special sequential
  agents-file examples/special-agents.txt

  substage prep parallel
    agent localhost:29001
    agent localhost:29003
    task prep-a -- /bin/sh -c "echo prep A"
    task prep-b agent node3 -- /bin/sh -c "echo pinned to node3"
  end
end
```

In that example:

- the `prep` substage can only use agents listed in `examples/special-agents.txt`
- the inline `agent` lines narrow that substage further to `localhost:29001` and `localhost:29003`
- `prep-b` is pinned to the agent named `node3`

Task execution details:

- The task command is executed as a normal process on the agent machine.
- The task working directory is created under the agent root directory.
- Shared files are written into every task working directory.
- Split file shards are written as `name.part0`, `name.part1`, and so on, and each shard keeps whole rows.
- In `queue` mode, the controller uploads one file per job and appends the uploaded filename to the task argv.
- If a task prints lines starting with `::progress::<percent>::<message>`, those are forwarded as progress events.

## Example Walkthrough

See [`examples/demo.plan`](/home/phil/dev/remote/examples/demo.plan), [`examples/demo.queue.plan`](/home/phil/dev/remote/examples/demo.queue.plan), and [`examples/README.txt`](/home/phil/dev/remote/examples/README.txt).

That example shows:

- one workflow that runs a prepare substage first
- one workflow that runs a shard substage and a queue substage at the same time
- one final workflow that runs after the parallel workflow completes

## Logging and Control

Each remote node gets its own log file in the controller log directory.

The controller records:

- authentication
- task dispatch
- task state changes
- stdout and stderr lines
- progress updates
- exit status

While the controller is running, stdin commands are available:

- `status` shows connected nodes and their current task
- `pause <node>` sends SIGSTOP to the running task on that node
- `resume <node>` sends SIGCONT
- `kill <node>` sends SIGTERM

## Build

```bash
make
```

## Local TLS Setup

For a local demo, generate a CA and controller cert/key:

```bash
./examples/make-dev-certs.sh
```

This writes:

- `examples/certs/ca.crt`
- `examples/certs/server.crt`
- `examples/certs/server.key`

## Run

Controller:

```bash
./bin/rc --plan examples/demo.plan
```

Local variant on higher loopback ports:

```bash
./bin/rc --agents-file examples/agents.local.txt --plan examples/demo.plan
```

Queued-file example:

```bash
./bin/rc --plan examples/demo.queue.plan
```

Agents:

```bash
./bin/rc-agent --port 29001 --cert examples/certs/server.crt --key examples/certs/server.key --ca examples/certs/ca.crt --token secret --name node1
./bin/rc-agent --port 29002 --cert examples/certs/server.crt --key examples/certs/server.key --ca examples/certs/ca.crt --token secret --name node2
./bin/rc-agent --port 29003 --cert examples/certs/server.crt --key examples/certs/server.key --ca examples/certs/ca.crt --token secret --name node3
```

The controller reads the agent list from `examples/agents.txt` through the plan.
You can also supply `--agents-file <path>` on `rc` to override the default pool at runtime.

Plaintext dev-only mode:

```bash
./bin/rc-agent --no-cert --port 29001 --token secret --name node1
./bin/rc-agent --no-cert --port 29002 --token secret --name node2
./bin/rc-agent --no-cert --port 29003 --token secret --name node3
./bin/rc --no-cert --plan examples/demo.plan
```
