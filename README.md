# remote

`remote` is a two-binary C++ tool for running command batches across remote machines.

## Getting Started

Build the project from the repository root:

```bash
make
```

That produces:

- `bin/rc`
- `bin/rc-agent`

To remove build outputs:

```bash
make clean
```

You can override the compiler or flags if needed:

```bash
make CXX=clang++ CXXFLAGS="-std=c++17 -O2 -Wall -Wextra -pedantic"
```

For a local demo:

1. Generate the development certificates.

   ```bash
   ./examples/make-dev-certs.sh
   ```

2. Start three agents in separate terminals.

   ```bash
   ./bin/rc-agent --port 19001 --cert examples/certs/server.crt --key examples/certs/server.key --ca examples/certs/ca.crt --token secret --name node1
   ./bin/rc-agent --port 19002 --cert examples/certs/server.crt --key examples/certs/server.key --ca examples/certs/ca.crt --token secret --name node2
   ./bin/rc-agent --port 19003 --cert examples/certs/server.crt --key examples/certs/server.key --ca examples/certs/ca.crt --token secret --name node3
   ```

3. Start the controller from the repository root.

   ```bash
   ./bin/rc --plan examples/demo.plan
   ```

For plaintext development only, use `--no-cert` on both binaries:

```bash
./bin/rc-agent --no-cert --port 19001 --token secret --name node1
./bin/rc-agent --no-cert --port 19002 --token secret --name node2
./bin/rc-agent --no-cert --port 19003 --token secret --name node3
./bin/rc --no-cert --plan examples/demo.plan
```

## Components

- `rc` is the controller. It connects to agents over TLS by default, schedules stages, pushes file payloads, streams logs, and exposes live controls on stdin.
- `rc-agent` runs on each remote machine as a TLS listener by default. It accepts a controller connection, receives work, stages files locally, executes commands, and streams stdout/stderr and progress back.

## How it works

1. The controller reads a plan file.
2. Each agent starts as a TLS server and waits for the controller to connect.
3. The controller authenticates itself with a shared token after the TLS handshake.
4. The controller runs stages in order.
5. Inside a `single` stage, one task runs before moving on.
6. Inside a `parallel` stage, tasks are dispatched across available connected agents.
7. Shared files are copied to every task in the stage.
8. Split files are divided into row-based shards, one shard per task.
9. Queue-file stages keep feeding the next file to whichever agent becomes free.
10. Each agent writes command output back to a per-node log.
11. The controller waits for all tasks in the current stage before advancing to the next stage.

## Security

- Transport is encrypted with TLS.
- The agent validates the controller certificate with a CA file.
- A shared token is checked after the TLS handshake so only authorized controllers are accepted.
- The controller presents a certificate/key pair when it connects to agents, so the agent can verify the controller identity with the CA file.
- `--no-cert` switches both binaries into plaintext mode for local development only.

## Plan Format

A plan is a plain text file made of top-level options, an agent list source, and stages. Each stage contains one or more tasks.

Example:

```text
option name rc
option token secret
option log-dir examples/logs
option cert examples/certs/server.crt
option key examples/certs/server.key
option ca examples/certs/ca.crt
option agents-file examples/agents.txt

stage build parallel
task compile-a -- /usr/bin/g++ -c src/a.cpp -o a.o
task compile-b -- /usr/bin/g++ -c src/b.cpp -o b.o
shared file assets/common.dat
split file assets/big-input.bin
end

stage test single
task run-tests -- ./run-tests --suite smoke
end
```

Rules:

- `option <key> <value...>` sets controller defaults
- `agent <host:port>` adds an agent endpoint
- `option agents-file <path>` loads agent endpoints from a separate file
- `queue file <path>` adds one queued file per line; the controller feeds them through one task template across available agents
- `stage <name> single|parallel`
- `task <name> -- <argv...>`
- `shared file <path>` copies the file to every task in the stage
- `split file <path>` splits a text file into per-task row shards
- `end` closes the stage

Task execution details:

- The task command is executed as a normal process on the agent machine.
- The task working directory is created under the agent root directory.
- Shared files are written into every task working directory.
- Split file shards are written as `name.part0`, `name.part1`, and so on, and each shard keeps whole rows.
- In `queue file` mode, the controller uploads one file per job and appends the uploaded filename to the task argv.
- If a task prints lines starting with `::progress::<percent>::<message>`, those are forwarded as progress events.

## Example Walkthrough

See [`examples/demo.plan`](/home/phil/dev/remote/examples/demo.plan), [`examples/demo.queue.plan`](/home/phil/dev/remote/examples/demo.queue.plan), and [`examples/README.txt`](/home/phil/dev/remote/examples/README.txt).

That example shows:

- one single-task stage that copies a shared text file
- one parallel stage that splits `examples/data/input.txt` into three row-based shards
- one final single-task stage that runs after the parallel stage completes
- a queue example that feeds one file at a time to available agents

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
./bin/rc-agent --port 19001 --cert examples/certs/server.crt --key examples/certs/server.key --ca examples/certs/ca.crt --token secret --name node1
./bin/rc-agent --port 19002 --cert examples/certs/server.crt --key examples/certs/server.key --ca examples/certs/ca.crt --token secret --name node2
./bin/rc-agent --port 19003 --cert examples/certs/server.crt --key examples/certs/server.key --ca examples/certs/ca.crt --token secret --name node3
```

The controller reads the agent list from `examples/agents.txt` through the plan.
You can also supply `--agents-file <path>` on `rc` to override the plan at runtime.

Plaintext dev-only mode:

## Common Failure Modes

- `loading server certificate: ... No such file or directory` means the `--cert` or `--key` path is wrong or the certificate files have not been generated yet.
- If you passed `--no-cert`, no certificate files are required.
- If `rc-agent` fails with `bind() failed`, the chosen port is already in use. Pick a different free port for that agent.
- If the controller cannot connect to an agent, check the agent host, port, CA file, and shared token.
- If an agent rejects the controller, make sure the controller is using the same CA and the same certificate/key pair generated by `examples/make-dev-certs.sh`.
- If relative file paths in the plan do not resolve, start the controller from the repository root.

## Runnable Local Example

1. Generate certs:

   ```bash
   ./examples/make-dev-certs.sh
   ```

2. Start the three agents in three terminals:

   ```bash
   ./bin/rc-agent --port 19001 --cert examples/certs/server.crt --key examples/certs/server.key --ca examples/certs/ca.crt --token secret --name node1
   ./bin/rc-agent --port 19002 --cert examples/certs/server.crt --key examples/certs/server.key --ca examples/certs/ca.crt --token secret --name node2
   ./bin/rc-agent --port 19003 --cert examples/certs/server.crt --key examples/certs/server.key --ca examples/certs/ca.crt --token secret --name node3
   ```

3. Start the controller in another terminal after the agents are listening:

   ```bash
   ./bin/rc --plan examples/demo.plan
   ```

4. Optional dev-only plaintext mode:

   ```bash
   ./bin/rc-agent --no-cert --port 19001 --token secret --name node1
   ./bin/rc-agent --no-cert --port 19002 --token secret --name node2
   ./bin/rc-agent --no-cert --port 19003 --token secret --name node3
   ./bin/rc --no-cert --plan examples/demo.plan
   ```
