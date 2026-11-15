# Changelog

All notable changes to a2a-hub are listed here. Versions follow SemVer.

## [Unreleased]

### Added

- The host CLI `a2a` with the commands `claude-hook`, `claude-headers`, `mcp`, `codex`, `codex-server`, `codex-adapter`, and `version`. `a2a codex` passes the terminal cwd with `-C` unless the user gives `-C` or `--cd`.
- The Claude Code helpers: one hub agent per Claude process, its id from the SessionStart hook or a pid fallback, and the `/mcp` headers.
- `a2a mcp`, a stdio MCP server that forwards to `/mcp`, retries without limit, and opens a new MCP session when the hub forgot the old one. It retries a `tools/call` only after a failed connect or a `503`, so a message is not sent twice.
- The Codex adapter: it links each Codex thread to the hub, maps its status, and wakes it with `turn/start`. It takes only user threads, as Codex does: no ephemeral, sub-agent, or internal threads. A state file keeps the threads of the last 7 days across restarts. A new thread without a rollout is woken with `turn/start` alone. A thread is named `codex-<last 6 hex digits of the thread id>`.
- The systemd user units `a2a-codex-server.service` and `a2a-codex.service` in `deploy/systemd/`. `a2a codex-server --mcp-transport stdio` runs the hub MCP server through `a2a mcp`.

### Fixed

- `/mcp` names a Codex thread that it registers by the last 6 hex digits of the thread id, as the adapter does. Codex thread ids are UUIDv7, so their first digits are a time prefix that many threads share.

## [1.2.0] - 2026-11-15

### Added

- The MCP endpoint `/mcp` for the harnesses `claude-code`, `codex`, and `hermes`, with the tools `send`, `reply`, `inbox`, `set_status`, and `peers`. For `hermes`, `send.wait` returns the first reply.
- Claude Code channel pushes: a `wake` `now` message to a Claude Code agent with an MCP session becomes a `notifications/claude/channel` on its stream. The send gives `unconfirmed`.
- Replies to the MCP sends of an external agent stay in its MCP inbox. With the new `webhook` field of the external agents file, the hub posts each one with a Hermes generic V2 HMAC signature and the origin of the send.
- The Hermes plugin `a2a_hub_origin` in `integrations/hermes/`. It adds the Telegram chat origin of the turn to each hub `send` call. Its tests run it and the Hermes webhook route from a Hermes source tree, and `webhook-vector.json` pins the push signature.

## [1.1.0] - 2026-11-15

### Added

- Link protocol 3 and `hub-protocol-v3.json`. A session of any harness registers as kind `session` with the label `harness`, such as `pi`, `claude-code`, or `codex`. A protocol 3 register without `labels.harness` gets `400`, unless the stored labels already have it.
- The agent header `X-A2A-Agent` and the bus extension URI `https://github.com/orswn/a2a-hub/bus/v1`.
- The hub logs `migrated schema <old> to <new>` once to stderr when it migrates an existing database.
- A `426` body lists the accepted protocols in `supported`, and its text names them: `hub protocol 2 or 3 required`.

### Changed

- The agent kind `pi` is now `session`. Schema 3 renames it in agents, message senders, and stored link events. Each renamed agent gets `harness` `pi`. The TypeScript hub cannot open a schema 3 file.
- The card of a session agent says `<harness> session on <host>`. The skill id stays `pi-session`.
- Register also accepts protocol 2. A protocol 2 register without a harness gets `harness` `pi`. A child without a harness gets the harness of its parent, else `pi`. The hub still reads `X-Pimon-Agent` and the pimon bus extension URI, and answers a send under the URI that the request used.

## [1.0.0] - 2026-11-15

### Added

- `a2a-hub`, the Go port of the pimon hub. It reads the same flags and `HUB_*` environment, loads the same token file, and opens the same `hub.sqlite` (`user_version` 2). It prints `listening <port>` and serves `GET /healthz`. `SIGTERM` and `SIGINT` stop it. `SIGHUP` reloads the token file.
- `a2a-hub healthcheck` asks the local hub for `/healthz` and exits 0 when it answers 200.
- `@orswn/a2a-hub-protocol`: the TypeBox source, types, constants, route manifest, and JSON Schema of link protocol 2, unchanged from pimon.
- The black-box contract suite of the link protocol in `test/`.
- The hub engine, a port of the TypeScript engine with its tests: agents, tasks, messages, mailbox events, presence, retention, push configs and outbox, and external agent jobs. It writes the same sqlite rows as the TypeScript engine. Listeners run after the commit in commit order, and a link subscription reads its replay and subscribes under one lock.
- The push URL guard with the allow list.
- `GET /healthz` reports the online agent count.
- The routes of the TypeScript hub: A2A 1.0 JSON-RPC and SSE at `/a2a/<agent>` with agent cards, `/agents`, and the link API with event streams, replay, heartbeat, and presence. JSON-RPC params also take the proto field names, such as `task_id`, and a role as its enum number.
- The push sender with retries and a socket-level address check.
- The built-in test agents `tck` and `echo` behind `--test-agents`.
- The external agents file (`--external`, `HUB_EXTERNAL_FILE`) with the format, defaults, and checks of the TypeScript hub.
- The gateway to external A2A agents: streamed dispatch, `GetTask` and `ListTasks` recovery, rate-limit retries, connect-failure backoff, `CancelTask`, and card refresh. The hub card of an external agent shows the configured description and the skills of the agent card.
- `SIGHUP` reloads the external agents file. A bad file keeps the old agents.
- A link stream ends when its client falls 10,000 events behind. The client gets the events in its replay.
- The process suites in `test/process/` and the a2a-tck conformance job in CI.
- A distroless container image that compiles the binary in a Go build stage. Its `/data` belongs to the `nonroot` user. CI builds and runs it on every push and pull request. It pushes it to `ghcr.io/orswn/a2a-hub:vX.Y.Z` only on a `v*` tag.
