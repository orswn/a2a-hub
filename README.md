# a2a-hub

An [A2A 1.0](https://a2a-protocol.org) hub with its own task store. Agent harness sessions (pi, Claude Code, Codex) connect to it through the link API, a REST and SSE protocol. External A2A agents, such as Hermes, sit behind it as peers. The hub keeps agents, tasks, messages, and mailbox events in one sqlite file.

This is the Go port of the hub that started inside [pimon](https://github.com/orswn/pimon). It keeps the same command line, `HUB_*` environment, sqlite file, and wire.

## Status

What exists now:

- The server binary `a2a-hub`: flags and env, token file, the sqlite store (`user_version` 3), `GET /healthz` with the online agent count, and the `healthcheck` subcommand.
- The routes of the TypeScript hub: the A2A 1.0 JSON-RPC endpoint and agent card of each agent at `/a2a/<agent>`, `/agents`, and the link API with its event streams, heartbeat, and presence.
- The push sender: it posts each task event to the webhooks of the task, with retries after 1 s, 5 s, and 30 s. It checks the address of each webhook again on the socket that it opens.
- The gateway to external A2A agents, such as Hermes: it sends their hub tasks with `SendStreamingMessage`, maps the stream back to the hub task, falls back to `GetTask` after a dropped stream or a restart, retries after a rate limit or a connect failure, sends `CancelTask`, and refreshes each agent card. `SIGHUP` reloads the external agents file.
- The built-in test agents `tck` and `echo` of `--test-agents`. The a2a-tck conformance suite runs against `tck` in CI.
- The engine (`internal/engine`): agents and host binding, `@name` resolution, the task lifecycle, context follow-ups, status text, open questions, task scope, task lists with page tokens, the mailbox events with replay and ack, retention, push configs and the push outbox, and the job and dispatch rows of external agents.
- The push URL guard (`internal/pushguard`): the config-time and send-time address checks and the allow list.
- The protocol package `@orswn/a2a-hub-protocol` with the JSON Schema of link protocols 3 and 2.
- The black-box contract suites of link protocols 3 and 2.
- The MCP endpoint `/mcp` for Claude Code, Codex, and Hermes, with Claude Code channel pushes and signed reply webhooks to Hermes. See [MCP endpoint](#mcp-endpoint).
- The host CLI `a2a`: the Claude Code helpers, the stdio MCP shim, and the Codex launcher, app-server unit, and adapter. See [Host CLI](#host-cli-a2a).

## Agent kinds and protocol versions

Every harness session registers as kind `session`. The label `harness` names the harness, such as `pi`, `claude-code`, or `codex`. External A2A agents have kind `external`, and the test agents have kind `builtin`.

`POST /link/register` accepts protocol 3 and protocol 2. Another protocol gets `426` with the text `hub protocol 2 or 3 required`, `expected` (3), and `supported` (`[2,3]`).

| | Protocol 3 | Protocol 2 |
|---|---|---|
| Register without `labels.harness` | `400`, unless the stored labels already have it | the hub sets `harness` to `pi` |
| Agent header of an A2A call | `X-A2A-Agent` | `X-Pimon-Agent` |
| Bus extension URI | `https://github.com/orswn/a2a-hub/bus/v1` | `https://github.com/orswn/pimon/a2a/bus/v1` |

- The hub reads both agent headers. When a request has both with different values, it gets `400`.
- The hub reads bus metadata under both URIs. The current URI wins when a message has both.
- The delivery stamp of a send uses the URI of the request metadata.
- The status text mark uses the protocol 2 URI, so that protocol 2 clients still find it.
- `POST /link/<agent>/children` without `labels.harness` gives the child the `harness` of its parent, else `pi`.
- The card of a session agent says `<harness> session on <host>`. Its skill id stays `pi-session`.
- Schema 3 renames the stored kind `pi` to `session` in `agents`, `messages.sender_kind`, and the `from.kind` of stored link events. Each renamed agent gets `harness` `pi`. The TypeScript hub cannot open a schema 3 file.

Planned:

- A released container image. CI builds it now but pushes it only on a `v*` tag.

## MCP endpoint

`/mcp` is a streamable HTTP MCP server. The hub writes its JSON-RPC by hand: it answers `server/discover` with `-32601` in HTTP 200 and negotiates `2025-11-25`, so Claude Code keeps its channel. One request carries one JSON-RPC message. The hub does not check the `Origin` header: every request needs a bearer token, and the hub sends no CORS headers, so a page in a browser cannot call it with the token.

| Header | Meaning |
|---|---|
| `Authorization: Bearer <token>` | Required. A host token, or the peer token of an external agent for `hermes`. |
| `X-A2A-Harness` | Required on `initialize`: `claude-code`, `codex`, or `hermes`. Another value gets `400`. |
| `X-A2A-Agent` | `claude-code` only, required: the agent id. |
| `X-A2A-Session`, `X-A2A-Cwd` | Optional. The session label and the working directory of a `claude-code` agent. Percent-encoding is decoded. |
| `Mcp-Session-Id` | The session id from the `initialize` answer. Every later request needs it. |

How each harness binds to an agent:

- `claude-code`: `initialize` registers the `session` agent of `X-A2A-Agent` with `harness` `claude-code` on the host of the token. Its name is `claude-<first 6 hex digits of X-A2A-Session>`, or `claude-<pid>` for an agent id of the form `claude-<host>-<pid>-<n>`. It keeps the name of its first register. The `GET` stream of the session owns the presence of the agent, with the link grace. A newer session of the same agent takes over the stream.
- `codex`: each `tools/call` acts for the agent whose id is `_meta.threadId`. The hub registers an unknown thread as an offline `session` agent with `harness` `codex`, named `codex-<last 6 hex digits of the thread id>`. The Codex adapter registers the same id over the link API and owns presence and wakes. The MCP session has no `GET` stream.
- `hermes`: the session acts for the external agent whose id is the token principal. It has no `GET` stream.

Tools: `send`, `reply`, `inbox`, `set_status`, and `peers`. A `hermes` session also gets `send.wait` (0 to 30 s): it blocks until the first reply, answer, result, or final task state, returns it, and marks it read. Claude Code and Codex get each reply as a push or a wake, so their `send` refuses `wait`. `send` of a `hermes` session also takes `origin` `{platform, chat_id, thread_id}` and stores it with the message. `internal/mcp/testdata/model-visible.txt` records the model-visible text of each harness and its char counts.

### Claude Code channel

A message with `wake` `now` to an agent with a Claude Code MCP session becomes one `notifications/claude/channel` on its `GET` stream. `content` is `<sender name>: <text>`. `meta` holds `message_id`, `sender_kind`, and `kind`. It has no task id, because the tools take a message id. A task result or question goes the same way. Claude Code never confirms a channel event, so the hub acks the event when the write succeeds, and the send gives `unconfirmed` at once. Without an open stream the send gives `queued`, and the next stream pushes the event. A message with `wake` `never` gives `queued` with the text `stored in the hub inbox of <name>`. The hub does not push a message that the model already read.

### Hermes replies

A message to an external agent that answers or continues a hub thread that the agent started through `/mcp` stays in its MCP inbox. The hub does not send it as an A2A task. Unless a waiting `send` took it, the hub posts it to the `webhook` of the agent, signed like the generic route of the Hermes webhook platform:

- `X-Webhook-Timestamp`: Unix seconds.
- `X-Webhook-Signature-V2`: lowercase hex HMAC-SHA256 of `<timestamp>.<raw body>` with the secret.
- `X-Request-ID`: the hub message id. Hermes records the id before it delivers and answers a known id with 200 `duplicate`. So a try after a 5xx answer uses `<message id>.r<try>`, and a try after a network error keeps the id.

The JSON body:

```json
{"event": "a2a-hub.message",
 "origin": {"platform": "telegram", "chat_id": "-100123", "thread_id": "42"},
 "from": {"id": "pi-1", "name": "mars", "harness": "pi"},
 "kind": "result", "message_id": "...", "in_reply_to": "...", "hub_thread_id": "...",
 "task_id": "...", "state": "completed", "text": "..."}
```

`origin` is the origin of the send that started the thread. Each of its fields is a string, `""` when unknown. The hub tries 3 times, 1 s and 4 s apart, after a network error or a 5xx answer. Another answer ends the tries. After the last failure the hub logs one line that starts with `webhook delivery failed; the message stays in the inbox`. The queue of posts is in memory: a restart or a full queue of 256 loses the post, and the hub does not post it again at the next start. The message stays in the inbox until `inbox` reads it. Tasks, questions, and corrections outside such a thread still go to the agent over A2A.

## Host CLI `a2a`

`a2a` connects the Claude Code and Codex sessions of a host to the hub. One binary has all commands:

| Command | Use |
|---|---|
| `a2a claude-hook` | SessionStart hook of Claude Code. Records the session id of the Claude process. |
| `a2a claude-headers` | `headersHelper` of the Claude Code MCP server. Prints the hub headers as JSON. |
| `a2a mcp [--harness codex\|claude-code]` | stdio MCP server that forwards to `<hub>/mcp`. A fallback for a client that cannot use HTTP MCP. It retries a `tools/call` only after a failed connect or a `503`. |
| `a2a codex [args]` | Starts the Codex units if they do not run, then runs `codex --remote unix://$XDG_RUNTIME_DIR/a2a/codex.sock -C <cwd> [args]`. |
| `a2a codex-server [--mcp-transport http\|stdio]` | Runs `codex app-server` on that socket with the hub as MCP server `a2a`. Used by a unit. |
| `a2a codex-adapter` | Links each Codex thread to the hub. Used by a unit. |
| `a2a version` | Prints the version. |

Install:

```sh
GOPRIVATE=github.com/orswn GOBIN=~/.local/bin mise exec go@1.26.11 -- go install github.com/orswn/a2a-hub/cmd/a2a@vX.Y.Z
```

### Token and hub URL

- Every command reads the host token from `~/.config/a2a/host.token`.
- The file must be a regular file of the user with mode `0600`. Else the command stops with an error.
- The hub URL is `--hub`, else env `A2A_HUB_URL`, else `https://a2a-hub.rswn.dev`. The MCP URL is `<hub>/mcp`.

```sh
install -d -m 0700 ~/.config/a2a
install -m 0600 /dev/stdin ~/.config/a2a/host.token   # paste the token, then Ctrl-D
```

### Claude Code

Each Claude process is one hub agent. The hook and the headers helper share the file `$XDG_RUNTIME_DIR/a2a/claude/<pid>.json`:

- The hook writes the session id. A `/clear` gives a new session id but keeps the agent.
- The first headers call picks the agent id: the session id of the hook if it comes within 3 s, else `claude-<host>-<pid>-<start>`. Claude starts the MCP connect without waiting for the hook, so the helper waits for it.
- Later headers calls, such as reconnects, reuse the agent id.
- A file of an older process with the same pid has another start time. The helper replaces it.
- `claude --resume <id>` starts a new process whose first session id is `<id>`, so it maps to the same agent.

Setup, once per host. These commands change the Claude config, so run them by hand:

```sh
claude mcp add-json -s user a2a '{"type":"http","url":"https://a2a-hub.rswn.dev/mcp","headersHelper":"a2a claude-headers"}'
```

Keep the helper a plain command. A redirect or pipe changes its parent process.

The SessionStart hook in `~/.claude/settings.json`:

```json
{
	"hooks": {
		"SessionStart": [{ "hooks": [{ "type": "command", "command": "a2a claude-hook" }] }]
	}
}
```

Claude delivers hub messages only with the channel flag. Add `--dangerously-load-development-channels server:a2a` to your existing `claude` alias, and keep its other flags. Without an alias:

```sh
alias claude='claude --dangerously-load-development-channels server:a2a'
```

Claude gives up on a dropped HTTP MCP server after about 15 s. The stdio shim retries without limit. To use it, register the shim instead:

```sh
claude mcp add-json -s user a2a '{"type":"stdio","command":"a2a","args":["mcp","--harness","claude-code"]}'
```

### Codex

Two systemd user units in `deploy/systemd/` run Codex for the hub:

- `a2a-codex-server.service` runs `a2a codex-server`: `codex app-server --listen unix://$XDG_RUNTIME_DIR/a2a/codex.sock` with the hub as MCP server `a2a`. The token is only in the environment of this process. A restart of this unit ends every Codex TUI on the host.
- `a2a-codex.service` runs `a2a codex-adapter`. It can restart alone. Its restart does not touch the Codex sessions.

The units get the environment of the systemd user manager, not the environment of your terminal. With `a2a codex`, the TUI is only a client: Codex runs its commands inside the app-server. So these commands do not see `SSH_AUTH_SOCK` or keys that `~/.bashrc` exports. Check the manager environment with `systemctl --user show-environment`. Add a missing value with `systemctl --user set-environment NAME=value`, or with `Environment=` in `systemctl --user edit a2a-codex-server.service`.

Install, once per host. `go install` keeps the module source in the Go module cache, so the unit files are there:

```sh
src="$(mise exec go@1.26.11 -- go env GOMODCACHE)/github.com/orswn/a2a-hub@vX.Y.Z"
install -d ~/.config/systemd/user
install -m 0644 "$src"/deploy/systemd/a2a-codex-server.service "$src"/deploy/systemd/a2a-codex.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now a2a-codex-server.service a2a-codex.service
```

From a clone of this repository, use `src=.` instead.

To use another hub, add `Environment=A2A_HUB_URL=<url>` to both units with `systemctl --user edit`.

Codex sends the thread id of each tool call in `_meta.threadId`, and the hub maps it to the thread agent. If that does not reach the hub over HTTP, run the hub MCP server over stdio through `a2a mcp`. Set this in `systemctl --user edit a2a-codex-server.service`:

```ini
[Service]
ExecStart=
ExecStart=%h/.local/bin/a2a codex-server --mcp-transport stdio
```

Start Codex with `a2a codex`. It takes the same arguments as `codex`, such as `a2a codex resume --last`. It adds `-C` with the current directory unless you give `-C` or `--cd`: with `--remote`, Codex otherwise starts a new thread in the directory of the app-server. A resumed thread keeps its own directory. An alias makes it the default:

```sh
alias codex='a2a codex'
```

The adapter:

- Registers each Codex thread as a session agent: agent id = thread id, `labels.harness=codex`, the thread cwd, and the thread name, else `codex-<last 6 hex digits of the thread id>`.
- Takes only user threads, as Codex itself does. It skips ephemeral threads, sub-agent threads (a `parentThreadId` or a sub-agent `source`), internal threads (`source` `unknown`), and a `threadSource` other than `user`.
- Finds threads at connect, on `thread/started`, on any status or name event of an unknown thread, and every 30 s through `thread/loaded/list`.
- Maps the thread status `active` to `busy` and all else to `idle`.
- Keeps a thread online when Codex unloads it, so a message can still wake it. After 7 days without activity the thread goes offline. Its next event registers it again.
- Wakes a thread for a `wake=now` message and for a task update with text in a final, `input-required`, or `auth-required` state. A wake is `thread/resume` with `excludeTurns` unless the adapter is subscribed to the thread, then `turn/start` with the text below. `turn/start` steers a running turn.
- Drops its subscription with `thread/unsubscribe` at the next `idle`, so Codex can unload the thread as usual. An `idle` during a wake takes effect at the end of the wake, if the thread is still idle.
- Acks a link event only after `turn/start` succeeds. It handles the events of a thread in order, so a waiting wake holds back the later acks of that thread.
- Retries an overloaded (`-32001`), stopping, or lost app-server with backoff and no limit. A wake that meets a review or compact turn waits for `turn/completed` or `idle`, then tries again. After 5 other errors the event stays unacked, and the thread goes offline until its next event.
- Sets a `submitted` task to `working` after its turn starts.
- Takes a thread offline when Codex has no rollout for it and has not loaded it. A new thread has no rollout until its first turn. Its first wake is a `turn/start` without `thread/resume`.
- Never answers approvals and never sets turn policies.

The wake text:

```text
Hub message from <name> (<harness>) kind=<kind> messageId=<id>: <text>
```

A sender that is not a session shows its kind instead, such as `(external)`. After a failed sender lookup it shows `(unknown)`.

A task adds a blank line and `Answer with the a2a reply tool and this messageId.`

The state file `$XDG_STATE_HOME/a2a/codex-threads.json` (mode `0600`) lists the threads with activity in the last 7 days: id, name, cwd, and last activity. After a restart the adapter registers them again, so an unloaded thread stays reachable. The adapter writes it to a temp file and renames it, and drops older entries on each write. A corrupt file is logged once and written new.

## Build

Go and Node versions are in `mise.toml`.

```sh
make build            # bin/a2a-hub, static, CGO_ENABLED=0
```

## Run

```sh
bin/a2a-hub --listen 127.0.0.1:8080 --data ./data --tokens ./tokens.json
```

The hub prints `listening <port>` on stdout when it accepts connections. Logs go to stderr. When the hub migrates an existing database, it logs one line such as `migrated schema 2 to 3`.

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--listen <host:port>` | `HUB_LISTEN` | `0.0.0.0:8080` | Listen address. Port `0` picks a free port. An IPv6 host may be in brackets. |
| `--data <dir>` | `HUB_DATA` | `/data` | Data directory with `hub.sqlite`. `:memory:` keeps the database in memory. |
| `--tokens <file>` | `HUB_TOKENS_FILE` | none | Token file. Without it, only `/healthz` and agent cards answer. |
| `--external <file>` | `HUB_EXTERNAL_FILE` | none | External A2A agents file. See below. |
| `--test-agents` | `HUB_TEST_AGENTS=1` | off | Test mode: adds the built-in agents `tck` and `echo`, and adds loopback to the push allow list. |
| `--public-url <url>` | `HUB_PUBLIC_URL` | none | Base URL in agent cards. Must start with `http://` or `https://`. |
| `--push-allow <list>` | `HUB_PUSH_ALLOW` | empty | Comma list of host names, addresses, and CIDRs that push webhooks may reach although they are private. |
| | `HUB_PRESENCE_GRACE_MS` | `20000` | Time before a closed link stream marks its agent offline. |
| | `HUB_HEARTBEAT_MS` | `15000` | Interval of the link stream heartbeat. |

A flag wins over its env variable.

Exit codes: `0` after `SIGTERM` or `SIGINT`, `1` for a bad command line or a start failure, `2` for a bad setting value. `SIGHUP` reloads the token file and the external agents file. For a bad file it keeps the old tokens or agents and logs the error. Open link streams stay open.

The token file maps a principal name to its kind and the sha256 hex digest of its token:

```json
{
	"mars": { "kind": "host", "sha256": "<sha256 hex of the token>" },
	"hermes": { "kind": "peer", "sha256": "<sha256 hex of the token>" }
}
```

The external agents file maps a hub agent id to the agent:

```json
{
	"hermes": { "url": "http://hermes:9900/", "token": "<bearer token of the hub at Hermes>" }
}
```

- `url` (required): the JSON-RPC URL of the agent. Its card is at `<url>/.well-known/agent-card.json`.
- `token` (required): visible ASCII. The hub sends it as `Authorization: Bearer`.
- `name`: what `@<name>` resolves to. Default: the id.
- `description`: the description in the hub card. Default: `<name>: an external A2A agent`.
- `webhook`: `{"url": "<http or https URL>", "secret": "<shared secret>"}`. The hub posts replies to the MCP sends of this agent there. See [Hermes replies](#hermes-replies). Without it, the replies wait in the MCP inbox of the agent.
- Timings in milliseconds: `pollMs` (`10000`, the `GetTask` and card retry interval), `cardTtlMs` (`600000`), `replyTimeoutMs` (`960000`, from dispatch to the final answer), `idleMs` (`30000`, the longest stream silence), `retryMs` (`60000`, the wait after a rate limit), `offlineMs` (`600000`, how long the hub retries an agent that it cannot connect to).

A bad file at start stops the hub with exit code `1`. No error text quotes the file, because it holds plain tokens.

`a2a-hub healthcheck` sends `GET /healthz` to the listen address from the same flags and env, and exits `0` only for status 200. It dials `127.0.0.1` for the host `0.0.0.0` and `::1` for the host `::`.

### Container

```sh
podman build --format docker -f Containerfile --build-arg VERSION=0.1.0 -t a2a-hub .
podman run -v a2a-hub-data:/data -p 8080:8080 a2a-hub
```

The Containerfile compiles the binary in a Go build stage. The image holds only `/a2a-hub` on distroless static. Its health check runs `/a2a-hub healthcheck`. It runs as `nonroot` (uid 65532). The image `/data` belongs to that user, so a new named or anonymous volume is writable. A bind mount must be writable by the container user.

CI builds the image on every push and pull request, then runs it: `/a2a-hub healthcheck`, the `/healthz` version, and the `65532` owner of `/data` and `hub.sqlite`. Only a `v*` tag pushes it to `ghcr.io/orswn/a2a-hub`, with the git tag as the image tag, such as `ghcr.io/orswn/a2a-hub:v1.0.0`. The job summary shows the pushed digest.

## Test

```sh
make verify
```

- Go unit tests: `mise exec -- go test -race ./...`.
- Database compatibility: the engine tests run one scenario that `internal/engine/testdata/compat.mjs` also runs with the TypeScript engine. After the schema 3 migration both files must hold the same rows, and the Go engine must read the committed TypeScript-written file in `testdata/compat-node` the same way. With `PIMON_DIR` set to a pimon checkout and `node` on `PATH`, the test also runs the TypeScript engine again and checks that it refuses the schema 3 file that the Go engine wrote.
- Protocol package: `npm --prefix protocol test` checks that the committed JSON Schema matches the TypeBox source and that the files of earlier protocols have not changed.
- Contract suite: `npm --prefix test run contract` starts `bin/a2a-hub` (or env `HUB_BIN`) and checks it over HTTP and SSE against the JSON Schema of protocol 3. A reduced suite checks a protocol 2 client against the protocol 2 file.
- Process suites: `npm --prefix test run process` starts the same binary and checks the A2A face with the `@a2a-js/sdk` client, the link API, push webhooks, a restart, external agents behind a fake A2A agent (`test/support/fake-hermes.ts`), and `/mcp` driven like Claude Code, Codex, and Hermes with a webhook receiver that checks the recorded Hermes signature vector.
- Conformance: CI runs a2a-tck against `/a2a/tck` of a hub with `--test-agents`. `scripts/tck-gate.py` fails the job on a failed MUST requirement or fewer than 75 MUST passes. `scripts/hub-python-client.py` then sends a task with the Python A2A SDK.

## Known wire differences from the TypeScript hub

- A path with a percent escape that is not hex, such as `/agents/%zz`, gets `400` with a `text/plain` body from Go's HTTP server. The TypeScript hub answers `400` with `{"error":"malformed percent escape in path"}`. An escape that decodes to invalid UTF-8, such as `%ff`, gets the JSON body in both hubs.
- Go's HTTP server sends no `Connection: keep-alive` and no `Keep-Alive: timeout=65` header. Both are the HTTP/1.1 default, and the idle timeout is 65 s as in the TypeScript hub.
- A `413` answer to a body that is larger than 1 MB has `Connection: close`, and Go's HTTP server closes the connection. The TypeScript hub keeps it open.
- In A2A responses, the keys of struct values (`metadata` and data parts) are in sorted order: a2a-go decodes them into Go maps. The stored message, the link events, and the push payloads keep the order of the client. The TypeScript hub keeps the order of the client everywhere.
- A JSON-RPC body that is a JSON string is parsed again, as in `@a2a-js/sdk`. When that fails, the error text is `Failed to parse JSON request.`. `@a2a-js/sdk` sends the text of the V8 `SyntaxError`.
- JSON-RPC params with a wrong JSON type are refused with `-32602` and the text `Invalid method parameters.`. Examples: a string `historyLength`, a number `id`, a string `includeArtifacts` or `returnImmediately`, a role number outside the enum, or a part with no content. A string `pageSize` gets `pageSize must be between 1 and 100`. The hub log has the detail. The TypeScript hub coerces such values or ignores them.
- The path splits into its non-empty segments with no other normalization. `GET //healthz` gets `200`, and `GET /x/../healthz` gets `404`. The TypeScript hub parses the request target as a WHATWG URL: `//healthz` names the host `healthz` and gets `404`, and `/x/../healthz` loses its dot segments and gets `200`.

Known gateway differences:

- Requests to external agents have the headers of Go's HTTP client: `User-Agent: Go-http-client/1.1` and `Accept-Encoding: gzip`. The TypeScript hub sends the headers of Node `fetch`: `User-Agent: node`, `Accept-Encoding: gzip, deflate`, `Accept-Language: *`, and `Sec-Fetch-Mode: cors`. The A2A headers and bodies are the same.
- A JSON answer or one stream event of an external agent may be at most 4 MiB. A larger answer fails the task at once with `<name> answer is larger than 4 MiB`. The TypeScript hub has no limit.
- The error text of a connect failure is Go's text, such as `dial tcp 127.0.0.1:9: connect: connection refused (ECONNREFUSED)`. The TypeScript hub gives `fetch failed (ECONNREFUSED)`. Both name the same error code.
- A link stream whose client falls 10,000 events behind ends, and the client gets the events in its replay when it reconnects. The TypeScript hub buffers without a limit.
- A bad external agents file at start prints the error text. The TypeScript hub prints the stack trace.

Known engine and push differences:

- A `statusTimestampAfter` filter takes only the ISO 8601 forms of `Date.parse`. The TypeScript hub also takes the forms of the V8 legacy date parser, such as `Jan 1 2026` or `2026-02-30`.
- Status text is cut at 160 UTF-16 units. When the cut splits a surrogate pair, this hub drops the whole pair. The TypeScript hub keeps the lone high surrogate, and its sqlite driver stores it as U+FFFD, so the stored text ends in one more character.
- The config-time push URL check does not map fullwidth digits or decode percent escapes in the host. `http://１２７.０.０.１/` passes it and fails at send time. `http://%31%32%37.0.0.1/` gets `push config url is not a valid URL`. The TypeScript hub reads both hosts as `127.0.0.1` and refuses them at config time with `push url address 127.0.0.1 is not allowed`.
- The push sender checks the address of the socket again when it connects. A host name that resolves to an allowed address at the send-time check and to a denied address at the connect fails the attempt. The TypeScript hub resolves the host again in `fetch` and does not check that address.

## Protocol package

`@orswn/a2a-hub-protocol` is the repository root package. It ships `protocol/`: the TypeBox schemas, the wire types, the constants, the route manifest, and `hub-protocol-v<N>.json`. A client installs it from git:

```sh
npm install --save-dev "git+ssh://git@github.com/orswn/a2a-hub.git#<tag>"
```

Entry points: `@orswn/a2a-hub-protocol` (constants and types), `/schema`, `/routes`, `/hub-protocol-v3.json`, and `/hub-protocol-v2.json`. The package is TypeScript source. `typebox` is a peer dependency.
