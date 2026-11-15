# a2a-hub

An [A2A 1.0](https://a2a-protocol.org) hub with its own task store. Agent harness sessions (pi, Claude Code, Codex) connect to it through the link API, a REST and SSE protocol. External A2A agents, such as Hermes, sit behind it as peers. The hub keeps agents, tasks, messages, and mailbox events in one sqlite file.

This is the Go port of the hub that started inside [pimon](https://github.com/orswn/pimon). It keeps the same command line, `HUB_*` environment, sqlite file, and wire.

## Status

What exists now:

- The server binary `a2a-hub`: flags and env, token file, the sqlite store with the same schema (`user_version` 2), `GET /healthz` with the online agent count, and the `healthcheck` subcommand.
- The routes of the TypeScript hub: the A2A 1.0 JSON-RPC endpoint and agent card of each agent at `/a2a/<agent>`, `/agents`, and the link API with its event streams, heartbeat, and presence.
- The push sender: it posts each task event to the webhooks of the task, with retries after 1 s, 5 s, and 30 s. It checks the address of each webhook again on the socket that it opens.
- The built-in test agents `tck` and `echo` of `--test-agents`. The a2a-tck conformance suite runs against `tck` in CI.
- The engine (`internal/engine`): agents and host binding, `@name` resolution, the task lifecycle, context follow-ups, status text, open questions, task scope, task lists with page tokens, the mailbox events with replay and ack, retention, push configs and the push outbox, and the job and dispatch rows of external agents. It writes the same rows as the TypeScript engine, so either hub opens a file that the other wrote.
- The push URL guard (`internal/pushguard`): the config-time and send-time address checks and the allow list.
- The protocol package `@orswn/a2a-hub-protocol` with the JSON Schema of link protocol 2.
- The black-box contract suite of the link protocol.

Planned:

- The gateway to external agents and the external agent file.
- The `a2a` host CLI and the MCP endpoint for Claude Code and Codex.
- A released container image. CI builds it now but pushes it only on a `v*` tag.

## Build

Go and Node versions are in `mise.toml`.

```sh
make build            # bin/a2a-hub, static, CGO_ENABLED=0
```

## Run

```sh
bin/a2a-hub --listen 127.0.0.1:8080 --data ./data --tokens ./tokens.json
```

The hub prints `listening <port>` on stdout when it accepts connections. Logs go to stderr.

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--listen <host:port>` | `HUB_LISTEN` | `0.0.0.0:8080` | Listen address. Port `0` picks a free port. An IPv6 host may be in brackets. |
| `--data <dir>` | `HUB_DATA` | `/data` | Data directory with `hub.sqlite`. `:memory:` keeps the database in memory. |
| `--tokens <file>` | `HUB_TOKENS_FILE` | none | Token file. Without it, only `/healthz` and agent cards answer. |
| `--external <file>` | `HUB_EXTERNAL_FILE` | none | External A2A agents. Read in a later version. |
| `--test-agents` | `HUB_TEST_AGENTS=1` | off | Test mode: adds the built-in agents `tck` and `echo`, and adds loopback to the push allow list. |
| `--public-url <url>` | `HUB_PUBLIC_URL` | none | Base URL in agent cards. Must start with `http://` or `https://`. |
| `--push-allow <list>` | `HUB_PUSH_ALLOW` | empty | Comma list of host names, addresses, and CIDRs that push webhooks may reach although they are private. |
| | `HUB_PRESENCE_GRACE_MS` | `20000` | Time before a closed link stream marks its agent offline. |
| | `HUB_HEARTBEAT_MS` | `15000` | Interval of the link stream heartbeat. |

A flag wins over its env variable.

Exit codes: `0` after `SIGTERM` or `SIGINT`, `1` for a bad command line or a start failure, `2` for a bad setting value. `SIGHUP` reloads the token file and keeps the old tokens if the new file is bad.

The token file maps a principal name to its kind and the sha256 hex digest of its token:

```json
{
	"mars": { "kind": "host", "sha256": "<sha256 hex of the token>" },
	"hermes": { "kind": "peer", "sha256": "<sha256 hex of the token>" }
}
```

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
- Database compatibility: the engine tests run one scenario that `internal/engine/testdata/compat.mjs` also runs with the TypeScript engine. Both must write the same rows, and the Go engine must read the committed TypeScript-written file in `testdata/compat-node` the same way. With `PIMON_DIR` set to a pimon checkout and `node` on `PATH`, the test also runs the TypeScript engine again and makes it read the file that the Go engine wrote.
- Protocol package: `npm --prefix protocol test` checks that the committed JSON Schema matches the TypeBox source and that protocol 2 has not changed.
- Contract suite: `npm --prefix test run contract` starts `bin/a2a-hub` (or env `HUB_BIN`) and checks it over HTTP and SSE against the JSON Schema.
- Process suites: `npm --prefix test run process` starts the same binary and checks the A2A face with the `@a2a-js/sdk` client, the link API, push webhooks, and a restart. With `HUB_BIN` set to a wrapper of the TypeScript hub, both suites also run against it.
- Conformance: CI runs a2a-tck against `/a2a/tck` of a hub with `--test-agents`. `scripts/tck-gate.py` fails the job on a failed MUST requirement or fewer than 75 MUST passes. `scripts/hub-python-client.py` then sends a task with the Python A2A SDK.

## Known wire differences from the TypeScript hub

- A path with a percent escape that is not hex, such as `/agents/%zz`, gets `400` with a `text/plain` body from Go's HTTP server. The TypeScript hub answers `400` with `{"error":"malformed percent escape in path"}`. An escape that decodes to invalid UTF-8, such as `%ff`, gets the JSON body in both hubs.
- Go's HTTP server sends no `Connection: keep-alive` and no `Keep-Alive: timeout=65` header. Both are the HTTP/1.1 default, and the idle timeout is 65 s as in the TypeScript hub.
- A `413` answer to a body that is larger than 1 MB has `Connection: close`, and Go's HTTP server closes the connection. The TypeScript hub keeps it open.
- In A2A responses, the keys of struct values (`metadata` and data parts) are in sorted order: a2a-go decodes them into Go maps. The stored message, the link events, and the push payloads keep the order of the client. The TypeScript hub keeps the order of the client everywhere.
- A JSON-RPC body that is a JSON string is parsed again, as in `@a2a-js/sdk`. When that fails, the error text is `Failed to parse JSON request.`. `@a2a-js/sdk` sends the text of the V8 `SyntaxError`.
- The path splits into its non-empty segments with no other normalization. `GET //healthz` gets `200`, and `GET /x/../healthz` gets `404`. The TypeScript hub parses the request target as a WHATWG URL: `//healthz` names the host `healthz` and gets `404`, and `/x/../healthz` loses its dot segments and gets `200`.

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

Entry points: `@orswn/a2a-hub-protocol` (constants and types), `/schema`, `/routes`, and `/hub-protocol-v2.json`. The package is TypeScript source. `typebox` is a peer dependency.
