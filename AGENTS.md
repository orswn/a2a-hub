# a2a-hub

The A2A 1.0 hub for agent harness sessions (pi, Claude Code, Codex) and external A2A agents. One Go module, `github.com/orswn/a2a-hub`, with one static binary. The repository root `package.json` is the npm package `@orswn/a2a-hub-protocol`.

## Commands

- `make verify`: gofmt check, `go vet`, `golangci-lint`, `go test -race`, the protocol package tests, and the contract and process suites against a binary built from the current sources. Run it before each commit.
- `make build`: the static binary `bin/a2a-hub`. The version comes from `git describe`.
- `mise exec -- go test ./...`: the Go unit tests only.
- `npm --prefix protocol run schema`: write `protocol/schema/hub-protocol-v<N>.json` after a change in `protocol/src`.
- `npm --prefix protocol test`: schema drift check, type check, and protocol tests.
- `npm --prefix test run contract`: the contract suite against `bin/a2a-hub`, or against env `HUB_BIN`.
- `npm --prefix test run process`: the process suites (A2A face, link API, restart) against the same binary.
- `make vuln`: `govulncheck`.
- `make containerfile-lint`: `hadolint` on the `Containerfile`.

Go and Node come from `mise.toml`. Run Go through `mise exec --`. Install tools only through `mise exec` or as `go tool` directives, never globally.

## Layout

- `cmd/a2a-hub/`: the server. Flags and env, signals, `listening <port>`, and the `healthcheck` subcommand.
- `internal/config/`: flags, `HUB_*` env, defaults.
- `internal/auth/`: token file and bearer lookup.
- `internal/store/`: sqlite open, schema, migrations, table rebuild.
- `internal/engine/`: agents, tasks, messages, mailbox events, push outbox. All database access goes through it.
- `internal/a2aapi/`: the A2A request handler on a2a-go types, agent cards, hold limits, delivery states.
- `internal/server/`: the HTTP front: routes, JSON-RPC and SSE framing, link routes.
- `internal/link/`: link event streams, heartbeat, and presence.
- `internal/push/`: the push sender. `internal/pushguard/`: the push address checks.
- `internal/testagents/`: the `tck` and `echo` agents of `--test-agents`.
- `protocol/`: TypeBox source, constants, route manifest, generator, and the committed JSON Schema of the link protocol.
- `test/`: TypeScript workspace with the black-box contract and process suites.
- `scripts/`: `tck-gate.py` and `hub-python-client.py` for the conformance job.

## Code rules

- `CGO_ENABLED=0`. Every dependency must build without cgo.
- Comments only for a non-obvious why: a hidden constraint, an invariant, a workaround. One or two lines.
- Deferred work gets a `FIXME(okka): ...` comment.
- Go unit tests live next to the package. Inject a clock and an id generator where a test needs fixed times or ids.
- Store migrations are append-only. Never edit a released migration. Each migration runs with foreign keys off, so a `DROP TABLE` does not cascade. `PRAGMA foreign_key_check` must then find no row, else the migration rolls back. The error names the table and rowid of each orphan row.

## Wire parity

- The committed JSON Schema (`protocol/schema/hub-protocol-v<N>.json`) and the contract suite (`test/contract/`) are the spec of the link protocol.
- A wire change needs a protocol bump: a new `HUB_PROTOCOL`, a new schema file, and a contract suite for it. The old file stays while the hub accepts its clients.
- Never edit a schema file by hand. Change `protocol/src` and run `npm --prefix protocol run schema`.
- Flags, `HUB_*` env names, defaults, exit codes, and the stdout line `listening <port>` stay the same as the TypeScript hub.
- The sqlite file stays compatible with the TypeScript hub until the cutover is verified.

## Model-visible text

- Model-visible text is all text that the hub sends to a model at run time: the MCP `instructions`, the `tools/list` names, descriptions, and input schemas, tool result wording, and rendered messages.
- A snapshot test records the `tools/list` result and the `instructions` with their char counts. The commit that adds `/mcp` adds this test.
- A change of model-visible text updates the snapshot. Its commit body states the char counts before and after.
- Keep this text short. Clients send tool schemas with each model request.

## Tests

- Each bug fix starts with a failing regression test that describes the behavior. Test names carry no issue ids.
- No test calls a real LLM.
- No test may reach the human: no desktop or phone notification, terminal multiplexer, `sudo`, or live agent session.
- Each test owns its temp dirs (`t.TempDir()`, `tempDir()`). No test writes outside them.
- Tests in `test/` are black-box: they talk to the hub binary only over HTTP and SSE. They import no Go code and no value from `protocol/src`.

## Releases

- SemVer. `CHANGELOG.md` headings are `## [x.y.z] - YYYY-MM-DD`.
- A release is one `chore(release): vX.Y.Z` commit and a signed `vX.Y.Z` tag on it. Steps:
  1. Move the `## [Unreleased]` entries to `## [X.Y.Z] - YYYY-MM-DD` in `CHANGELOG.md`.
  2. Set `"version"` in the root `package.json` to `X.Y.Z`, equal to the tag without `v`. Clients install `@orswn/a2a-hub-protocol` from that tag.
  3. Run `make verify`.
  4. Commit as `chore(release): vX.Y.Z`, then `git tag -s vX.Y.Z`.
- The binary takes its version from the tag through `-ldflags "-X main.version=<version>"`.
- Commit headers follow Conventional Commits. Bodies follow the Linux kernel style.
