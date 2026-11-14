# Changelog

All notable changes to a2a-hub are listed here. Versions follow SemVer.

## [Unreleased]

### Added

- `a2a-hub`, the Go port of the pimon hub. It reads the same flags and `HUB_*` environment, loads the same token file, and opens the same `hub.sqlite` (`user_version` 2). It prints `listening <port>`, serves `GET /healthz`, and answers 404 for every other route. `SIGTERM` and `SIGINT` stop it. `SIGHUP` reloads the token file.
- `a2a-hub healthcheck` asks the local hub for `/healthz` and exits 0 when it answers 200.
- `@orswn/a2a-hub-protocol`: the TypeBox source, types, constants, route manifest, and JSON Schema of link protocol 2, unchanged from pimon.
- The black-box contract suite of the link protocol in `test/`.
- The hub engine, a port of the TypeScript engine with its tests: agents, tasks, messages, mailbox events, presence, retention, push configs and outbox, and external agent jobs. It writes the same sqlite rows as the TypeScript engine. Listeners run after the commit in commit order, and a link subscription reads its replay and subscribes under one lock.
- The push URL guard with the allow list.
- `GET /healthz` reports the online agent count.
- A distroless container image that compiles the binary in a Go build stage. Its `/data` belongs to the `nonroot` user. CI builds and runs it on every push and pull request. It pushes it to `ghcr.io/orswn/a2a-hub:vX.Y.Z` only on a `v*` tag.
