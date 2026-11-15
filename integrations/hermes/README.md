# Hermes integration

Hermes Agent uses the hub through its MCP client: the hub `/mcp` endpoint with the header `X-A2A-Harness: hermes`. The hub pushes each reply to a Hermes `send` back to the chat that sent it, through a Hermes webhook route. This directory holds the Hermes side of that path and its tests. The checks below ran against Hermes Agent v0.23.1.

## Plugin `a2a_hub_origin`

The plugin adds the chat of the current turn to each hub `send` call. Hermes knows the chat per turn, but its MCP client does not send it.

- Hook: `pre_tool_call`. It returns `{"action": "modify", "args": {"origin": ...}}`. Hermes merges these keys into the call arguments before the dispatch.
- Tool: `mcp__a2a_hub__send` only. Hermes names an MCP tool `mcp__<server>__<tool>` and replaces each char outside `[A-Za-z0-9_]` with `_`. The server name must be `a2a-hub`.
- Origin: `{"platform": HERMES_SESSION_PLATFORM, "chat_id": HERMES_SESSION_CHAT_ID, "thread_id": HERMES_SESSION_THREAD_ID or null}`, read with `gateway.session_context.get_session_env`.
- A Telegram forum topic or DM topic gives `thread_id`. A chat without a topic gives `null`.
- A turn without a platform or chat (WebUI, CLI, cron) sends no `origin`. The hub then pushes to the home channel.
- The hook never raises. Hermes blocks the tool call when a `pre_tool_call` hook raises.
- `origin` is not in the hub tool schema. The model does not see it.

Install:

1. Copy `a2a_hub_origin/` to `$HERMES_HOME/plugins/a2a_hub_origin/`.
2. Add `a2a_hub_origin` to `plugins.enabled` in the Hermes config. Hermes loads no user plugin that is not in that list.
3. Restart the gateway. Hermes discovers plugins at start.

## Webhook route

The hub signs each push with the Hermes generic HMAC scheme V2. Hermes checks it in `gateway/platforms/webhook.py`.

- Header `X-Webhook-Timestamp`: Unix time in seconds. Hermes rejects a value more than 300 s from its clock.
- Header `X-Webhook-Signature-V2`: hex HMAC-SHA256 of `<timestamp>.<raw body>` with the route secret.
- Header `X-Request-ID`: the delivery id. A `deliver_only` route answers a repeated id with `duplicate` for one hour.
- `test/webhook-vector.json` is a fixed vector that Hermes accepts.

The route renders `deliver_extra` from the body. `origin` must be an object of three strings, with `""` for an unknown value:

- `chat_id` `""` sends to the Telegram home channel.
- `thread_id` `""` sends without a topic.
- `null` is not safe. Hermes keeps an unresolved `{origin.chat_id}` as literal text.

Route config, for example in the Hermes managed config, with the secret in `WEBHOOK_SECRET`:

```yaml
platforms:
  webhook:
    extra:
      routes:
        a2a-hub:
          deliver: telegram
          deliver_only: true
          mirror_to_session: true
          deliver_extra:
            chat_id: "{origin.chat_id}"
            message_thread_id: "{origin.thread_id}"
          prompt: "↩ {from.name}: {text}"
```

## Tests

The tests run the plugin and the webhook adapter from a Hermes source tree in a fresh process each. They use no model, no network, and no Telegram.

```bash
HERMES_SRC=/path/to/hermes-agent integrations/hermes/test/run.sh
```

- `run.sh` makes a venv from the Hermes `uv.lock` on the first run. `HERMES_TEST_VENV` sets its path.
- `test_origin_plugin.py`: Hermes loads the plugin and connects to a fake hub MCP server. The test calls the tools through `model_tools.handle_function_call` and reads the arguments on the wire.
- `test_webhook_route.py`: the real gateway config loader and webhook adapter take signed POSTs. A fake Telegram adapter records each send. The mirror writes into a real `state.db`.
- `HERMES_MANAGED_CONFIG=<file>` replaces the route config of the webhook test with a rendered managed config.
- `make verify` does not run these tests. They need a Hermes source tree and its Python dependencies.
