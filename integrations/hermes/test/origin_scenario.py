"""Run tool calls through the Hermes source tree in a fresh process; print the results as JSON.

argv[1] is the session: "topic" (a Telegram forum topic message), "dm" (a Telegram private chat),
or "none" (no gateway session). HERMES_HOME holds config.yaml and plugins/a2a_hub_origin.
"""

import json
import sys
from types import SimpleNamespace


def telegram_source(session):
    from plugins.platforms.telegram.adapter import TelegramAdapter

    if session == "topic":
        chat = SimpleNamespace(id=-100123, type="supergroup", is_forum=True)
        message = SimpleNamespace(chat=chat, message_thread_id=42, is_topic_message=True,
                                  from_user=SimpleNamespace(id=555, is_bot=False, username="okka"))
    else:
        chat = SimpleNamespace(id=555, type="private", is_forum=False)
        message = SimpleNamespace(chat=chat, message_thread_id=None, is_topic_message=False,
                                  from_user=SimpleNamespace(id=555, is_bot=False, username="okka"))
    adapter = SimpleNamespace(_normalize_chat_type=TelegramAdapter._normalize_chat_type)
    return TelegramAdapter._source_from_message_for_auth(adapter, message)


def bind_session(session):
    if session == "none":
        return None
    from gateway.run import GatewayRunner
    from gateway.session import SessionContext

    source = telegram_source(session)
    context = SessionContext(source=source, connected_platforms=[], home_channels={},
                             session_key="test-key", session_id="test-session")
    return GatewayRunner._set_session_env(SimpleNamespace(adapters={}), context)


def main():
    session = sys.argv[1]
    from hermes_cli.plugins import discover_plugins, get_plugin_manager

    discover_plugins()
    plugins = {p["name"]: p["enabled"] for p in get_plugin_manager().list_plugins()}

    from tools.mcp_tool_discovery import discover_mcp_tools

    tools = sorted(discover_mcp_tools())
    bind_session(session)

    from gateway.session_context import get_session_env
    import model_tools

    env = {name: get_session_env(name) for name in
           ("HERMES_SESSION_PLATFORM", "HERMES_SESSION_CHAT_ID", "HERMES_SESSION_THREAD_ID")}
    results = {
        "send": model_tools.handle_function_call(
            "mcp__a2a_hub__send", {"to": "mars", "body": "hello"}, task_id="t1", session_id="test-session"),
        "peers": model_tools.handle_function_call(
            "mcp__a2a_hub__peers", {}, task_id="t1", session_id="test-session"),
    }

    from tools.mcp_tool_lifecycle import shutdown_mcp_servers

    shutdown_mcp_servers()
    print(json.dumps({"plugins": plugins, "tools": tools, "env": env, "results": results}))


if __name__ == "__main__":
    main()
