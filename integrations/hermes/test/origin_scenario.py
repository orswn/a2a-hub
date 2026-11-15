"""Run tool calls through the Hermes source tree in a fresh process; print the results as JSON.

argv[1] is the session:
- "topic": a Telegram forum topic message.
- "general": a Telegram forum message in the General topic.
- "group": a Telegram group without topics.
- "dm": a Telegram private chat.
- "api_server": an API server session.
- "none": no gateway session.
HERMES_HOME holds config.yaml and plugins/a2a_hub_origin.
"""

import json
import sys
from types import SimpleNamespace

SEND_ARGS = {"target": "mars", "text": "hello"}

TELEGRAM_CHATS = {
    "topic": (SimpleNamespace(id=-100123, type="supergroup", title="Ops", is_forum=True), 42, True),
    "general": (SimpleNamespace(id=-100123, type="supergroup", title="Ops", is_forum=True), None, False),
    "group": (SimpleNamespace(id=-100456, type="group", title="Team", is_forum=False), None, False),
    "dm": (SimpleNamespace(id=555, type="private", title=None, full_name="Okka", is_forum=False), None, False),
}


def telegram_adapter():
    from gateway.config import Platform, PlatformConfig
    from plugins.platforms.telegram.adapter import TelegramAdapter

    adapter = object.__new__(TelegramAdapter)
    adapter.platform = Platform.TELEGRAM
    adapter.config = PlatformConfig(enabled=True, token="***", extra={"allowed_topics": [], "allowed_chats": []})
    adapter._bot = SimpleNamespace(id=999, username="hermes_bot")
    adapter._mention_patterns = adapter._compile_mention_patterns()
    return adapter


def telegram_source(session):
    """The session source that a gateway turn builds from an inbound Telegram message."""
    from gateway.platforms.event import MessageType

    chat, thread_id, is_topic_message = TELEGRAM_CHATS[session]
    message = SimpleNamespace(
        message_id=7, text="hello", caption=None, entities=[], caption_entities=[],
        message_thread_id=thread_id, is_topic_message=is_topic_message, chat=chat,
        from_user=SimpleNamespace(id=555, is_bot=False, full_name="Okka", first_name="Okka", username="okka"),
        reply_to_message=None, quote=None, date=None)
    return telegram_adapter()._build_message_event(message, MessageType.TEXT).source


def bind_session(session):
    if session == "none":
        return
    if session == "api_server":
        from gateway.session_context import set_session_vars

        set_session_vars(platform="api_server", chat_id="api-session-1", session_id="api-session-1")
        return
    from gateway.run import GatewayRunner
    from gateway.session import SessionContext

    context = SessionContext(source=telegram_source(session), connected_platforms=[], home_channels={},
                             session_key="test-key", session_id="test-session")
    GatewayRunner._set_session_env(SimpleNamespace(adapters={}), context)


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
            "mcp__a2a_hub__send", dict(SEND_ARGS), task_id="t1", session_id="test-session"),
        "peers": model_tools.handle_function_call(
            "mcp__a2a_hub__peers", {}, task_id="t1", session_id="test-session"),
        # The model reaches a deferred MCP tool through the Tool Search bridge.
        "bridge_send": model_tools.handle_function_call(
            "tool_call", {"calls": [{"name": "mcp__a2a_hub__send", "arguments": {"target": "terra", "text": "via bridge"}}]},
            task_id="t1", session_id="test-session"),
    }

    from tools.mcp_tool_lifecycle import shutdown_mcp_servers

    shutdown_mcp_servers()
    print(json.dumps({"plugins": plugins, "tools": tools, "env": env, "results": results}))


if __name__ == "__main__":
    main()
