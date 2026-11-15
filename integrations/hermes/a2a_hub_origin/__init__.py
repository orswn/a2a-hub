"""Hermes plugin: add the chat origin to each send call of the a2a-hub MCP server.

The hub pushes a reply to a Hermes send back to the chat that sent it. Hermes knows that chat per
turn, but its MCP client does not send it, so this pre_tool_call hook adds it as the send argument
origin: {"platform", "chat_id", "thread_id"}. Without a Telegram chat in the session the call stays as
is, and the hub pushes the reply to the home channel.
"""

import logging
import os

logger = logging.getLogger(__name__)

# Hermes names an MCP tool mcp__<server>__<tool>, with each char outside [A-Za-z0-9_] as "_".
HUB_SEND_TOOL = "mcp__a2a_hub__send"
# The webhook route delivers to Telegram only. A chat id of another platform is not a Telegram chat.
ORIGIN_PLATFORM = "telegram"


def _session_env(name):
    try:
        from gateway.session_context import get_session_env
    except Exception:
        return os.getenv(name, "")
    return get_session_env(name, "")


def current_origin():
    """The origin of the current turn, or None when the session has no Telegram chat."""
    platform = str(_session_env("HERMES_SESSION_PLATFORM") or "").strip()
    chat_id = str(_session_env("HERMES_SESSION_CHAT_ID") or "").strip()
    if platform != ORIGIN_PLATFORM or not chat_id:
        return None
    thread_id = str(_session_env("HERMES_SESSION_THREAD_ID") or "").strip()
    return {"platform": platform, "chat_id": chat_id, "thread_id": thread_id or None}


def on_pre_tool_call(tool_name, args=None, **_):
    if tool_name != HUB_SEND_TOOL:
        return None
    # Hermes blocks the tool call when a pre_tool_call hook raises.
    try:
        origin = current_origin()
    except Exception:
        logger.warning("a2a_hub_origin: cannot read the session origin", exc_info=True)
        return None
    if origin is None:
        return None
    return {"action": "modify", "args": {"origin": origin}}


def register(ctx):
    ctx.register_hook("pre_tool_call", on_pre_tool_call)
