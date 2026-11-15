"""Drive the Hermes webhook adapter from the source tree with signed hub pushes; print JSON.

The gateway config comes from the real loader: HERMES_MANAGED_DIR/config.yaml and the WEBHOOK_*
and TELEGRAM_* environment. A fake Telegram adapter records each send. The mirror writes into a
real state.db in HERMES_HOME.
"""

import asyncio
import json
import os
import time
from types import SimpleNamespace
from unittest import mock

import webhook_vector as vec

CHAT = "-100123"
HOME_CHAT = "777"


class FakeTelegram:
    def __init__(self):
        self.sends = []
        self.failures_left = 0

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        from gateway.platforms.base import SendResult

        if self.failures_left:
            self.failures_left -= 1
            return SendResult(success=False, error="Bad Request: chat not found")
        self.sends.append({"chat_id": chat_id, "content": content, "metadata": metadata})
        return SendResult(success=True)


def seed_sessions():
    from hermes_state_registry import acquire, release_or_close

    db = acquire()
    try:
        db.create_session("s-topic-42", "telegram", session_key="k-topic-42", chat_id=CHAT,
                          chat_type="forum", thread_id="42", user_id="555")
        db.create_session("s-topic-43", "telegram", session_key="k-topic-43", chat_id=CHAT,
                          chat_type="forum", thread_id="43", user_id="555")
        db.create_session("s-home", "telegram", session_key="k-home", chat_id=HOME_CHAT,
                          chat_type="dm", user_id="555")
    finally:
        release_or_close(db)


def session_messages():
    from hermes_state_registry import acquire, release_or_close

    db = acquire()
    try:
        return {sid: [{"role": m["role"], "content": m["content"]} for m in db.get_messages(sid)]
                for sid in ("s-topic-42", "s-topic-43", "s-home")}
    finally:
        release_or_close(db)


def body(origin, text, message_id):
    return vec.body_bytes({**vec.BODY, "origin": origin, "text": text, "message_id": message_id})


async def post(session, url, data, headers):
    async with session.post(url, data=data, headers=headers) as resp:
        return {"status": resp.status, "json": await resp.json()}


async def main():
    import aiohttp
    from gateway.config import Platform, load_gateway_config
    from gateway.platforms.webhook import WebhookAdapter

    config = load_gateway_config()
    webhook = config.platforms[Platform.WEBHOOK]
    route = webhook.extra["routes"]["a2a-hub"]
    secret = os.environ["WEBHOOK_SECRET"]
    seed_sessions()

    telegram = FakeTelegram()
    adapter = WebhookAdapter(webhook)
    adapter.gateway_runner = SimpleNamespace(
        config=config,
        _authorization_adapter=lambda platform, profile=None: telegram if platform is Platform.TELEGRAM else None)
    adapter.handle_message = mock.AsyncMock(side_effect=AssertionError("deliver_only must not start a turn"))
    assert await adapter.connect()
    url = f"http://127.0.0.1:{adapter._port}/webhooks/a2a-hub"
    now = str(int(time.time()))
    topic = {"platform": "telegram", "chat_id": CHAT, "thread_id": "42"}
    no_thread = {"platform": "telegram", "chat_id": CHAT, "thread_id": ""}
    none = {"platform": "", "chat_id": "", "thread_id": ""}
    out = {"enabled": webhook.enabled, "route": {k: v for k, v in route.items() if k != "secret"},
           "route_has_secret": "secret" in route, "global_secret_set": bool(webhook.extra.get("secret")),
           "home_channel": getattr(config.get_home_channel(Platform.TELEGRAM), "chat_id", None),
           "responses": {}}
    try:
        async with aiohttp.ClientSession() as session:
            cases = [
                ("topic", body(topic, "done in topic", "m-topic"), secret, now, "m-topic"),
                ("no_thread", body(no_thread, "done no thread", "m-flat"), secret, now, "m-flat"),
                ("no_origin", body(none, "done no origin", "m-home"), secret, now, "m-home"),
                ("null_origin", body(None, "null origin", "m-null"), secret, now, "m-null"),
                ("duplicate", body(topic, "done in topic", "m-topic"), secret, now, "m-topic"),
                ("bad_signature", body(topic, "forged", "m-bad"), "wrong-secret", now, "m-bad"),
                ("stale", body(topic, "stale", "m-stale"), secret, str(int(now) - 400), "m-stale"),
            ]
            for name, data, key, ts, request_id in cases:
                out["responses"][name] = await post(session, url, data, vec.headers(key, ts, data, request_id))
            # One failed Telegram send, then the same delivery id again, then a new id for the same message.
            retry = body(topic, "done after retry", "m-retry")
            telegram.failures_left = 1
            for name, request_id in (("failed", "m-retry"), ("failed_same_id", "m-retry"), ("failed_new_id", "m-retry.r1")):
                out["responses"][name] = await post(session, url, retry, vec.headers(secret, now, retry, request_id))
            # The fixed vector, with the Hermes clock set 5 s after its timestamp.
            v = vec.vector()
            with mock.patch("gateway.platforms.webhook.time.time", return_value=int(v["timestamp"]) + 5):
                out["responses"]["vector"] = await post(session, url, v["body"].encode(), v["headers"])
    finally:
        await adapter.disconnect()
    out["sends"] = telegram.sends
    out["sessions"] = session_messages()
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
