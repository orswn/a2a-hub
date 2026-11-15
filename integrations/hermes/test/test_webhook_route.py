import json
import os
import shutil
import socket
from pathlib import Path

import webhook_vector as vec

# The Hermes side of the hub push, as personal-infra renders it into the managed config.
MANAGED_CONFIG = """\
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
          prompt: "\\u21a9 {from.name}: {text}"
"""


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def run_route(run_hermes, tmp_path):
    managed = tmp_path / "managed" / "config.yaml"
    override = os.environ.get("HERMES_MANAGED_CONFIG")
    if override:
        shutil.copyfile(override, managed)
    else:
        managed.write_text(MANAGED_CONFIG, encoding="utf-8")
    return run_hermes("webhook_scenario.py", env={
        "WEBHOOK_ENABLED": "true",
        "WEBHOOK_PORT": str(free_port()),
        "WEBHOOK_SECRET": vec.SECRET,
        "TELEGRAM_BOT_TOKEN": "123456789:AAtest-token-not-real-0000000000000",
        "TELEGRAM_HOME_CHANNEL": "777",
    })


def test_route_config_loads_with_secret_from_env(run_hermes, tmp_path):
    out = run_route(run_hermes, tmp_path)
    assert out["enabled"] is True
    assert out["route"]["deliver_only"] is True
    assert out["route"]["mirror_to_session"] is True
    assert out["route_has_secret"] is False
    assert out["global_secret_set"] is True
    assert out["home_channel"] == "777"


def test_origin_topic_reaches_telegram_send_and_topic_session(run_hermes, tmp_path):
    out = run_route(run_hermes, tmp_path)
    assert out["responses"]["topic"]["status"] == 200
    assert out["responses"]["topic"]["json"]["status"] == "delivered"
    assert out["sends"][0] == {"chat_id": "-100123", "content": "\u21a9 mars: done in topic",
                               "metadata": {"thread_id": "42"}}
    mirrored = "[Webhook delivery: a2a-hub]\n\u21a9 mars: done in topic"
    assert {"role": "user", "content": mirrored} in out["sessions"]["s-topic-42"]
    assert all(m["content"] != mirrored for m in out["sessions"]["s-topic-43"])


def test_empty_thread_sends_to_the_chat_without_thread(run_hermes, tmp_path):
    out = run_route(run_hermes, tmp_path)
    assert out["responses"]["no_thread"]["status"] == 200
    assert out["sends"][1] == {"chat_id": "-100123", "content": "\u21a9 mars: done no thread", "metadata": None}


def test_empty_origin_goes_to_the_home_channel(run_hermes, tmp_path):
    out = run_route(run_hermes, tmp_path)
    assert out["responses"]["no_origin"]["status"] == 200
    assert out["sends"][2] == {"chat_id": "777", "content": "\u21a9 mars: done no origin", "metadata": None}
    assert {"role": "user", "content": "[Webhook delivery: a2a-hub]\n\u21a9 mars: done no origin"} \
        in out["sessions"]["s-home"]


def test_null_origin_is_not_a_home_channel_fallback(run_hermes, tmp_path):
    # Hermes leaves an unresolved placeholder as literal text, so the hub must send "" instead of null.
    out = run_route(run_hermes, tmp_path)
    send = next(s for s in out["sends"] if s["content"].endswith("null origin"))
    assert send["chat_id"] == "{origin.chat_id}"
    assert send["metadata"] == {"thread_id": "{origin.thread_id}"}


def test_repeated_request_id_is_not_delivered_twice(run_hermes, tmp_path):
    out = run_route(run_hermes, tmp_path)
    assert out["responses"]["duplicate"]["json"]["status"] == "duplicate"
    assert [s["content"] for s in out["sends"]].count("\u21a9 mars: done in topic") == 1


def test_wrong_signature_and_stale_timestamp_are_rejected(run_hermes, tmp_path):
    out = run_route(run_hermes, tmp_path)
    assert out["responses"]["bad_signature"]["status"] == 401
    assert out["responses"]["stale"]["status"] == 401
    assert not any(s["content"].endswith(("forged", "stale")) for s in out["sends"])


def test_fixed_vector_is_accepted(run_hermes, tmp_path):
    out = run_route(run_hermes, tmp_path)
    assert out["responses"]["vector"]["status"] == 200
    assert out["sends"][-1] == {"chat_id": "-100123", "content": "\u21a9 mars: Build passed on mars \u2714",
                                "metadata": {"thread_id": "42"}}


def test_committed_vector_file_matches_the_generator():
    committed = json.loads((Path(__file__).parent / "webhook-vector.json").read_text(encoding="utf-8"))
    assert committed == vec.vector()
