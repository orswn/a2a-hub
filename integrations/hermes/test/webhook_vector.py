"""The fixed signature vector of a hub push to the Hermes webhook route.

Hermes generic HMAC V2: X-Webhook-Signature-V2 is the hex HMAC-SHA256 of "<timestamp>.<body>"
with the route secret; X-Webhook-Timestamp is the Unix time in seconds. Hermes rejects a
timestamp more than 300 s from its clock. X-Request-ID is the delivery id that Hermes dedupes on.

Run as a script to print the vector as JSON. The secret is a test value only.
"""

import hashlib
import hmac
import json
import sys

SECRET = "5f2b8c1d9e4a7f3061b2c8d4e9f0a1b2c3d4e5f60718293a4b5c6d7e8f901234"
TIMESTAMP = "1800000000"
REQUEST_ID = "01HUBMSG0000000000000VECTOR"
BODY = {
    "event": "a2a-hub.message",
    "origin": {"platform": "telegram", "chat_id": "-100123", "thread_id": "42"},
    "from": {"id": "01a1b7750715", "name": "mars", "harness": "pi"},
    "kind": "result",
    "message_id": REQUEST_ID,
    "in_reply_to": "01HERMESSEND00000000000000",
    "hub_thread_id": "01HUBTHREAD000000000000000",
    "task_id": "task-1",
    "state": "completed",
    "text": "Build passed on mars \u2714",
}


def body_bytes(body=None):
    return json.dumps(BODY if body is None else body, ensure_ascii=False, separators=(",", ":")).encode()


def sign(secret, timestamp, body):
    return hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()


def headers(secret, timestamp, body, request_id):
    return {
        "Content-Type": "application/json",
        "X-Request-ID": request_id,
        "X-Webhook-Timestamp": timestamp,
        "X-Webhook-Signature-V2": sign(secret, timestamp, body),
    }


def vector():
    body = body_bytes()
    return {
        "scheme": "Hermes v0.23.1 generic HMAC V2 (gateway/platforms/webhook.py)",
        "signed_bytes": "<X-Webhook-Timestamp> + '.' + <raw body bytes>",
        "secret": SECRET,
        "timestamp": TIMESTAMP,
        "body": body.decode(),
        "body_sha256": hashlib.sha256(body).hexdigest(),
        "headers": headers(SECRET, TIMESTAMP, body, REQUEST_ID),
        "url_path": "/webhooks/a2a-hub",
        "replay_window_seconds": 300,
    }


if __name__ == "__main__":
    json.dump(vector(), sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
