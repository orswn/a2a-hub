import json

from fake_hub_mcp import FakeHubMCP


def write_config(hermes_home, url):
    config = {
        "plugins": {"enabled": ["a2a_hub_origin"]},
        "mcp_servers": {
            "a2a-hub": {
                "url": url,
                "headers": {"Authorization": "Bearer ${HUB_TEST_TOKEN}", "X-A2A-Harness": "hermes"},
                "timeout": 900,
            },
        },
    }
    # JSON is valid YAML.
    (hermes_home / "config.yaml").write_text(json.dumps(config), encoding="utf-8")


def run_session(run_hermes, hermes_home, session):
    with FakeHubMCP() as hub:
        write_config(hermes_home, hub.url)
        out = run_hermes("origin_scenario.py", session, env={"HUB_TEST_TOKEN": "test-token"})
        return out, hub.calls, hub.initialize_headers


def test_plugin_loads_and_the_hub_tools_get_their_hermes_names(run_hermes, hermes_home):
    out, _, headers = run_session(run_hermes, hermes_home, "none")
    assert out["plugins"]["a2a_hub_origin"] is True
    assert "mcp__a2a_hub__send" in out["tools"]
    assert "mcp__a2a_hub__peers" in out["tools"]
    assert headers[0]["authorization"] == "Bearer test-token"
    assert headers[0]["x-a2a-harness"] == "hermes"


def test_forum_topic_session_adds_origin_with_thread_to_send(run_hermes, hermes_home):
    out, calls, _ = run_session(run_hermes, hermes_home, "topic")
    assert out["env"] == {"HERMES_SESSION_PLATFORM": "telegram", "HERMES_SESSION_CHAT_ID": "-100123",
                          "HERMES_SESSION_THREAD_ID": "42"}
    send = next(c for c in calls if c["name"] == "send")
    assert send["arguments"] == {"to": "mars", "body": "hello",
                                 "origin": {"platform": "telegram", "chat_id": "-100123", "thread_id": "42"}}


def test_private_chat_session_adds_origin_without_thread(run_hermes, hermes_home):
    _, calls, _ = run_session(run_hermes, hermes_home, "dm")
    send = next(c for c in calls if c["name"] == "send")
    assert send["arguments"]["origin"] == {"platform": "telegram", "chat_id": "555", "thread_id": None}


def test_other_hub_tools_keep_their_arguments(run_hermes, hermes_home):
    _, calls, _ = run_session(run_hermes, hermes_home, "topic")
    peers = next(c for c in calls if c["name"] == "peers")
    assert peers["arguments"] == {}


def test_missing_session_sends_without_origin(run_hermes, hermes_home):
    out, calls, _ = run_session(run_hermes, hermes_home, "none")
    assert out["env"] == {"HERMES_SESSION_PLATFORM": "", "HERMES_SESSION_CHAT_ID": "",
                          "HERMES_SESSION_THREAD_ID": ""}
    assert "queued" in out["results"]["send"]
    send = next(c for c in calls if c["name"] == "send")
    assert send["arguments"] == {"to": "mars", "body": "hello"}
