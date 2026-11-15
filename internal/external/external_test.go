package external

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func parseValue(t *testing.T, value any) ([]Agent, error) {
	t.Helper()
	source, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return Parse(source)
}

type entry = map[string]any

func TestReadsOneAgentWithDefaults(t *testing.T) {
	agents, err := parseValue(t, entry{"hermes": entry{"url": "http://hermes:9900/", "token": "t"}})
	if err != nil {
		t.Fatal(err)
	}
	want := []Agent{{
		ID: "hermes", Name: "hermes", URL: "http://hermes:9900/", Token: "t",
		Description: "hermes: an external A2A agent",
		Poll:        10 * time.Second, CardTTL: 600 * time.Second, ReplyTimeout: 960 * time.Second,
		Idle: 30 * time.Second, Retry: 60 * time.Second, Offline: 600 * time.Second,
	}}
	if !reflect.DeepEqual(agents, want) {
		t.Errorf("got %+v", agents)
	}
	if Defaults["replyTimeoutMs"] != 960_000 {
		t.Errorf("replyTimeoutMs default %d", Defaults["replyTimeoutMs"])
	}
}

func TestRefusesABadFile(t *testing.T) {
	if _, err := Parse([]byte("{")); err == nil {
		t.Error("took {")
	}
	cases := []struct {
		value any
		want  string
	}{
		{[]any{}, "JSON object"},
		{entry{"bad id": entry{"url": "http://h/", "token": "t"}}, "invalid characters"},
		{entry{"h": entry{"token": "t"}}, "url must be"},
		{entry{"h": entry{"url": "ftp://h/", "token": "t"}}, "http or https"},
		{entry{"h": entry{"url": "http://h/"}}, "token must be"},
		{entry{"h": entry{"url": "http://h/", "token": "t", "pollMs": 0}}, "pollMs must be"},
		{entry{"h": entry{"url": "http://h/", "token": "t", "tokenEnv": "X"}}, "unknown field tokenEnv"},
		{entry{"a": entry{"url": "http://h/", "token": "t", "name": "x"}, "b": entry{"url": "http://h/", "token": "t", "name": "x"}}, "used twice"},
	}
	for _, c := range cases {
		_, err := parseValue(t, c.value)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%v: got %v, want %q", c.value, err, c.want)
		}
	}
}

func TestKeepsTheTokenOutOfEveryParseError(t *testing.T) {
	failure := func(source string) string {
		if _, err := Parse([]byte(source)); err != nil {
			return "Error: " + err.Error()
		}
		return "no error"
	}
	marshal := func(token string) string {
		text, _ := json.Marshal(entry{"h": entry{"url": "http://h/", "token": token}})
		return string(text)
	}
	broken := failure(`{ "h": { "url": "http://h/", "token": hX9secretTOKEN } }`)
	if broken != "Error: external file is not valid JSON" {
		t.Errorf("broken %q", broken)
	}
	const ascii = "Error: external agent h: token must be visible ASCII"
	control := failure(marshal("sec\nretTOKEN"))
	spaced := failure(marshal("sec retTOKEN"))
	padded := failure(marshal(" secretTOKEN "))
	for _, got := range []string{control, spaced, padded} {
		if got != ascii {
			t.Errorf("got %q", got)
		}
	}
	for _, got := range []string{broken, control, spaced} {
		if strings.Contains(got, "secret") || strings.Contains(got, "TOKEN") {
			t.Errorf("token in %q", got)
		}
	}
}

func TestRefusesANameThatNoA2AAliasURLCanReach(t *testing.T) {
	named := func(name string) ([]Agent, error) {
		return parseValue(t, entry{"h": entry{"url": "http://h/", "token": "t", "name": name}})
	}
	for _, name := range []string{"two words", "a/b", "@h", "-h"} {
		if _, err := named(name); err == nil || !strings.Contains(err.Error(), "name must match") {
			t.Errorf("%q: %v", name, err)
		}
	}
	agents, err := named("Hermes.2")
	if err != nil || len(agents) != 1 || agents[0].Name != "Hermes.2" {
		t.Errorf("Hermes.2: %v %+v", err, agents)
	}
}

func TestFindsTheCardNextToTheJSONRPCURL(t *testing.T) {
	for raw, want := range map[string]string{
		"http://hermes:9900/": "http://hermes:9900/.well-known/agent-card.json",
		"http://h:1/a2a/x":    "http://h:1/a2a/x/.well-known/agent-card.json",
	} {
		if got := CardURL(raw); got != want {
			t.Errorf("CardURL(%q) = %q", raw, got)
		}
	}
}
