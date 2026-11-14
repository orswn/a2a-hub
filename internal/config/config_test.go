package config

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func envOf(values map[string]string) Env {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}

func mustLoad(t *testing.T, args []string, env map[string]string) Config {
	t.Helper()
	cfg, err := Load(args, envOf(env))
	if err != nil {
		t.Fatalf("Load(%q): %v", args, err)
	}
	return cfg
}

func wantConfigError(t *testing.T, args []string, env map[string]string) {
	t.Helper()
	_, err := Load(args, envOf(env))
	var configErr *Error
	if !errors.As(err, &configErr) {
		t.Fatalf("Load(%q): want a config error, got %v", args, err)
	}
}

func TestDefaults(t *testing.T) {
	got := mustLoad(t, nil, nil)
	want := Config{
		Host:          "0.0.0.0",
		Port:          8080,
		Data:          "/data",
		PresenceGrace: 20 * time.Second,
		Heartbeat:     15 * time.Second,
		PushAllow:     []string{},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestPushAllowFromEnvOrFlagWithLoopbackInTestMode(t *testing.T) {
	cases := []struct {
		args []string
		env  map[string]string
		want []string
	}{
		{nil, map[string]string{"HUB_PUSH_ALLOW": "hooks.example,10.0.0.0/8"}, []string{"hooks.example", "10.0.0.0/8"}},
		{[]string{"--push-allow", "192.168.1.0/24"}, map[string]string{"HUB_PUSH_ALLOW": "x.example"}, []string{"192.168.1.0/24"}},
		{[]string{"--test-agents", "--push-allow", "x.example"}, nil, []string{"x.example", "127.0.0.0/8", "::1"}},
	}
	for _, c := range cases {
		if got := mustLoad(t, c.args, c.env).PushAllow; !reflect.DeepEqual(got, c.want) {
			t.Errorf("Load(%q, %v).PushAllow = %q, want %q", c.args, c.env, got, c.want)
		}
	}
	wantConfigError(t, []string{"--push-allow", "10.0.0.0/40"}, nil)
}

func TestEnvAndFlagsWin(t *testing.T) {
	env := map[string]string{
		"HUB_LISTEN":        "127.0.0.1:9000",
		"HUB_DATA":          "/env",
		"HUB_TOKENS_FILE":   "/t.json",
		"HUB_TEST_AGENTS":   "1",
		"HUB_PUBLIC_URL":    "https://hub.example/",
		"HUB_EXTERNAL_FILE": "/e.json",
	}
	if got := mustLoad(t, []string{"--external", "/flag-e.json"}, env).ExternalFile; got != "/flag-e.json" {
		t.Errorf("ExternalFile = %q, want the flag value", got)
	}
	got := mustLoad(t, nil, env)
	if got.ExternalFile != "/e.json" || got.Host != "127.0.0.1" || got.Port != 9000 || got.Data != "/env" ||
		got.TokensFile != "/t.json" || !got.TestAgents || got.PublicURL != "https://hub.example" {
		t.Errorf("env config = %+v", got)
	}
	got = mustLoad(t, []string{"--listen", "[::1]:0", "--data", "/flag"}, env)
	if got.Host != "::1" || got.Port != 0 || got.Data != "/flag" {
		t.Errorf("flag config = %+v", got)
	}
}

func TestRefusesBadListenOrPublicURL(t *testing.T) {
	wantConfigError(t, []string{"--listen", "8080"}, nil)
	wantConfigError(t, []string{"--listen", "h:99999"}, nil)
	wantConfigError(t, []string{"--public-url", "hub.example"}, nil)
}

func TestTimersFromEnv(t *testing.T) {
	got := mustLoad(t, nil, map[string]string{"HUB_PRESENCE_GRACE_MS": "300", "HUB_HEARTBEAT_MS": ""})
	if got.PresenceGrace != 300*time.Millisecond || got.Heartbeat != 15*time.Second {
		t.Errorf("timers = %v %v", got.PresenceGrace, got.Heartbeat)
	}
	for _, bad := range []string{"0", "-5", "1.5", "abc", " "} {
		wantConfigError(t, nil, map[string]string{"HUB_HEARTBEAT_MS": bad})
	}
}

func TestEmptyEnvValues(t *testing.T) {
	got := mustLoad(t, nil, map[string]string{"HUB_DATA": "", "HUB_TOKENS_FILE": "", "HUB_PUBLIC_URL": "", "HUB_TEST_AGENTS": "true"})
	if got.Data != "" || got.TokensFile != "" || got.PublicURL != "" || got.TestAgents {
		t.Errorf("config = %+v", got)
	}
}

func TestArgumentErrorsAreNotConfigErrors(t *testing.T) {
	for _, args := range [][]string{
		{"--nope"},
		{"stray"},
		{"--listen"},
		{"--listen", "--data", "/x"},
		{"--test-agents=1"},
		{"--", "x"},
		{"-l", "x"},
	} {
		_, err := Load(args, envOf(nil))
		var argErr *ArgError
		if !errors.As(err, &argErr) {
			t.Errorf("Load(%q): want an argument error, got %v", args, err)
		}
	}
	got := mustLoad(t, []string{"--listen=127.0.0.1:1", "--data", "-", "--test-agents", "--"}, nil)
	if got.Port != 1 || got.Data != "-" || !got.TestAgents {
		t.Errorf("config = %+v", got)
	}
}

func TestParseListen(t *testing.T) {
	cases := map[string]struct {
		host string
		port int
	}{
		"0.0.0.0:8080":  {"0.0.0.0", 8080},
		"[::]:80":       {"::", 80},
		"localhost:":    {"localhost", 0},
		"h:0x10":        {"h", 16},
		"h: 7 ":         {"h", 7},
		"a:b:c:1e3":     {"a:b:c", 1000},
		"127.0.0.1:-0":  {"127.0.0.1", 0},
		"[fe80::1]:443": {"fe80::1", 443},
	}
	for in, want := range cases {
		host, port, err := ParseListen(in)
		if err != nil || host != want.host || port != want.port {
			t.Errorf("ParseListen(%q) = %q, %d, %v; want %q, %d", in, host, port, err, want.host, want.port)
		}
	}
	for _, bad := range []string{"8080", ":80", "[]:80", "h:-1", "h:65536", "h:1.5", "h:x", "h:Infinity"} {
		if _, _, err := ParseListen(bad); err == nil {
			t.Errorf("ParseListen(%q): want an error", bad)
		}
	}
}

func TestParsePushAllow(t *testing.T) {
	got, err := ParsePushAllow(" Hooks.Example. , ,::1,fd00::/8,10.1.2.3,a_b.c ")
	want := []string{"hooks.example.", "::1", "fd00::/8", "10.1.2.3", "a_b.c"}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("ParsePushAllow = %q, %v; want %q", got, err, want)
	}
	if got, err := ParsePushAllow(""); err != nil || len(got) != 0 {
		t.Errorf("ParsePushAllow(\"\") = %q, %v", got, err)
	}
	for _, bad := range []string{"10.0.0.0/33", "::/129", "x/8", "1.2.3.4/", "1.2.3.4/1234", "-bad.example", "a..b", "exa mple"} {
		if _, err := ParsePushAllow(bad); err == nil {
			t.Errorf("ParsePushAllow(%q): want an error", bad)
		}
	}
}
