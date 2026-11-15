package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
	"time"
)

type fakeSys struct {
	sys
	out, errOut bytes.Buffer
	env         map[string]string
	calls       [][]string
	active      map[string]bool
	execed      []string
}

func newSys(t *testing.T) *fakeSys {
	t.Helper()
	root := t.TempDir()
	f := &fakeSys{env: map[string]string{"XDG_RUNTIME_DIR": filepath.Join(root, "run")}, active: map[string]bool{}}
	f.sys = sys{
		getenv:  func(k string) string { return f.env[k] },
		stdin:   strings.NewReader(""),
		stdout:  &f.out,
		stderr:  &f.errOut,
		pid:     100,
		ppid:    4242,
		home:    filepath.Join(root, "home"),
		cwd:     "/work",
		host:    "mars",
		procDir: filepath.Join(root, "proc"),
		systemctl: func(args ...string) error {
			f.calls = append(f.calls, args)
			if args[0] == "is-active" && !f.active[args[2]] {
				return errors.New("inactive")
			}
			return nil
		},
		execve: func(path string, argv []string, _ []string) error {
			f.execed = append([]string{path}, argv...)
			return nil
		},
		environ:    func() []string { return nil },
		lookPath:   func(name string) (string, error) { return "/bin/" + name, nil },
		signals:    func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
		socketWait: 5 * time.Second,
	}
	stat := filepath.Join(f.procDir, "4242", "stat")
	if err := os.MkdirAll(filepath.Dir(stat), 0o755); err != nil {
		t.Fatal(err)
	}
	fields := strings.Repeat("0 ", 18) + "555 0 0"
	if err := os.WriteFile(stat, []byte("4242 (claude) S "+fields+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *fakeSys) writeToken(t *testing.T, mode os.FileMode) {
	t.Helper()
	path := filepath.Join(f.home, ".config", "a2a", "host.token")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

func TestTheHookAndTheHeadersHelperShareTheAgent(t *testing.T) {
	f := newSys(t)
	f.writeToken(t, 0o600)
	f.env["CLAUDE_PID"] = "4242"
	f.stdin = strings.NewReader(`{"session_id":"sess-1","source":"startup","cwd":"/proj","hook_event_name":"SessionStart"}`)
	if code := run([]string{"claude-hook"}, f.sys); code != 0 {
		t.Fatalf("hook exit %d: %s", code, f.errOut.String())
	}
	delete(f.env, "CLAUDE_PID")
	if code := run([]string{"claude-headers"}, f.sys); code != 0 {
		t.Fatalf("headers exit %d: %s", code, f.errOut.String())
	}
	var headers map[string]string
	if err := json.Unmarshal(f.out.Bytes(), &headers); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"Authorization": "Bearer secret", "X-A2A-Harness": "claude-code", "X-A2A-Agent": "sess-1",
		"X-A2A-Session": "sess-1", "X-A2A-Cwd": "%2Fproj",
	}
	if fmt.Sprint(headers) != fmt.Sprint(want) {
		t.Fatalf("headers = %v", headers)
	}
}

func TestTheHeadersHelperRefusesAnOpenToken(t *testing.T) {
	f := newSys(t)
	f.writeToken(t, 0o644)
	if code := run([]string{"claude-headers"}, f.sys); code != 1 || !strings.Contains(f.errOut.String(), "mode 0600") {
		t.Fatalf("exit %d: %s", code, f.errOut.String())
	}
	if f.out.Len() != 0 {
		t.Fatalf("stdout = %q", f.out.String())
	}
}

func TestTheLauncherStartsStoppedUnitsAndRunsCodexOnTheSocket(t *testing.T) {
	f := newSys(t)
	// A unix socket path must stay under 108 bytes.
	dir, err := os.MkdirTemp("", "rt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	f.env["XDG_RUNTIME_DIR"] = dir
	socket := filepath.Join(dir, "a2a", "codex.sock")
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	f.active[serverUnit] = true
	if code := run([]string{"codex", "resume", "--last"}, f.sys); code != 0 {
		t.Fatalf("exit %d: %s", code, f.errOut.String())
	}
	if !slices.ContainsFunc(f.calls, func(c []string) bool { return slices.Equal(c, []string{"start", adapterUnit}) }) {
		t.Fatalf("systemctl calls = %v", f.calls)
	}
	want := []string{"/bin/codex", "codex", "resume", "--remote", "unix://" + socket, "-C", "/work", "--last"}
	if !slices.Equal(f.execed, want) {
		t.Fatalf("exec = %v", f.execed)
	}
}

func TestCodexArgvPutsRemoteAndTheCwdWhereCodexTakesThem(t *testing.T) {
	// codex --remote starts a new thread in the cwd of the app-server unless -C names the terminal cwd.
	cases := []struct {
		args []string
		cwd  string
		want []string
	}{
		{nil, "/w", []string{"codex", "--remote", "unix://s", "-C", "/w"}},
		{[]string{"fix the tests"}, "/w", []string{"codex", "--remote", "unix://s", "-C", "/w", "fix the tests"}},
		{[]string{"fork", "abc"}, "/w", []string{"codex", "fork", "--remote", "unix://s", "-C", "/w", "abc"}},
		{[]string{"resume", "--last"}, "/w", []string{"codex", "resume", "--remote", "unix://s", "-C", "/w", "--last"}},
		{[]string{"-C", "/x"}, "/w", []string{"codex", "--remote", "unix://s", "-C", "/x"}},
		{[]string{"--cd", "/x", "hi"}, "/w", []string{"codex", "--remote", "unix://s", "--cd", "/x", "hi"}},
		{[]string{"resume", "--cd=/x"}, "/w", []string{"codex", "resume", "--remote", "unix://s", "--cd=/x"}},
		{[]string{"-C/x"}, "/w", []string{"codex", "--remote", "unix://s", "-C/x"}},
		{[]string{"--", "-C"}, "/w", []string{"codex", "--remote", "unix://s", "-C", "/w", "--", "-C"}},
		{nil, "", []string{"codex", "--remote", "unix://s"}},
	}
	for _, c := range cases {
		if got := codexArgv(c.args, "unix://s", c.cwd); !slices.Equal(got, c.want) {
			t.Fatalf("%q in %q: %q", c.args, c.cwd, got)
		}
	}
}

func TestUsageErrors(t *testing.T) {
	f := newSys(t)
	if code := run(nil, f.sys); code != 2 {
		t.Fatalf("no command: %d", code)
	}
	if code := run([]string{"nope"}, f.sys); code != 2 {
		t.Fatalf("unknown command: %d", code)
	}
	if code := run([]string{"mcp", "--harness", "pi"}, f.sys); code != 2 {
		t.Fatalf("bad harness: %d", code)
	}
	if code := run([]string{"version"}, f.sys); code != 0 || strings.TrimSpace(f.out.String()) != version {
		t.Fatalf("version: %d %q", code, f.out.String())
	}
}

func TestTheAdapterNeedsTheToken(t *testing.T) {
	f := newSys(t)
	if code := run([]string{"codex-adapter"}, f.sys); code != 1 || !strings.Contains(f.errOut.String(), "no hub token") {
		t.Fatalf("exit %d: %s", code, f.errOut.String())
	}
}

func TestTheServerPassesTheTokenOnlyInTheEnvironment(t *testing.T) {
	f := newSys(t)
	f.writeToken(t, 0o600)
	dir, err := os.MkdirTemp("", "rt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	f.env["XDG_RUNTIME_DIR"] = dir
	f.env["A2A_HUB_URL"] = "http://hub.test:8080/"
	// A socket that nobody listens on is left from a crash.
	socket := filepath.Join(dir, "a2a", "codex.sock")
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	listener.(*net.UnixListener).SetUnlinkOnClose(false)
	_ = listener.Close()
	var env []string
	f.environ = func() []string { return []string{"HOME=/h", "A2A_HUB_TOKEN=old"} }
	f.execve = func(path string, argv []string, e []string) error {
		f.execed = append([]string{path}, argv...)
		env = e
		return nil
	}
	if code := run([]string{"codex-server"}, f.sys); code != 0 {
		t.Fatalf("exit %d: %s", code, f.errOut.String())
	}
	want := []string{
		"/bin/codex", "codex", "app-server", "--listen", "unix://" + socket,
		"-c", `mcp_servers.a2a.url="http://hub.test:8080/mcp"`,
		"-c", `mcp_servers.a2a.bearer_token_env_var="A2A_HUB_TOKEN"`,
		"-c", `mcp_servers.a2a.http_headers={"X-A2A-Harness"="codex"}`,
	}
	if !slices.Equal(f.execed, want) {
		t.Fatalf("exec = %q", f.execed)
	}
	if !slices.Equal(env, []string{"HOME=/h", "A2A_HUB_TOKEN=secret"}) {
		t.Fatalf("env = %q", env)
	}
	if _, err := os.Lstat(socket); !os.IsNotExist(err) {
		t.Fatalf("stale socket stays: %v", err)
	}
}

func TestTheHeadersHelperIgnoresAnInheritedClaudePid(t *testing.T) {
	f := newSys(t)
	f.writeToken(t, 0o600)
	f.env["CLAUDE_PID"] = "4242"
	f.stdin = strings.NewReader(`{"session_id":"sess-1","source":"startup","cwd":"/proj"}`)
	if code := run([]string{"claude-hook"}, f.sys); code != 0 {
		t.Fatalf("hook exit %d: %s", code, f.errOut.String())
	}
	// A CLAUDE_PID from the parent environment names another Claude process.
	f.env["CLAUDE_PID"] = "999"
	if code := run([]string{"claude-headers"}, f.sys); code != 0 {
		t.Fatalf("headers exit %d: %s", code, f.errOut.String())
	}
	var headers map[string]string
	if err := json.Unmarshal(f.out.Bytes(), &headers); err != nil {
		t.Fatal(err)
	}
	if headers["X-A2A-Agent"] != "sess-1" {
		t.Fatalf("agent = %q", headers["X-A2A-Agent"])
	}
}

func TestTheLauncherDoesNotTakeAStaleSocket(t *testing.T) {
	f := newSys(t)
	dir, err := os.MkdirTemp("", "rt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	f.env["XDG_RUNTIME_DIR"] = dir
	f.socketWait = 200 * time.Millisecond
	socket := filepath.Join(dir, "a2a", "codex.sock")
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	listener.(*net.UnixListener).SetUnlinkOnClose(false)
	_ = listener.Close()
	f.active[serverUnit], f.active[adapterUnit] = true, true
	if code := run([]string{"codex"}, f.sys); code != 1 || f.execed != nil {
		t.Fatalf("exit %d, exec %v: %s", code, f.execed, f.errOut.String())
	}
}

func TestTheShimKeepsTheTokenOutOfStderr(t *testing.T) {
	f := newSys(t)
	f.writeToken(t, 0o600)
	var auth string
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":"unknown token"}`)
	}))
	defer hub.Close()
	f.stdin = strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n")
	if code := run([]string{"mcp", "--hub", hub.URL}, f.sys); code != 0 {
		t.Fatalf("exit %d: %s", code, f.errOut.String())
	}
	if auth != "Bearer secret" {
		t.Fatalf("authorization = %q", auth)
	}
	if !strings.Contains(f.out.String(), "401") {
		t.Fatalf("stdout = %q", f.out.String())
	}
	if strings.Contains(f.errOut.String(), "secret") || strings.Contains(f.out.String(), "secret") {
		t.Fatalf("token in output: %q %q", f.out.String(), f.errOut.String())
	}
}

func TestTheServerCanRunTheHubMCPServerOverStdio(t *testing.T) {
	f := newSys(t)
	f.writeToken(t, 0o600)
	dir, err := os.MkdirTemp("", "rt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	f.env["XDG_RUNTIME_DIR"] = dir
	f.self = "/home/me/.local/bin/a2a"
	var env []string
	f.environ = func() []string { return []string{"HOME=/h", "A2A_HUB_TOKEN=old"} }
	f.execve = func(path string, argv []string, e []string) error {
		f.execed = append([]string{path}, argv...)
		env = e
		return nil
	}
	if code := run([]string{"codex-server", "--mcp-transport", "stdio", "--hub", "http://hub.test"}, f.sys); code != 0 {
		t.Fatalf("exit %d: %s", code, f.errOut.String())
	}
	want := []string{
		"/bin/codex", "codex", "app-server", "--listen", "unix://" + filepath.Join(dir, "a2a", "codex.sock"),
		"-c", `mcp_servers.a2a.command="/home/me/.local/bin/a2a"`,
		"-c", `mcp_servers.a2a.args=["mcp","--harness","codex","--hub","http://hub.test"]`,
	}
	if !slices.Equal(f.execed, want) {
		t.Fatalf("exec = %q", f.execed)
	}
	if !slices.Equal(env, []string{"HOME=/h"}) {
		t.Fatalf("env = %q", env)
	}
	if code := run([]string{"codex-server", "--mcp-transport", "sse"}, f.sys); code != 2 {
		t.Fatalf("bad transport: exit %d", code)
	}
}

func TestTheShimLogsAHeaderFailureOnce(t *testing.T) {
	f := newSys(t)
	f.writeToken(t, 0o644)
	f.stdin = strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n")
	if code := run([]string{"mcp", "--hub", "http://127.0.0.1:1"}, f.sys); code != 0 {
		t.Fatalf("exit %d: %s", code, f.errOut.String())
	}
	if !strings.Contains(f.out.String(), `"error"`) {
		t.Fatalf("stdout = %q", f.out.String())
	}
	if lines := strings.Count(f.errOut.String(), "\n"); lines != 1 {
		t.Fatalf("stderr = %q", f.errOut.String())
	}
}

func TestAGoInstallBuildReportsItsModuleVersion(t *testing.T) {
	build := func(v string) *debug.BuildInfo { return &debug.BuildInfo{Main: debug.Module{Version: v}} }
	cases := []struct {
		set  string
		info *debug.BuildInfo
		ok   bool
		want string
	}{
		{"0.0.0-dev", build("v1.3.1"), true, "1.3.1"},
		{"1.3.1", build("v9.9.9"), true, "1.3.1"},
		{"0.0.0-dev", build("(devel)"), true, "0.0.0-dev"},
		{"0.0.0-dev", build(""), true, "0.0.0-dev"},
		{"0.0.0-dev", nil, false, "0.0.0-dev"},
	}
	for _, c := range cases {
		if got := moduleVersion(c.set, c.info, c.ok); got != c.want {
			t.Errorf("moduleVersion(%q, %v, %v) = %q, want %q", c.set, c.info, c.ok, got, c.want)
		}
	}
}
