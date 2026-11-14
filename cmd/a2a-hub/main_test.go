package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/orswn/a2a-hub/internal/store"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func noEnv(string) (string, bool) { return "", false }

type hubRun struct {
	port    int
	signals chan os.Signal
	stderr  *syncBuffer
	exit    chan int
}

func startHub(t *testing.T, args ...string) *hubRun {
	t.Helper()
	reader, writer := io.Pipe()
	h := &hubRun{signals: make(chan os.Signal, 1), stderr: &syncBuffer{}, exit: make(chan int, 1)}
	go func() {
		h.exit <- run(args, noEnv, writer, h.stderr, h.signals)
		_ = writer.Close()
	}()
	line, err := bufio.NewReader(reader).ReadString('\n')
	if err != nil {
		t.Fatalf("no listening line: %v; stderr: %s", err, h.stderr)
	}
	match := regexp.MustCompile(`^listening (\d+)\n$`).FindStringSubmatch(line)
	if match == nil {
		t.Fatalf("stdout line = %q", line)
	}
	h.port, _ = strconv.Atoi(match[1])
	go func() { _, _ = io.Copy(io.Discard, reader) }()
	t.Cleanup(func() {
		select {
		case <-h.exit:
		default:
			h.signals <- syscall.SIGTERM
			<-h.exit
		}
	})
	return h
}

func (h *hubRun) stop(t *testing.T, sig os.Signal) int {
	t.Helper()
	h.signals <- sig
	select {
	case code := <-h.exit:
		h.exit <- code
		return code
	case <-time.After(5 * time.Second):
		t.Fatal("the hub did not stop")
		return -1
	}
}

func TestServesHealthzAndStopsOnSignals(t *testing.T) {
	for _, sig := range []os.Signal{syscall.SIGTERM, syscall.SIGINT} {
		h := startHub(t, "--listen", "127.0.0.1:0", "--data", store.Memory)
		res, err := http.Get("http://127.0.0.1:" + strconv.Itoa(h.port) + "/healthz")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		_ = res.Body.Close()
		if res.StatusCode != 200 || string(body) != `{"ok":true,"version":"0.0.0-dev","agents":0}` {
			t.Errorf("healthz = %d %s", res.StatusCode, body)
		}
		if code := h.stop(t, sig); code != 0 {
			t.Errorf("exit code after %v = %d", sig, code)
		}
		name := map[os.Signal]string{syscall.SIGTERM: "SIGTERM", syscall.SIGINT: "SIGINT"}[sig]
		if !strings.Contains(h.stderr.String(), " "+name+": stopping\n") {
			t.Errorf("stderr = %q", h.stderr)
		}
		if !strings.Contains(h.stderr.String(), "warning: no token file") {
			t.Errorf("no warning without a token file: %q", h.stderr)
		}
	}
}

func TestHealthcheckSubcommand(t *testing.T) {
	h := startHub(t, "--listen", "127.0.0.1:0", "--data", store.Memory, "--test-agents")
	var stderr bytes.Buffer
	if code := run([]string{"healthcheck", "--listen", "0.0.0.0:" + strconv.Itoa(h.port)}, noEnv, io.Discard, &stderr, nil); code != 0 {
		t.Errorf("healthcheck = %d: %s", code, stderr.String())
	}
	env := func(name string) (string, bool) {
		if name == "HUB_LISTEN" {
			return "0.0.0.0:" + strconv.Itoa(h.port), true
		}
		return "", false
	}
	if code := run([]string{"healthcheck"}, env, io.Discard, &stderr, nil); code != 0 {
		t.Errorf("healthcheck with HUB_LISTEN = %d: %s", code, stderr.String())
	}
	if strings.Contains(h.stderr.String(), "warning") {
		t.Errorf("warning in test mode: %q", h.stderr)
	}
	h.stop(t, syscall.SIGTERM)

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	for _, args := range [][]string{{"healthcheck", "--listen", "127.0.0.1:" + strconv.Itoa(closed)}, {"healthcheck", "--bad"}} {
		if code := run(args, noEnv, io.Discard, io.Discard, nil); code != 1 {
			t.Errorf("%q = %d, want 1", args, code)
		}
	}
}

func TestHealthcheckFailsOnAnotherStatus(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.NotFoundHandler(), ReadHeaderTimeout: time.Second}
	go func() { _ = srv.Serve(listener) }()
	defer func() { _ = srv.Close() }()
	port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	if code := run([]string{"healthcheck", "--listen", "127.0.0.1:" + port}, noEnv, io.Discard, io.Discard, nil); code != 1 {
		t.Errorf("healthcheck against a 404 = %d, want 1", code)
	}
}

func TestReloadsTokensOnHangup(t *testing.T) {
	dir := t.TempDir()
	tokens := filepath.Join(dir, "tokens.json")
	if err := os.WriteFile(tokens, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	h := startHub(t, "--listen", "127.0.0.1:0", "--data", store.Memory, "--tokens", tokens)
	waitFor := func(text string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !strings.Contains(h.stderr.String(), text) {
			if time.Now().After(deadline) {
				t.Fatalf("no %q in stderr: %q", text, h.stderr)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	h.signals <- syscall.SIGHUP
	waitFor("SIGHUP: tokens reloaded\n")
	if err := os.WriteFile(tokens, []byte(`[]`), 0o600); err != nil {
		t.Fatal(err)
	}
	h.signals <- syscall.SIGHUP
	waitFor("SIGHUP: token reload failed, old tokens kept: token file must be a JSON object\n")
	if code := h.stop(t, syscall.SIGTERM); code != 0 {
		t.Errorf("exit code = %d", code)
	}
}

func TestExitCodes(t *testing.T) {
	dir := t.TempDir()
	newer := filepath.Join(dir, "newer")
	if err := os.MkdirAll(newer, 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(newer, store.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), "PRAGMA user_version = 9"); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	badTokens := filepath.Join(dir, "tokens.json")
	if err := os.WriteFile(badTokens, []byte(`{"x": {"kind": "admin"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		args   []string
		code   int
		stderr string
	}{
		{[]string{"--listen", "8080"}, 2, "--listen must be host:port, got 8080"},
		{[]string{"--public-url", "hub.example"}, 2, "--public-url must start with http:// or https://"},
		{[]string{"--nope"}, 1, "unknown option"},
		{[]string{"--listen", "127.0.0.1:0", "--data", newer}, 1, "database schema 9 is newer than this hub (2)"},
		{[]string{"--listen", "127.0.0.1:0", "--data", store.Memory, "--tokens", badTokens}, 1, `token x: kind must be "host" or "peer"`},
		{[]string{"--listen", "127.0.0.1:0", "--data", store.Memory, "--tokens", filepath.Join(dir, "missing.json")}, 1, "no such file"},
	}
	for _, c := range cases {
		var stderr bytes.Buffer
		if code := run(c.args, noEnv, io.Discard, &stderr, nil); code != c.code || !strings.Contains(stderr.String(), c.stderr) {
			t.Errorf("%q = %d %q, want %d %q", c.args, code, stderr.String(), c.code, c.stderr)
		}
	}
}

func TestPortInUseFails(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	var stderr bytes.Buffer
	if code := run([]string{"--listen", "127.0.0.1:" + port, "--data", store.Memory}, noEnv, io.Discard, &stderr, nil); code != 1 {
		t.Errorf("exit code = %d: %s", code, stderr.String())
	}
}
