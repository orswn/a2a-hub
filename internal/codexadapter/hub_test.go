package codexadapter

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// The link side of these tests runs against the hub binary of this repository, built once per run.

const hostToken = "mars-token"

var hubBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "a2a-hub-bin")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	hubBin = filepath.Join(dir, "a2a-hub")
	build := exec.Command("go", "build", "-o", hubBin, "github.com/orswn/a2a-hub/cmd/a2a-hub")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "hub build failed: %v\n%s", err, out)
		_ = os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

type testHub struct {
	t   *testing.T
	url string
}

func startHub(t *testing.T) *testHub {
	t.Helper()
	dir := t.TempDir()
	digest := sha256.Sum256([]byte(hostToken))
	tokens := filepath.Join(dir, "tokens.json")
	raw := fmt.Sprintf(`{"mars":{"kind":"host","sha256":%q}}`, hex.EncodeToString(digest[:]))
	if err := os.WriteFile(tokens, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(hubBin, "--listen", "127.0.0.1:0", "--data", filepath.Join(dir, "data"), "--tokens", tokens)
	cmd.Env = append(os.Environ(), "HUB_PRESENCE_GRACE_MS=300")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	port := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if p, ok := strings.CutPrefix(scanner.Text(), "listening "); ok {
				port <- p
			}
		}
	}()
	select {
	case p := <-port:
		return &testHub{t: t, url: "http://127.0.0.1:" + p}
	case <-time.After(20 * time.Second):
		t.Fatalf("hub did not start: %s", stderr.String())
	}
	return nil
}

func (h *testHub) do(method, path string, headers map[string]string, body any) (int, map[string]any) {
	h.t.Helper()
	var reader io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, h.url+path, reader)
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+hostToken)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	var out map[string]any
	_ = json.NewDecoder(res.Body).Decode(&out)
	return res.StatusCode, out
}

// session registers a pi session that sends messages.
func (h *testHub) session(id string) {
	h.t.Helper()
	status, out := h.do("POST", "/link/register", nil, map[string]any{
		"agent": id, "name": id, "protocol": 3, "labels": map[string]string{"harness": "pi"},
	})
	if status != 200 {
		h.t.Fatalf("register %s: %d %v", id, status, out)
	}
}

func (h *testHub) agent(id string) (int, map[string]any) {
	return h.do("GET", "/agents/"+id, nil, nil)
}

// send sends a message from agent from to agent to and returns the A2A result.
func (h *testHub) send(from, to, text string, bus map[string]any) map[string]any {
	h.t.Helper()
	status, out := h.do("POST", "/a2a/"+to, map[string]string{
		"X-A2A-Agent": from, "A2A-Version": "1.0", "A2A-Extensions": "https://github.com/orswn/a2a-hub/bus/v1",
	}, map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "SendMessage",
		"params": map[string]any{
			"message": map[string]any{
				"messageId": fmt.Sprintf("m-%d", time.Now().UnixNano()), "role": "ROLE_USER",
				"parts":    []any{map[string]any{"text": text}},
				"metadata": map[string]any{"https://github.com/orswn/a2a-hub/bus/v1": bus},
			},
			"configuration": map[string]any{"returnImmediately": true},
		},
	})
	if status != 200 || out["result"] == nil {
		h.t.Fatalf("send: %d %v", status, out)
	}
	return out["result"].(map[string]any)
}

// delivery reads the delivery state of a send result.
func delivery(result map[string]any) string {
	item, _ := result["task"].(map[string]any)
	if item == nil {
		item, _ = result["message"].(map[string]any)
	}
	meta, _ := item["metadata"].(map[string]any)
	bus, _ := meta["https://github.com/orswn/a2a-hub/bus/v1"].(map[string]any)
	state, _ := bus["delivery"].(string)
	return state
}

func taskOf(result map[string]any) (id, messageID string) {
	task, _ := result["task"].(map[string]any)
	if task == nil {
		return "", ""
	}
	id, _ = task["id"].(string)
	history, _ := task["history"].([]any)
	if len(history) > 0 {
		messageID, _ = history[0].(map[string]any)["messageId"].(string)
	}
	return id, messageID
}

func (h *testHub) taskState(agent, taskID string) string {
	h.t.Helper()
	_, out := h.do("POST", "/a2a/"+agent, map[string]string{"A2A-Version": "1.0"}, map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "GetTask", "params": map[string]any{"id": taskID},
	})
	result, _ := out["result"].(map[string]any)
	status, _ := result["status"].(map[string]any)
	state, _ := status["state"].(string)
	return state
}

// waitFor polls cond until it holds or 10 s pass.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (h *testHub) waitAgent(t *testing.T, id string, cond func(map[string]any) bool) map[string]any {
	t.Helper()
	var last map[string]any
	waitFor(t, "agent "+id, func() bool {
		status, out := h.agent(id)
		last = out
		return status == 200 && cond(out)
	})
	return last
}

func online(info map[string]any) bool { return info["online"] == true }

// fakeClock moves only when a test advances it.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	tickers []*fakeTicker
}

type fakeTicker struct {
	clock *fakeClock
	every time.Duration
	next  time.Time
	c     chan time.Time
	dead  bool
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(1_800_000_000, 0)} }

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) NewTicker(d time.Duration) Ticker {
	f.mu.Lock()
	defer f.mu.Unlock()
	ticker := &fakeTicker{clock: f, every: d, next: f.now.Add(d), c: make(chan time.Time, 1)}
	f.tickers = append(f.tickers, ticker)
	return ticker
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	for _, ticker := range f.tickers {
		if !ticker.dead && !ticker.next.After(f.now) {
			select {
			case ticker.c <- f.now:
			default:
			}
			ticker.next = f.now.Add(ticker.every)
		}
	}
}

func (t *fakeTicker) C() <-chan time.Time { return t.c }

func (t *fakeTicker) Stop() {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	t.dead = true
}

// tickers counts the live tickers with period d, so a test knows that a loop waits on one.
func (f *fakeClock) liveTickers(d time.Duration) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, ticker := range f.tickers {
		if !ticker.dead && ticker.every == d {
			n++
		}
	}
	return n
}

// openStream opens a link event stream for agent with the host token, as a second client would.
func (h *testHub) openStream(ctx context.Context, agent string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", h.url+"/link/"+agent+"/events", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+hostToken)
	return http.DefaultClient.Do(req)
}
