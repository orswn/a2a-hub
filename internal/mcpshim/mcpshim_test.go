package mcpshim

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// stub is a scripted hub /mcp endpoint.
type stub struct {
	t        *testing.T
	mu       sync.Mutex
	sessions map[string]bool
	next     int
	requests []stubRequest
	pushes   chan string
	// failNext answers the next n POSTs with this status.
	failStatus int
	failCount  int
	// failBody is the body of a failed answer. Empty writes none.
	failBody string
	// allNotFound answers every POST with 404, as a hub without /mcp does.
	allNotFound bool
	// forgetful answers every tools/call with 404, as a hub that loses each new session does.
	forgetful bool
	// holdCalls makes a tools/call wait for a DELETE, then answer 404. holdDelete makes a DELETE forget the
	// sessions, end the GET streams, and wait up to 300 ms for a later initialize.
	holdCalls  bool
	holdDelete bool
	deleted    chan struct{}
	reopened   chan struct{}
	listener   net.Listener
	server     *http.Server
	addr       string
	changed    chan struct{}
}

type stubRequest struct {
	Method  string
	RPC     map[string]any
	Header  http.Header
	Session string
}

func newStub(t *testing.T) *stub {
	s := &stub{t: t, sessions: map[string]bool{}, pushes: make(chan string, 8), changed: make(chan struct{}, 1),
		deleted: make(chan struct{}), reopened: make(chan struct{}, 1)}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.addr = listener.Addr().String()
	s.serve(listener)
	t.Cleanup(s.stop)
	return s
}

func (s *stub) serve(listener net.Listener) {
	s.listener = listener
	s.server = &http.Server{Handler: http.HandlerFunc(s.handle), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = s.server.Serve(listener) }()
}

func (s *stub) stop() { _ = s.server.Close() }

func (s *stub) restart() {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		s.t.Fatal(err)
	}
	s.serve(listener)
}

func (s *stub) url() string { return "http://" + s.addr + "/mcp" }

func (s *stub) forget() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions = map[string]bool{}
}

func (s *stub) record(r stubRequest) {
	s.mu.Lock()
	s.requests = append(s.requests, r)
	s.mu.Unlock()
	select {
	case s.changed <- struct{}{}:
	default:
	}
}

func (s *stub) handle(w http.ResponseWriter, r *http.Request) {
	session := r.Header.Get("Mcp-Session-Id")
	switch r.Method {
	case http.MethodGet:
		s.record(stubRequest{Method: "GET", Header: r.Header.Clone(), Session: session})
		s.mu.Lock()
		known := s.sessions[session]
		s.mu.Unlock()
		if !known {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, ": hb\n\n")
		w.(http.Flusher).Flush()
		for {
			select {
			case push := <-s.pushes:
				_, _ = fmt.Fprintf(w, "id: 1\nevent: message\ndata: %s\n\n", push)
				w.(http.Flusher).Flush()
			case <-s.deleted:
				return
			case <-r.Context().Done():
				return
			}
		}
	case http.MethodDelete:
		s.record(stubRequest{Method: "DELETE", Header: r.Header.Clone(), Session: session})
		s.mu.Lock()
		hold := s.holdDelete
		s.mu.Unlock()
		if hold {
			s.forget()
			close(s.deleted)
			select {
			case <-s.reopened:
			case <-time.After(300 * time.Millisecond):
			}
		}
		return
	}
	var rpc map[string]any
	_ = json.NewDecoder(r.Body).Decode(&rpc)
	// The record and the scripted answer share one lock, so a test that saw the request cannot change its answer.
	s.mu.Lock()
	s.requests = append(s.requests, stubRequest{Method: "POST", RPC: rpc, Header: r.Header.Clone(), Session: session})
	select {
	case s.changed <- struct{}{}:
	default:
	}
	if s.allNotFound {
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":"not found"}`)
		return
	}
	if s.failCount > 0 {
		s.failCount--
		status, body := s.failStatus, s.failBody
		s.mu.Unlock()
		if body != "" {
			w.Header().Set("Content-Type", "application/json")
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
		return
	}
	method, _ := rpc["method"].(string)
	if method == "tools/call" && s.holdCalls {
		s.mu.Unlock()
		<-s.deleted
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if method == "initialize" && s.holdDelete {
		select {
		case s.reopened <- struct{}{}:
		default:
		}
	}
	if method == "tools/call" && s.forgetful {
		s.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if method != "initialize" && method != "server/discover" && !s.sessions[session] {
		s.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		return
	}
	switch method {
	case "initialize":
		s.next++
		id := fmt.Sprintf("session-%d", s.next)
		s.sessions[id] = true
		s.mu.Unlock()
		w.Header().Set("Mcp-Session-Id", id)
		writeJSON(w, map[string]any{"jsonrpc": "2.0", "id": rpc["id"], "result": map[string]any{"protocolVersion": "2025-11-25"}})
	case "server/discover":
		s.mu.Unlock()
		writeJSON(w, map[string]any{"jsonrpc": "2.0", "id": rpc["id"], "error": map[string]any{"code": -32601, "message": "Method not found"}})
	case "tools/list":
		s.mu.Unlock()
		writeJSON(w, map[string]any{"jsonrpc": "2.0", "id": rpc["id"], "result": map[string]any{"tools": []any{}}})
	case "tools/call":
		s.mu.Unlock()
		params, _ := rpc["params"].(map[string]any)
		if params["name"] == "streamed" {
			w.Header().Set("Content-Type", "text/event-stream")
			raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": rpc["id"], "result": map[string]any{"streamed": true}})
			_, _ = fmt.Fprintf(w, ": hb\n\nevent: message\ndata: %s\n\n", raw)
			return
		}
		writeJSON(w, map[string]any{"jsonrpc": "2.0", "id": rpc["id"], "result": map[string]any{"meta": params["_meta"]}})
	default:
		s.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}
}

func writeJSON(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func (s *stub) waitFor(t *testing.T, what string, match func([]stubRequest) bool) []stubRequest {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		s.mu.Lock()
		copied := append([]stubRequest(nil), s.requests...)
		s.mu.Unlock()
		if match(copied) {
			return copied
		}
		select {
		case <-s.changed:
		case <-deadline:
			t.Fatalf("timed out waiting for %s; requests: %+v", what, copied)
		}
	}
}

type harness struct {
	// failHeaders makes the header source fail, as an unreadable token file does.
	failHeaders atomic.Bool
	stub        *stub
	stdin       *io.PipeWriter
	lines       chan map[string]any
	done        chan error
	cancel      context.CancelFunc
}

func runShim(t *testing.T, s *stub) *harness {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	h := &harness{stub: s, stdin: inW, lines: make(chan map[string]any, 16), done: make(chan error, 1)}
	shim := &Shim{
		URL: s.url(),
		Headers: func() (http.Header, error) {
			if h.failHeaders.Load() {
				return nil, errors.New("token file has mode 0644")
			}
			return http.Header{"Authorization": {"Bearer tok"}, "X-A2A-Harness": {"codex"}}, nil
		},
		RetryDelay: func(int) time.Duration { return 5 * time.Millisecond },
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() {
		h.done <- shim.Run(ctx, inR, outW)
		_ = outW.Close()
	}()
	go func() {
		scanner := bufio.NewScanner(outR)
		for scanner.Scan() {
			var msg map[string]any
			if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
				t.Errorf("bad stdout line %q", scanner.Text())
				continue
			}
			h.lines <- msg
		}
		close(h.lines)
	}()
	t.Cleanup(func() {
		cancel()
		_ = inW.Close()
	})
	return h
}

func (h *harness) send(t *testing.T, msg string) {
	t.Helper()
	if _, err := io.WriteString(h.stdin, msg+"\n"); err != nil {
		t.Fatal(err)
	}
}

func (h *harness) read(t *testing.T) map[string]any {
	t.Helper()
	select {
	case msg := <-h.lines:
		return msg
	case <-time.After(10 * time.Second):
		t.Fatal("no stdout line")
	}
	return nil
}

func (h *harness) handshake(t *testing.T) {
	t.Helper()
	h.send(t, `{"jsonrpc":"2.0","id":0,"method":"server/discover","params":{}}`)
	if got := h.read(t); got["error"].(map[string]any)["code"] != float64(-32601) {
		t.Fatalf("discover answer = %v", got)
	}
	h.send(t, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25"}}`)
	if got := h.read(t); got["id"] != float64(1) {
		t.Fatalf("initialize answer = %v", got)
	}
	h.send(t, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	h.stub.waitFor(t, "notifications/initialized", func([]stubRequest) bool { return h.stub.posts("notifications/initialized") > 0 })
}

func TestToolCallsKeepMetaAndCarryTheSessionAndIdentity(t *testing.T) {
	s := newStub(t)
	h := runShim(t, s)
	h.handshake(t)
	h.send(t, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"send","arguments":{"target":"x"},"_meta":{"threadId":"thread-1","progressToken":1}}}`)
	got := h.read(t)
	meta := got["result"].(map[string]any)["meta"].(map[string]any)
	if got["id"] != float64(2) || meta["threadId"] != "thread-1" {
		t.Fatalf("tools/call answer = %v", got)
	}
	requests := s.waitFor(t, "tools/call", func(r []stubRequest) bool { return len(r) >= 4 })
	for _, r := range requests {
		if r.Method != "POST" {
			continue
		}
		if r.Header.Get("Authorization") != "Bearer tok" || r.Header.Get("X-A2A-Harness") != "codex" {
			t.Fatalf("identity headers missing on %v", r.RPC["method"])
		}
		if r.RPC["method"] == "tools/call" {
			if r.Session != "session-1" || r.Header.Get("Mcp-Protocol-Version") != "2025-11-25" {
				t.Fatalf("session headers = %q %q", r.Session, r.Header.Get("Mcp-Protocol-Version"))
			}
			if !strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
				t.Fatalf("accept = %q", r.Header.Get("Accept"))
			}
		}
	}
}

func TestAStreamedAnswerAndAGetPushReachStdout(t *testing.T) {
	s := newStub(t)
	h := runShim(t, s)
	h.handshake(t)
	s.waitFor(t, "GET stream", func(r []stubRequest) bool {
		for _, x := range r {
			if x.Method == "GET" && x.Session == "session-1" {
				return true
			}
		}
		return false
	})
	s.pushes <- `{"jsonrpc":"2.0","method":"notifications/claude/channel","params":{"content":"hi","meta":{"message_id":"m1"}}}`
	if got := h.read(t); got["method"] != "notifications/claude/channel" {
		t.Fatalf("push = %v", got)
	}
	h.send(t, `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"streamed"}}`)
	if got := h.read(t); got["result"].(map[string]any)["streamed"] != true {
		t.Fatalf("streamed answer = %v", got)
	}
}

func TestAForgottenSessionIsOpenedAgainWithoutTheHarnessSeeingIt(t *testing.T) {
	s := newStub(t)
	h := runShim(t, s)
	h.handshake(t)
	s.forget()
	h.send(t, `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"inbox","_meta":{"threadId":"t"}}}`)
	got := h.read(t)
	if got["id"] != float64(4) || got["result"] == nil {
		t.Fatalf("answer after reopen = %v", got)
	}
	requests := s.waitFor(t, "reopen", func(r []stubRequest) bool {
		for _, x := range r {
			if x.RPC["id"] == "a2a-mcp-reopen-1" {
				return true
			}
		}
		return false
	})
	initialized := 0
	for _, r := range requests {
		if r.RPC["method"] == "notifications/initialized" {
			initialized++
		}
	}
	if initialized < 2 {
		t.Fatalf("notifications/initialized sent %d times", initialized)
	}
	// The GET stream also comes back on the new session.
	s.waitFor(t, "GET on session-2", func(r []stubRequest) bool {
		for _, x := range r {
			if x.Method == "GET" && x.Session == "session-2" {
				return true
			}
		}
		return false
	})
	select {
	case extra := <-h.lines:
		t.Fatalf("the reopen answer reached stdout: %v", extra)
	default:
	}
}

func TestACallWaitsWhileTheHubIsDownAndThenGoesThrough(t *testing.T) {
	s := newStub(t)
	h := runShim(t, s)
	h.handshake(t)
	s.stop()
	h.send(t, `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"peers"}}`)
	s.restart()
	got := h.read(t)
	if got["id"] != float64(5) || got["result"] == nil {
		t.Fatalf("answer after outage = %v", got)
	}
}

func (s *stub) posts(method string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.requests {
		if r.Method == "POST" && r.RPC["method"] == method {
			n++
		}
	}
	return n
}

func (s *stub) fail(status, count int, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failStatus, s.failCount, s.failBody = status, count, body
}

func TestARefusedNewSessionAnswersTheRequestWithAnError(t *testing.T) {
	s := newStub(t)
	h := runShim(t, s)
	h.handshake(t)
	s.mu.Lock()
	s.allNotFound = true
	s.mu.Unlock()
	h.send(t, `{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"peers"}}`)
	got := h.read(t)
	if got["id"] != float64(9) || got["error"] == nil {
		t.Fatalf("answer = %v", got)
	}
	calls, inits := s.posts("tools/call"), s.posts("initialize")
	if calls != 1 || inits != 2 {
		t.Fatalf("tools/call POSTs %d, initialize POSTs %d", calls, inits)
	}
}

func TestAToolCallIsNotSentAgainAfterABadGateway(t *testing.T) {
	for _, status := range []int{http.StatusBadGateway, http.StatusGatewayTimeout} {
		s := newStub(t)
		h := runShim(t, s)
		h.handshake(t)
		s.fail(status, 1, "")
		h.send(t, `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"send"}}`)
		got := h.read(t)
		if got["id"] != float64(7) || got["error"] == nil {
			t.Fatalf("%d: answer = %v", status, got)
		}
		if n := s.posts("tools/call"); n != 1 {
			t.Fatalf("%d: tools/call POSTs = %d", status, n)
		}
	}
}

func TestOtherMethodsAreRetriedAfterABadGateway(t *testing.T) {
	s := newStub(t)
	h := runShim(t, s)
	h.handshake(t)
	s.fail(http.StatusBadGateway, 2, "")
	h.send(t, `{"jsonrpc":"2.0","id":8,"method":"tools/list"}`)
	if got := h.read(t); got["id"] != float64(8) {
		t.Fatalf("answer = %v", got)
	}
	if n := s.posts("tools/list"); n != 3 {
		t.Fatalf("tools/list POSTs = %d", n)
	}
}

func TestAnErrorBodyThatIsNotJSONRPCBecomesAnErrorAnswer(t *testing.T) {
	s := newStub(t)
	h := runShim(t, s)
	h.handshake(t)
	s.fail(http.StatusUnauthorized, 1, `{"error":"bad token"}`)
	h.send(t, `{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"peers"}}`)
	got := h.read(t)
	if got["id"] != float64(10) || got["jsonrpc"] != "2.0" || got["error"] == nil {
		t.Fatalf("answer = %v", got)
	}
	if msg := got["error"].(map[string]any)["message"].(string); !strings.Contains(msg, "401") {
		t.Fatalf("message = %q", msg)
	}
}

func TestAServiceUnavailableToolCallIsRetried(t *testing.T) {
	s := newStub(t)
	h := runShim(t, s)
	h.handshake(t)
	s.fail(http.StatusServiceUnavailable, 2, "")
	h.send(t, `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"peers"}}`)
	if got := h.read(t); got["id"] != float64(6) || got["result"] == nil {
		t.Fatalf("answer after 502 = %v", got)
	}
}

func TestStdinEndClosesTheSession(t *testing.T) {
	s := newStub(t)
	h := runShim(t, s)
	h.handshake(t)
	_ = h.stdin.Close()
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shim did not end")
	}
	s.waitFor(t, "DELETE", func(r []stubRequest) bool {
		for _, x := range r {
			if x.Method == "DELETE" && x.Session == "session-1" {
				return true
			}
		}
		return false
	})
}

func TestDefaultRetryDelayDoublesUpTo30Seconds(t *testing.T) {
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
	for i, w := range want {
		if got := DefaultRetryDelay(i + 1); got != w {
			t.Fatalf("DefaultRetryDelay(%d) = %v, want %v", i+1, got, w)
		}
	}
}

func TestAHubThatForgetsEachNewSessionGetsAnErrorAnswer(t *testing.T) {
	s := newStub(t)
	h := runShim(t, s)
	h.handshake(t)
	s.mu.Lock()
	s.forgetful = true
	s.mu.Unlock()
	h.send(t, `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"peers"}}`)
	got := h.read(t)
	if got["id"] != float64(2) || got["error"] == nil {
		t.Fatalf("answer = %v", got)
	}
	select {
	case extra := <-h.lines:
		t.Fatalf("second answer %v", extra)
	case <-time.After(100 * time.Millisecond):
	}
	calls, inits := s.posts("tools/call"), s.posts("initialize")
	if calls != 3 || inits != 3 {
		t.Fatalf("tools/call POSTs %d, initialize POSTs %d", calls, inits)
	}
}

func TestNoNewSessionAfterTheCloseAtStdinEnd(t *testing.T) {
	s := newStub(t)
	h := runShim(t, s)
	h.handshake(t)
	s.mu.Lock()
	s.holdCalls, s.holdDelete = true, true
	s.mu.Unlock()
	h.send(t, `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"peers"}}`)
	s.waitFor(t, "tools/call", func([]stubRequest) bool { return s.posts("tools/call") > 0 })
	_ = h.stdin.Close()
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shim did not end")
	}
	if n := s.posts("initialize"); n != 1 {
		t.Fatalf("initialize POSTs = %d", n)
	}
}

func TestAHeaderFailureAnswersAtOnce(t *testing.T) {
	s := newStub(t)
	h := runShim(t, s)
	h.failHeaders.Store(true)
	h.send(t, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25"}}`)
	got := h.read(t)
	if got["id"] != float64(1) || got["error"] == nil {
		t.Fatalf("answer = %v", got)
	}
	if n := s.posts("initialize"); n != 0 {
		t.Fatalf("initialize POSTs = %d", n)
	}

	// A refresh of the headers after a retry fails too.
	s = newStub(t)
	h = runShim(t, s)
	h.handshake(t)
	s.fail(http.StatusServiceUnavailable, 1, "")
	h.failHeaders.Store(true)
	h.send(t, `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"peers"}}`)
	got = h.read(t)
	if got["id"] != float64(4) || got["error"] == nil {
		t.Fatalf("answer = %v", got)
	}
	if n := s.posts("tools/call"); n != 1 {
		t.Fatalf("tools/call POSTs = %d", n)
	}
}

func TestAHeaderFailureDuringANewSessionAnswersAtOnce(t *testing.T) {
	s := newStub(t)
	h := runShim(t, s)
	h.handshake(t)
	s.forget()
	h.failHeaders.Store(true)
	h.send(t, `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"peers"}}`)
	got := h.read(t)
	if got["id"] != float64(5) || got["error"] == nil {
		t.Fatalf("answer = %v", got)
	}
	if n := s.posts("initialize"); n != 1 {
		t.Fatalf("initialize POSTs = %d", n)
	}
}

func TestTheGETStreamOpensNoSessionAfterTheClose(t *testing.T) {
	s := newStub(t)
	h := runShim(t, s)
	h.handshake(t)
	s.waitFor(t, "GET stream", func(r []stubRequest) bool {
		return slices.ContainsFunc(r, func(x stubRequest) bool { return x.Method == "GET" })
	})
	s.mu.Lock()
	s.holdDelete = true
	s.mu.Unlock()
	_ = h.stdin.Close()
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shim did not end")
	}
	if n := s.posts("initialize"); n != 1 {
		t.Fatalf("initialize POSTs = %d", n)
	}
}
