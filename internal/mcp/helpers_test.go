package mcp

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orswn/a2a-hub/internal/auth"
	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/external"
	"github.com/orswn/a2a-hub/internal/jsonx"
	"github.com/orswn/a2a-hub/internal/link"
	"github.com/orswn/a2a-hub/internal/store"
)

var (
	mars   = auth.Principal{Name: "mars", Kind: auth.KindHost}
	venus  = auth.Principal{Name: "venus", Kind: auth.KindHost}
	hermes = auth.Principal{Name: "hermes", Kind: auth.KindPeer}
	tokens = map[string]auth.Principal{"mars-token": mars, "venus-token": venus, "hermes-token": hermes}
)

type hooks struct {
	mu    sync.Mutex
	hooks map[string]*external.Webhook
}

func (h *hooks) Webhook(agentID string) *external.Webhook {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hooks[agentID]
}

func (h *hooks) set(agentID string, hook *external.Webhook) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.hooks[agentID] = hook
}

type env struct {
	t     *testing.T
	e     *engine.Engine
	links *link.Hub
	srv   *Server
	url   string
	hooks *hooks
	mu    sync.Mutex
	logs  []string
}

type envOptions struct {
	grace       time.Duration
	heartbeat   time.Duration
	idle        time.Duration
	retryDelays []time.Duration
	now         func() time.Time
}

func setup(t *testing.T, options ...envOptions) *env {
	t.Helper()
	opts := envOptions{grace: time.Minute, heartbeat: time.Minute}
	if len(options) > 0 {
		opts = options[0]
	}
	db, err := store.Open(context.Background(), store.Memory)
	if err != nil {
		t.Fatal(err)
	}
	h := &env{t: t, hooks: &hooks{hooks: map[string]*external.Webhook{}}}
	h.e, err = engine.New(context.Background(), db, engine.Options{Log: h.log})
	if err != nil {
		t.Fatal(err)
	}
	h.links = link.New(h.e, link.Options{PresenceGrace: opts.grace, Heartbeat: opts.heartbeat, Log: h.log})
	h.srv = New(Options{
		Engine: h.e, Link: h.links, Webhooks: h.hooks, Version: "test", PresenceGrace: opts.grace,
		IdleTimeout: opts.idle, RetryDelays: opts.retryDelays, Now: opts.now, Log: h.log,
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		if !ok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		h.srv.Serve(w, r, principal)
	}))
	h.url = server.URL
	t.Cleanup(func() {
		h.links.Stop()
		server.CloseClientConnections()
		server.Close()
		h.srv.Stop()
		_ = h.e.Close()
		_ = db.Close()
	})
	return h
}

func (h *env) log(line string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.logs = append(h.logs, line)
}

func (h *env) logged() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return strings.Join(h.logs, "\n")
}

// register makes a session agent of host mars.
func (h *env) register(id string, extra ...any) {
	h.t.Helper()
	body := jsonx.NewObject("agent", id, "protocol", float64(engine.HubProtocol), "name", id,
		"labels", jsonx.NewObject(engine.HarnessLabel, "pi"))
	for i := 0; i+1 < len(extra); i += 2 {
		body.Set(extra[i].(string), extra[i+1])
	}
	if _, err := h.e.Register(mars, body); err != nil {
		h.t.Fatal(err)
	}
}

func (h *env) online(id string) {
	h.t.Helper()
	if err := h.e.SetOnline(id, true); err != nil {
		h.t.Fatal(err)
	}
}

// sendFrom sends a bus message from a mars session agent.
func (h *env) sendFrom(from, to, messageID string, meta ...any) *engine.SendResult {
	h.t.Helper()
	bus := jsonx.NewObject("kind", "note")
	for i := 0; i+1 < len(meta); i += 2 {
		bus.Set(meta[i].(string), meta[i+1])
	}
	message := jsonx.NewObject("messageId", messageID, "role", "ROLE_USER", "parts", []any{jsonx.NewObject("text", "text of "+messageID)},
		"metadata", jsonx.NewObject(engine.BusExtensionURI, bus))
	if contextID, ok := bus.String("contextId"); ok {
		message.Set("contextId", contextID)
		bus.Delete("contextId")
	}
	result, err := h.e.Send(engine.SendInput{AgentID: to, Caller: engine.Caller{Principal: mars, Agent: from}, Message: message})
	if err != nil {
		h.t.Fatal(err)
	}
	return result
}

type client struct {
	h       *env
	token   string
	headers map[string]string
	session string
	next    int
}

func (h *env) client(token, harness string, headers ...string) *client {
	c := &client{h: h, token: token, headers: map[string]string{}}
	if harness != "" {
		c.headers[HarnessHeader] = harness
	}
	for i := 0; i+1 < len(headers); i += 2 {
		c.headers[headers[i]] = headers[i+1]
	}
	return c
}

type reply struct {
	status int
	header http.Header
	body   *jsonx.Object
}

func (c *client) do(method string, body []byte) reply {
	c.h.t.Helper()
	request, err := http.NewRequest(method, c.h.url, bytes.NewReader(body))
	if err != nil {
		c.h.t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	for key, value := range c.headers {
		request.Header.Set(key, value)
	}
	if c.session != "" {
		request.Header.Set(SessionIDHeader, c.session)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		c.h.t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		c.h.t.Fatal(err)
	}
	out := reply{status: response.StatusCode, header: response.Header}
	if len(data) > 0 {
		parsed, err := jsonx.Parse(data)
		if err != nil {
			c.h.t.Fatalf("%s: %v: %s", method, err, data)
		}
		out.body, _ = parsed.(*jsonx.Object)
	}
	return out
}

func (c *client) rpc(method string, params *jsonx.Object) reply {
	c.h.t.Helper()
	c.next++
	message := jsonx.NewObject("jsonrpc", "2.0", "id", float64(c.next), "method", method)
	if params != nil {
		message.Set("params", params)
	}
	return c.do(http.MethodPost, jsonx.Stringify(message))
}

func (c *client) initialize() *jsonx.Object {
	c.h.t.Helper()
	got := c.rpc("initialize", jsonx.NewObject("protocolVersion", ProtocolVersion, "capabilities", jsonx.NewObject(),
		"clientInfo", jsonx.NewObject("name", "test", "version", "1")))
	if got.status != http.StatusOK {
		c.h.t.Fatalf("initialize: %d %s", got.status, jsonx.Stringify(got.body))
	}
	c.session = got.header.Get(SessionIDHeader)
	if c.session == "" {
		c.h.t.Fatal("initialize gave no session id")
	}
	result, _ := got.body.Object("result")
	return result
}

// call runs a tool. meta pairs go into _meta.
func (c *client) call(tool string, args *jsonx.Object, meta ...any) (string, bool) {
	c.h.t.Helper()
	params := jsonx.NewObject("name", tool, "arguments", args)
	if len(meta) > 0 {
		m := jsonx.NewObject()
		for i := 0; i+1 < len(meta); i += 2 {
			m.Set(meta[i].(string), meta[i+1])
		}
		params.Set("_meta", m)
	}
	got := c.rpc("tools/call", params)
	if got.status != http.StatusOK {
		c.h.t.Fatalf("tools/call %s: %d %s", tool, got.status, jsonx.Stringify(got.body))
	}
	result, ok := got.body.Object("result")
	if !ok {
		c.h.t.Fatalf("tools/call %s: %s", tool, jsonx.Stringify(got.body))
	}
	content, _ := result.Value("content").([]any)
	first, _ := content[0].(*jsonx.Object)
	text, _ := first.String("text")
	return text, result.Value("isError") == true
}

// ok runs a tool and fails the test on a tool error.
func (c *client) ok(tool string, args *jsonx.Object, meta ...any) string {
	c.h.t.Helper()
	text, isError := c.call(tool, args, meta...)
	if isError {
		c.h.t.Fatalf("%s: %s", tool, text)
	}
	return text
}

type stream struct {
	t      *testing.T
	status int
	cancel func()
	mu     sync.Mutex
	frames []string
	closed bool
	wake   chan struct{}
	done   chan struct{}
}

// open starts a GET stream. A stream that the server refuses has status other than 200 and no frames.
func (c *client) open() *stream {
	c.h.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.h.url, nil)
	if err != nil {
		c.h.t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set(SessionIDHeader, c.session)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		cancel()
		c.h.t.Fatal(err)
	}
	s := &stream{t: c.h.t, status: response.StatusCode, cancel: cancel, wake: make(chan struct{}, 1), done: make(chan struct{})}
	go func() {
		defer close(s.done)
		defer func() { _ = response.Body.Close() }()
		reader := bufio.NewReader(response.Body)
		var frame strings.Builder
		for {
			text, err := reader.ReadString('\n')
			if err != nil {
				s.mu.Lock()
				s.closed = true
				s.mu.Unlock()
				s.signal()
				return
			}
			if text == "\n" {
				s.mu.Lock()
				s.frames = append(s.frames, frame.String())
				s.mu.Unlock()
				frame.Reset()
				s.signal()
				continue
			}
			frame.WriteString(text)
		}
	}()
	c.h.t.Cleanup(s.close)
	return s
}

func (s *stream) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *stream) close() {
	s.cancel()
	<-s.done
}

func (s *stream) snapshot() ([]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.frames...), s.closed
}

// waitFor waits until a frame contains text, or the stream ends.
func (s *stream) waitFor(text string) string {
	s.t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		frames, closed := s.snapshot()
		for _, frame := range frames {
			if strings.Contains(frame, text) {
				return frame
			}
		}
		if closed {
			s.t.Fatalf("stream ended without %q: %q", text, frames)
		}
		select {
		case <-s.wake:
		case <-deadline:
			s.t.Fatalf("no frame with %q: %q", text, frames)
		}
	}
}

// waitClosed waits until the server ends the stream.
func (s *stream) waitClosed() {
	s.t.Helper()
	select {
	case <-s.done:
	case <-time.After(5 * time.Second):
		s.t.Fatal("stream still open")
	}
}

func (s *stream) has(text string) bool {
	frames, _ := s.snapshot()
	for _, frame := range frames {
		if strings.Contains(frame, text) {
			return true
		}
	}
	return false
}

func args(pairs ...any) *jsonx.Object {
	out := jsonx.NewObject()
	for i := 0; i+1 < len(pairs); i += 2 {
		value := pairs[i+1]
		if n, ok := value.(int); ok {
			value = float64(n)
		}
		out.Set(pairs[i].(string), value)
	}
	return out
}

// eventually polls cond until it holds or 5 s pass.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func mustContain(t *testing.T, label, got string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(got, want) {
			t.Errorf("%s: %q does not contain %q", label, got, want)
		}
	}
}
