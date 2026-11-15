// Package mcpshim is a stdio MCP server that forwards every message to the hub /mcp endpoint over
// streamable HTTP. It hides hub restarts from the harness: it retries without limit and opens a new MCP
// session when the hub forgot the old one.
package mcpshim

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Shim forwards one stdio MCP connection.
type Shim struct {
	// URL is the hub /mcp endpoint.
	URL string
	// Headers gives the identity headers. It runs before the first request and again after a failure, like
	// a Claude headersHelper.
	Headers func() (http.Header, error)
	HTTP    *http.Client
	// RetryDelay gives the wait before retry n, from 1.
	RetryDelay func(n int) time.Duration
	Log        func(string)

	out      io.Writer
	outMu    sync.Mutex
	mu       sync.Mutex
	session  string
	version  string
	init     json.RawMessage
	headers  http.Header
	getOnce  sync.Once
	reopenMu sync.Mutex
	reinits  int
	// closing is set before the DELETE at stdin end, so a request that fails meanwhile opens no new session.
	closing bool
}

// DefaultRetryDelay is 1 s doubled per try, capped at 30 s.
func DefaultRetryDelay(n int) time.Duration {
	delay := time.Second
	for i := 1; i < n && delay < 30*time.Second; i++ {
		delay *= 2
	}
	return min(delay, 30*time.Second)
}

type rpcHead struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
}

// Run forwards until in ends or ctx ends.
func (s *Shim) Run(ctx context.Context, in io.Reader, out io.Writer) error {
	s.out = out
	if s.HTTP == nil {
		s.HTTP = &http.Client{}
	}
	if s.RetryDelay == nil {
		s.RetryDelay = DefaultRetryDelay
	}
	if s.Log == nil {
		s.Log = func(string) {}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	reader := bufio.NewReaderSize(in, 1<<16)
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		line, err := reader.ReadBytes('\n')
		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			msg := json.RawMessage(append([]byte(nil), line...))
			var head rpcHead
			_ = json.Unmarshal(msg, &head)
			if head.Method == "initialize" || s.currentSession() == "" {
				s.forward(ctx, msg, head)
			} else {
				wg.Add(1)
				go func() {
					defer wg.Done()
					s.forward(ctx, msg, head)
				}()
			}
		}
		if err != nil {
			// The DELETE goes before the cancel: a cancel during a connect of the GET stream also failed the DELETE.
			s.closeSession()
			cancel()
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func (s *Shim) currentSession() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.session
}

func (s *Shim) write(msg []byte) {
	s.outMu.Lock()
	defer s.outMu.Unlock()
	_, _ = s.out.Write(append(bytes.TrimSpace(msg), '\n'))
}

// errorReply answers a request that the hub could not take.
func (s *Shim) errorReply(head rpcHead, text string) {
	if len(head.ID) == 0 {
		return
	}
	raw, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      head.ID,
		"error":   map[string]any{"code": -32603, "message": text},
	})
	s.write(raw)
}

func (s *Shim) identity(refresh bool) (http.Header, error) {
	s.mu.Lock()
	cached := s.headers
	s.mu.Unlock()
	if cached != nil && !refresh {
		return cached, nil
	}
	headers, err := s.Headers()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.headers = headers
	s.mu.Unlock()
	return headers, nil
}

// notDelivered tells whether a request error means that the hub never got the request, so a retry cannot
// run it twice.
func notDelivered(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

func retryStatus(status int) bool {
	return status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
}

// retryStatusFor narrows retryStatus for tools/call: behind a proxy, a 502 or 504 can come after the hub
// already ran the call, and a second send would deliver a message twice.
func retryStatusFor(method string, status int) bool {
	if method == "tools/call" {
		return status == http.StatusServiceUnavailable
	}
	return retryStatus(status)
}

// maxReopens bounds the new sessions that one request may cause.
const maxReopens = 2

// forward posts one message and writes the answers. It waits through hub outages.
func (s *Shim) forward(ctx context.Context, msg json.RawMessage, head rpcHead) {
	if head.Method == "initialize" {
		s.mu.Lock()
		s.init = msg
		s.mu.Unlock()
	}
	failures, reopens := 0, 0
	for {
		s.mu.Lock()
		session := s.session
		s.mu.Unlock()
		res, err := s.post(ctx, msg, head.Method != "initialize", failures > 0)
		if ctx.Err() != nil {
			if res != nil {
				_ = res.Body.Close()
			}
			return
		}
		retry := false
		switch {
		case err != nil && notDelivered(err):
			retry = true
		case err != nil:
			s.Log("hub request failed: " + err.Error())
			s.errorReply(head, "hub request failed: "+err.Error())
			return
		case retryStatusFor(head.Method, res.StatusCode):
			retry = true
		case res.StatusCode == http.StatusNotFound && session != "" && head.Method != "initialize":
			_ = res.Body.Close()
			reopens++
			if reopens > maxReopens {
				s.errorReply(head, "hub forgot the MCP session again")
				return
			}
			if err := s.reopen(ctx, session); errors.Is(err, errClosing) {
				s.errorReply(head, "MCP session closed")
				return
			} else if err != nil {
				s.Log("new MCP session failed: " + err.Error())
				s.errorReply(head, "new MCP session failed: "+err.Error())
				return
			}
			continue
		}
		if retry {
			if res != nil {
				_ = res.Body.Close()
			}
			failures++
			if !s.sleep(ctx, failures) {
				return
			}
			continue
		}
		s.answer(res, head)
		if head.Method == "notifications/initialized" {
			s.getOnce.Do(func() { go s.listen(ctx) })
		}
		return
	}
}

func (s *Shim) sleep(ctx context.Context, n int) bool {
	timer := time.NewTimer(s.RetryDelay(n))
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *Shim) post(ctx context.Context, msg json.RawMessage, withSession, refresh bool) (*http.Response, error) {
	headers, err := s.identity(refresh)
	if err != nil {
		// Not a dial error: a retry cannot fix a token file, and the tool call would hang.
		return nil, fmt.Errorf("hub headers: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL, bytes.NewReader(msg))
	if err != nil {
		return nil, err
	}
	for key, values := range headers {
		req.Header[key] = values
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	s.mu.Lock()
	if withSession && s.session != "" {
		req.Header.Set("Mcp-Session-Id", s.session)
	}
	if withSession && s.version != "" {
		req.Header.Set("Mcp-Protocol-Version", s.version)
	}
	s.mu.Unlock()
	return s.HTTP.Do(req)
}

// answer writes the hub answer of one request and keeps the session of an initialize answer.
func (s *Shim) answer(res *http.Response, head rpcHead) {
	defer func() { _ = res.Body.Close() }()
	if head.Method == "initialize" && res.StatusCode == http.StatusOK {
		if session := res.Header.Get("Mcp-Session-Id"); session != "" {
			s.mu.Lock()
			s.session = session
			s.mu.Unlock()
		}
	}
	mediaType, _, _ := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if res.StatusCode == http.StatusAccepted {
		return
	}
	if mediaType == "text/event-stream" {
		_ = readSSE(res.Body, func(_ string, data []byte) { s.keepVersion(head, data); s.write(data) })
		return
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, 64<<20))
	if err != nil {
		s.errorReply(head, "hub answer failed: "+err.Error())
		return
	}
	if answers(head, body) {
		s.keepVersion(head, body)
		s.write(body)
		return
	}
	s.errorReply(head, fmt.Sprintf("hub answered %d", res.StatusCode))
}

// answers tells whether body is the JSON-RPC answer of the request. Other bodies, such as the plain error
// of a 401, must not reach the client.
func answers(head rpcHead, body []byte) bool {
	var msg struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
	}
	if len(head.ID) == 0 || json.Unmarshal(body, &msg) != nil || msg.JSONRPC != "2.0" {
		return false
	}
	var want, got bytes.Buffer
	return json.Compact(&want, head.ID) == nil && json.Compact(&got, msg.ID) == nil && bytes.Equal(want.Bytes(), got.Bytes())
}

func (s *Shim) keepVersion(head rpcHead, body []byte) {
	if head.Method != "initialize" {
		return
	}
	var answer struct {
		Result struct {
			ProtocolVersion string `json:"protocolVersion"`
		} `json:"result"`
	}
	if json.Unmarshal(body, &answer) == nil && answer.Result.ProtocolVersion != "" {
		s.mu.Lock()
		s.version = answer.Result.ProtocolVersion
		s.mu.Unlock()
	}
}

// errClosing is the reopen error after the close at stdin end.
var errClosing = errors.New("MCP session closed")

// reopen opens a new MCP session with the stored initialize request after the hub forgot session old. The
// answer goes nowhere: the harness already has one. Requests that fail with old meanwhile wait here and then
// use the new session.
func (s *Shim) reopen(ctx context.Context, old string) error {
	s.reopenMu.Lock()
	defer s.reopenMu.Unlock()
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return errClosing
	}
	if s.session != old {
		s.mu.Unlock()
		return nil
	}
	init := s.init
	s.reinits++
	n := s.reinits
	s.headers = nil
	s.mu.Unlock()
	if init == nil {
		return errors.New("no initialize request to repeat")
	}
	var request map[string]any
	if err := json.Unmarshal(init, &request); err != nil {
		return err
	}
	request["id"] = "a2a-mcp-reopen-" + strconv.Itoa(n)
	raw, _ := json.Marshal(request)
	failures := 0
	var session string
	for {
		res, err := s.post(ctx, raw, false, true)
		if err == nil {
			session = res.Header.Get("Mcp-Session-Id")
			_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<20))
			_ = res.Body.Close()
			if res.StatusCode == http.StatusOK && session != "" {
				break
			}
			if !retryStatus(res.StatusCode) {
				return fmt.Errorf("hub answered %d to initialize", res.StatusCode)
			}
		} else if !notDelivered(err) {
			return err
		}
		failures++
		if !s.sleep(ctx, failures) {
			return ctx.Err()
		}
	}
	s.mu.Lock()
	s.session = session
	s.mu.Unlock()
	initialized, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"})
	if res, err := s.post(ctx, initialized, true, false); err == nil {
		_ = res.Body.Close()
	}
	s.Log("hub forgot the MCP session; opened a new one")
	return nil
}

// listen keeps the GET stream open for server pushes, such as Claude channel notifications.
func (s *Shim) listen(ctx context.Context) {
	failures := 0
	lastID := ""
	for ctx.Err() == nil {
		session := s.currentSession()
		status, got, err := s.stream(ctx, session, &lastID)
		if ctx.Err() != nil {
			return
		}
		switch {
		case status == http.StatusMethodNotAllowed:
			s.Log("hub has no MCP GET stream")
			return
		case status == http.StatusNotFound && session != "":
			if err := s.reopen(ctx, session); errors.Is(err, errClosing) {
				return
			} else if err != nil {
				s.Log("new MCP session failed: " + err.Error())
			}
			lastID = ""
		case err != nil:
			s.Log("MCP stream ended: " + err.Error())
		}
		if got {
			failures = 0
		}
		failures++
		if !s.sleep(ctx, failures) {
			return
		}
	}
}

// stream runs one GET stream. got tells whether it delivered anything, so the retry delay starts again.
func (s *Shim) stream(ctx context.Context, session string, lastID *string) (int, bool, error) {
	headers, err := s.identity(false)
	if err != nil {
		return 0, false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL, nil)
	if err != nil {
		return 0, false, err
	}
	for key, values := range headers {
		req.Header[key] = values
	}
	req.Header.Set("Accept", "text/event-stream")
	if session != "" {
		req.Header.Set("Mcp-Session-Id", session)
	}
	s.mu.Lock()
	if s.version != "" {
		req.Header.Set("Mcp-Protocol-Version", s.version)
	}
	s.mu.Unlock()
	if *lastID != "" {
		req.Header.Set("Last-Event-ID", *lastID)
	}
	res, err := s.HTTP.Do(req)
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return res.StatusCode, false, fmt.Errorf("hub answered %d", res.StatusCode)
	}
	got := false
	err = readSSE(res.Body, func(id string, data []byte) {
		got = true
		if id != "" {
			*lastID = id
		}
		s.write(data)
	})
	if err == nil {
		err = errors.New("stream closed")
	}
	return res.StatusCode, got, err
}

// closeSession ends the MCP session at the hub. The harness has gone, so this is best effort.
func (s *Shim) closeSession() {
	s.mu.Lock()
	s.closing = true
	session := s.session
	s.mu.Unlock()
	if session == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, s.URL, nil)
	if err != nil {
		return
	}
	if headers, err := s.identity(false); err == nil {
		for key, values := range headers {
			req.Header[key] = values
		}
	}
	req.Header.Set("Mcp-Session-Id", session)
	if res, err := s.HTTP.Do(req); err == nil {
		_ = res.Body.Close()
	}
}

// readSSE calls fn with the data of each event until the stream ends. Comment lines are heartbeats.
func readSSE(body io.Reader, fn func(id string, data []byte)) error {
	reader := bufio.NewReaderSize(body, 1<<16)
	var data []string
	id := ""
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if data != nil {
				fn(id, []byte(strings.Join(data, "\n")))
			}
			data, id = nil, ""
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "data":
			data = append(data, value)
		case "id":
			id = value
		}
	}
}
