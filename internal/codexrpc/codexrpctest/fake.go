// Package codexrpctest is a scripted fake Codex app-server for tests: JSON-RPC without "jsonrpc" over a
// websocket on a unix socket.
package codexrpctest

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/orswn/a2a-hub/internal/codexrpc"
)

// Call is one request or answer that a client sent.
type Call struct {
	Conn   *Conn
	ID     json.RawMessage
	Method string
	Params json.RawMessage
	// Answer is set when the client answered a server request instead of calling a method.
	Answer bool
}

// Handler answers a request. A nil error and a nil result answer {}.
type Handler func(call Call) (any, *codexrpc.Error)

// Conn is one client connection.
type Conn struct {
	ws     *websocket.Conn
	server *Server
	mu     sync.Mutex
}

// Server is the fake app-server.
type Server struct {
	Path     string
	listener net.Listener
	http     *http.Server
	mu       sync.Mutex
	cond     *sync.Cond
	conns    map[*Conn]bool
	calls    []Call
	handler  Handler
}

// Start listens on path.
func Start(t *testing.T, path string, handler Handler) *Server {
	t.Helper()
	s := &Server{Path: path, conns: map[*Conn]bool{}, handler: handler}
	s.cond = sync.NewCond(&s.mu)
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	s.listener = listener
	s.http = &http.Server{Handler: http.HandlerFunc(s.serve), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = s.http.Serve(listener) }()
	t.Cleanup(s.Close)
	return s
}

// SetHandler replaces the handler.
func (s *Server) SetHandler(handler Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handler = handler
}

// Close stops the server and drops every connection.
func (s *Server) Close() {
	_ = s.http.Close()
	s.mu.Lock()
	conns := s.conns
	s.conns = map[*Conn]bool{}
	s.mu.Unlock()
	for c := range conns {
		_ = c.ws.CloseNow()
	}
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	ws.SetReadLimit(64 << 20)
	c := &Conn{ws: ws, server: s}
	s.mu.Lock()
	s.conns[c] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		_ = ws.CloseNow()
	}()
	for {
		_, raw, err := ws.Read(context.Background())
		if err != nil {
			return
		}
		var msg struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
			Result json.RawMessage `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if json.Unmarshal(raw, &msg) != nil {
			continue
		}
		call := Call{Conn: c, ID: msg.ID, Method: msg.Method, Params: msg.Params, Answer: msg.Method == ""}
		s.mu.Lock()
		s.calls = append(s.calls, call)
		handler := s.handler
		s.cond.Broadcast()
		s.mu.Unlock()
		if call.Answer || len(msg.ID) == 0 {
			continue
		}
		var result any = map[string]any{}
		var rpcErr *codexrpc.Error
		if handler != nil {
			got, gotErr := handler(call)
			if got != nil {
				result = got
			}
			rpcErr = gotErr
		}
		answer := map[string]any{"id": msg.ID}
		if rpcErr != nil {
			answer["error"] = rpcErr
		} else {
			answer["result"] = result
		}
		_ = c.send(answer)
	}
}

func (c *Conn) send(msg any) error {
	raw, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.ws.Write(ctx, websocket.MessageText, raw)
}

// Notify sends a notification to every client.
func (s *Server) Notify(method string, params any) {
	s.mu.Lock()
	conns := make([]*Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		_ = c.send(map[string]any{"method": method, "params": params, "emittedAtMs": time.Now().UnixMilli()})
	}
}

// Request sends a server request with id to every client.
func (s *Server) Request(id int, method string, params any) {
	s.mu.Lock()
	conns := make([]*Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		_ = c.send(map[string]any{"id": id, "method": method, "params": params})
	}
}

// Clients is the number of open connections.
func (s *Server) Clients() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// Calls returns a copy of every call so far.
func (s *Server) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Call(nil), s.calls...)
}

// ErrTimeout is the error of a wait that timed out.
var ErrTimeout = errors.New("timed out")

// WaitCall waits until the client sent the n-th call (0-based) that match accepts, and returns it.
func (s *Server) WaitCall(timeout time.Duration, n int, match func(Call) bool) (Call, error) {
	deadline := time.Now().Add(timeout)
	timer := time.AfterFunc(timeout, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer timer.Stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		seen := 0
		for _, call := range s.calls {
			if match(call) {
				if seen == n {
					return call, nil
				}
				seen++
			}
		}
		if !time.Now().Before(deadline) {
			return Call{}, ErrTimeout
		}
		s.cond.Wait()
	}
}

// Method matches a call by method.
func Method(method string) func(Call) bool {
	return func(c Call) bool { return !c.Answer && c.Method == method }
}
