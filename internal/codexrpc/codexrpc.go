// Package codexrpc is a client of the Codex app-server: JSON-RPC over a websocket on a unix socket. The
// app-server leaves out the "jsonrpc" member, so the client does not require or send it.
package codexrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/coder/websocket"
)

// Error codes of the app-server.
const (
	CodeInvalidRequest = -32600
	CodeOverloaded     = -32001
)

// Error is a JSON-RPC error answer.
type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *Error) Error() string { return fmt.Sprintf("app-server error %d: %s", e.Code, e.Message) }

// ErrClosed is the error of a call on a closed connection.
var ErrClosed = errors.New("app-server connection closed")

// Message is one inbound notification or server request. A server request has an ID.
type Message struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

type envelope struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *Error          `json:"error,omitempty"`
}

type outbound struct {
	ID     *int64 `json:"id,omitempty"`
	Method string `json:"method"`
	Params any    `json:"params,omitempty"`
}

type reply struct {
	result json.RawMessage
	err    error
}

// Conn is one app-server connection.
type Conn struct {
	ws      *websocket.Conn
	onEvent func(Message)
	writeMu sync.Mutex
	mu      sync.Mutex
	nextID  int64
	pending map[int64]chan reply
	done    chan struct{}
	closed  bool
	err     error
}

// Dial connects to the app-server socket at path. onEvent runs in the read loop for each notification and
// server request, in order; it must not block on a call of the same connection.
func Dial(ctx context.Context, path string, onEvent func(Message)) (*Conn, error) {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", path)
		},
	}
	ws, _, err := websocket.Dial(ctx, "ws://localhost/", &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		return nil, err
	}
	ws.SetReadLimit(64 << 20)
	c := &Conn{ws: ws, onEvent: onEvent, pending: map[int64]chan reply{}, done: make(chan struct{})}
	go c.readLoop() //nolint:gosec // G118: the read loop lives as long as the connection, not the dial.
	return c, nil
}

// Done closes when the connection ends.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Err is why the connection ended.
func (c *Conn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Close ends the connection.
func (c *Conn) Close() error {
	return c.ws.Close(websocket.StatusNormalClosure, "")
}

func (c *Conn) readLoop() {
	ctx := context.Background()
	var failure error
	for {
		_, raw, err := c.ws.Read(ctx)
		if err != nil {
			failure = err
			break
		}
		var msg envelope
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}
		if msg.Method == "" {
			var id int64
			if json.Unmarshal(msg.ID, &id) != nil {
				continue
			}
			c.mu.Lock()
			ch := c.pending[id]
			delete(c.pending, id)
			c.mu.Unlock()
			if ch != nil {
				if msg.Error != nil {
					ch <- reply{err: msg.Error}
				} else {
					ch <- reply{result: msg.Result}
				}
			}
			continue
		}
		if c.onEvent != nil {
			c.onEvent(Message{ID: msg.ID, Method: msg.Method, Params: msg.Params})
		}
	}
	c.mu.Lock()
	c.closed = true
	c.err = failure
	pending := c.pending
	c.pending = map[int64]chan reply{}
	c.mu.Unlock()
	for _, ch := range pending {
		ch <- reply{err: ErrClosed}
	}
	close(c.done)
}

func (c *Conn) write(ctx context.Context, msg outbound) error {
	raw, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.ws.Write(ctx, websocket.MessageText, raw)
}

// Call sends a request and waits for its answer. An error answer is an *Error.
func (c *Conn) Call(ctx context.Context, method string, params any, result any) error {
	ch := make(chan reply, 1)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	c.nextID++
	id := c.nextID
	c.pending[id] = ch
	c.mu.Unlock()
	if err := c.write(ctx, outbound{ID: &id, Method: method, Params: params}); err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return err
	}
	select {
	case r := <-ch:
		if r.err != nil {
			return r.err
		}
		if result == nil {
			return nil
		}
		return json.Unmarshal(r.result, result)
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return ctx.Err()
	}
}

// Notify sends a notification.
func (c *Conn) Notify(ctx context.Context, method string, params any) error {
	return c.write(ctx, outbound{Method: method, Params: params})
}

// ClientInfo names the client in initialize.
type ClientInfo struct {
	Name    string `json:"name"`
	Title   string `json:"title,omitempty"`
	Version string `json:"version"`
}

// Initialize runs the initialize handshake. optOut lists notifications that the client does not want.
func (c *Conn) Initialize(ctx context.Context, info ClientInfo, optOut []string) error {
	params := map[string]any{
		"clientInfo": info,
		"capabilities": map[string]any{
			"experimentalApi":           true,
			"optOutNotificationMethods": optOut,
		},
	}
	if err := c.Call(ctx, "initialize", params, nil); err != nil {
		return err
	}
	return c.Notify(ctx, "initialized", nil)
}

// IsCode tells whether err is an app-server error with code.
func IsCode(err error, code int) bool {
	var rpcErr *Error
	return errors.As(err, &rpcErr) && rpcErr.Code == code
}
