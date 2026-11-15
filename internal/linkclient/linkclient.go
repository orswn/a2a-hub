// Package linkclient is a client of the hub link API for session agents that a host process serves.
package linkclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Protocol is the link protocol that the client speaks.
const Protocol = 3

// HTTPError is a hub answer with a status other than 200.
type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string {
	var body struct {
		Error string `json:"error"`
	}
	if json.Unmarshal([]byte(e.Body), &body) == nil && body.Error != "" {
		return fmt.Sprintf("hub answered %d: %s", e.Status, body.Error)
	}
	return fmt.Sprintf("hub answered %d", e.Status)
}

// Fatal tells whether a retry cannot help: the token, the request, or the protocol is wrong.
func Fatal(err error) bool {
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		return false
	}
	switch httpErr.Status {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusUpgradeRequired:
		return true
	}
	return false
}

// Client calls the link routes with one host token.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// RegisterBody is the body of POST /link/register.
type RegisterBody struct {
	Agent    string            `json:"agent"`
	Name     string            `json:"name,omitempty"`
	Cwd      string            `json:"cwd,omitempty"`
	Labels   map[string]string `json:"labels,omitempty"`
	Protocol int               `json:"protocol"`
}

// RegisterResult is the answer of POST /link/register.
type RegisterResult struct {
	Agent string `json:"agent"`
	Name  string `json:"name"`
	Host  string `json:"host"`
}

// AgentInfo is the part of GET /agents/:id that the client reads.
type AgentInfo struct {
	ID     string            `json:"id"`
	Name   string            `json:"name"`
	Kind   string            `json:"kind"`
	Labels map[string]string `json:"labels"`
	Online bool              `json:"online"`
	State  string            `json:"state"`
}

func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.http().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return &HTTPError{Status: res.StatusCode, Body: string(raw)}
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

func (c *Client) http() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

func agentPath(agent string) string { return "/link/" + url.PathEscape(agent) }

// Register registers or updates a session agent.
func (c *Client) Register(ctx context.Context, body RegisterBody) (RegisterResult, error) {
	var result RegisterResult
	err := c.do(ctx, http.MethodPost, "/link/register", body, &result)
	return result, err
}

// Agent reads one agent.
func (c *Client) Agent(ctx context.Context, id string) (AgentInfo, error) {
	var info AgentInfo
	err := c.do(ctx, http.MethodGet, "/agents/"+url.PathEscape(id), nil, &info)
	return info, err
}

// Ack acks the events of agent up to seq.
func (c *Client) Ack(ctx context.Context, agent string, upTo int64) error {
	return c.do(ctx, http.MethodPost, agentPath(agent)+"/ack", map[string]int64{"upTo": upTo}, nil)
}

// State sets busy or idle.
func (c *Client) State(ctx context.Context, agent, state string) error {
	return c.do(ctx, http.MethodPost, agentPath(agent)+"/state", map[string]string{"state": state}, nil)
}

// TaskState sets the state of a task that the agent works on.
func (c *Client) TaskState(ctx context.Context, agent, taskID, state string) error {
	return c.do(ctx, http.MethodPost, agentPath(agent)+"/tasks/"+url.PathEscape(taskID), map[string]string{"state": state}, nil)
}

// Close ends the link of agent and marks it offline at once. The mailbox stays.
func (c *Client) Close(ctx context.Context, agent string) error {
	return c.do(ctx, http.MethodDelete, agentPath(agent), nil, nil)
}

// Event is one link event.
type Event struct {
	Seq     int64
	Type    string
	Payload json.RawMessage
}

// Stream reads the link event stream of an agent.
type Stream struct {
	body   io.ReadCloser
	reader *bufio.Reader
}

// Events opens the event stream. afterSeq is the last seq the caller holds; nil replays every unacked event.
func (c *Client) Events(ctx context.Context, agent string, afterSeq *int64) (*Stream, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+agentPath(agent)+"/events", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Accept", "text/event-stream")
	if afterSeq != nil {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(*afterSeq, 10))
	}
	res, err := c.http().Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
		_ = res.Body.Close()
		return nil, &HTTPError{Status: res.StatusCode, Body: string(raw)}
	}
	return &Stream{body: res.Body, reader: newReader(res.Body)}, nil
}

func newReader(body io.Reader) *bufio.Reader { return bufio.NewReaderSize(body, 1<<16) }

// Close ends the stream.
func (s *Stream) Close() error { return s.body.Close() }

// Next returns the next event. alive runs for every line, heartbeats too, so a caller can see a live stream.
// A frame without an id is a replaced event, which the hub does not store; its Seq is -1.
func (s *Stream) Next(alive func()) (Event, error) {
	frame := Event{Seq: -1, Type: "message"}
	var data []string
	for {
		line, err := s.reader.ReadString('\n')
		if err != nil {
			return Event{}, err
		}
		if alive != nil {
			alive()
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if data == nil {
				frame = Event{Seq: -1, Type: "message"}
				continue
			}
			frame.Payload = json.RawMessage(strings.Join(data, "\n"))
			return frame, nil
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "id":
			if n, err := strconv.ParseInt(value, 10, 64); err == nil {
				frame.Seq = n
			}
		case "event":
			frame.Type = value
		case "data":
			data = append(data, value)
		}
	}
}
