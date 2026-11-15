package server

import (
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/jsonx"
)

type okResult struct {
	OK bool `json:"ok"`
}

// inboxLimit is Math.min(Math.max(Number(limit ?? 20) || 20, 1), 200).
func inboxLimit(raw string, present bool) int {
	if !present {
		return 20
	}
	n, err := strconv.ParseFloat(strings.TrimFunc(raw, isJSSpace), 64)
	if strings.TrimFunc(raw, isJSSpace) == "" || err != nil || n == 0 || math.IsNaN(n) {
		n = 20
	}
	return int(math.Min(math.Max(n, 1), 200))
}

func (s *Server) linkRoute(w http.ResponseWriter, r *http.Request, route string, rest []string) error {
	principal, err := s.requireHost(r)
	if err != nil {
		return err
	}
	e := s.opts.Engine
	reply := func(body any, err error) error {
		if err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, body, nil)
		return nil
	}
	if route == "POST /link/register" {
		body, err := readJSON(r)
		if err != nil {
			return err
		}
		return reply(e.Register(principal, body))
	}
	if len(rest) == 0 || rest[0] == "" || len(rest) > 3 {
		return fail(http.StatusNotFound, "not found")
	}
	agentID := rest[0]
	var arg string
	if len(rest) == 3 {
		arg = rest[2]
	}
	if _, err := e.RequireOwnAgent(principal, agentID); err != nil {
		return err
	}
	withBody := func(fn func(body *jsonx.Object) error) error {
		body, err := readJSON(r)
		if err != nil {
			return err
		}
		return fn(body)
	}
	switch route {
	case "GET /link/:agent/events":
		s.opts.Link.Serve(w, r, agentID, lastEventID(r))
		return nil
	case "DELETE /link/:agent":
		if err := s.opts.Link.OfflineNow(agentID); err != nil {
			return err
		}
		return reply(okResult{true}, nil)
	case "POST /link/:agent/ack":
		return withBody(func(body *jsonx.Object) error {
			acked, err := e.Ack(agentID, body.Value("upTo"))
			return reply(struct {
				Acked int64 `json:"acked"`
			}{acked}, err)
		})
	case "POST /link/:agent/state":
		return withBody(func(body *jsonx.Object) error {
			return reply(okResult{true}, e.SetState(agentID, body.Value("state"), body.Value("activity")))
		})
	case "POST /link/:agent/status":
		return withBody(func(body *jsonx.Object) error {
			text, err := e.SetStatus(agentID, body.Value("text"))
			return reply(struct {
				Text string `json:"text"`
			}{text}, err)
		})
	case "POST /link/:agent/tasks/:taskId":
		return withBody(func(body *jsonx.Object) error {
			return reply(e.LinkUpdate(agentID, arg, body))
		})
	case "POST /link/:agent/topics":
		return withBody(func(body *jsonx.Object) error {
			topics, err := e.SetTopics(agentID, body.Value("subscribe"), body.Value("unsubscribe"))
			return reply(struct {
				Topics []string `json:"topics"`
			}{topics}, err)
		})
	case "POST /link/:agent/control":
		return withBody(func(body *jsonx.Object) error {
			return reply(okResult{true}, e.Control(agentID, body.Value("target"), body.Value("action")))
		})
	case "GET /link/:agent/inbox":
		query := r.URL.Query()
		limit := inboxLimit(query.Get("limit"), query.Has("limit"))
		messages, err := e.Inbox(agentID, query.Get("unread") == "1", limit)
		return reply(struct {
			Messages []engine.InboxItem `json:"messages"`
		}{messages}, err)
	case "POST /link/:agent/read":
		return withBody(func(body *jsonx.Object) error {
			read, err := e.MarkRead(agentID, body.Value("ids"))
			return reply(struct {
				Read int64 `json:"read"`
			}{read}, err)
		})
	case "GET /link/:agent/message/:idOrPrefix":
		return reply(e.Message(agentID, arg))
	case "GET /link/:agent/pending":
		return reply(e.Pending(agentID))
	case "GET /link/:agent/owed":
		return reply(e.Owed(agentID))
	case "GET /link/:agent/thread/:contextOrPrefix":
		return reply(e.Thread(agentID, arg))
	case "POST /link/:agent/children":
		return withBody(func(body *jsonx.Object) error {
			return reply(e.AddChild(principal, agentID, body))
		})
	}
	return fail(http.StatusNotFound, "not found")
}
