package mcp

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/orswn/a2a-hub/internal/a2aapi"
	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/jsonx"
)

// MaxWait is the longest wait of send.
const MaxWait = 30

// toolError is a refusal that the model reads in the tool result.
type toolError string

func (e toolError) Error() string { return string(e) }

func refusef(format string, args ...any) error { return toolError(fmt.Sprintf(format, args...)) }

// errorText gives the text of an error for the model. Errors of unknown origin are logged instead.
func (s *Server) errorText(err error) string {
	var refusal toolError
	if errors.As(err, &refusal) {
		return string(refusal)
	}
	var hubErr *engine.Error
	if errors.As(err, &hubErr) {
		return "The hub refused: " + hubErr.Message
	}
	s.opts.Log("mcp tool failed: " + err.Error())
	return "The hub failed with an internal error."
}

type call struct {
	ctx  context.Context
	sess *session
	self *engine.AgentRow
	args *jsonx.Object
}

func (c *call) caller() engine.Caller {
	return engine.Caller{Principal: c.sess.principal, Agent: c.self.ID}
}

func (s *Server) callTool(r *http.Request, sess *session, id any, params *jsonx.Object) *jsonx.Object {
	name, _ := params.String("name")
	run := map[string]func(*call) (string, error){
		"send": s.send, "reply": s.reply, "inbox": s.inbox, "set_status": s.setStatus, "peers": s.peers,
	}[name]
	if run == nil {
		return rpcError(id, codeInvalidParams, "Unknown tool: "+name)
	}
	args, _ := params.Object("arguments")
	if args == nil {
		args = jsonx.NewObject()
	}
	meta, _ := params.Object("_meta")
	text, err := func() (string, error) {
		self, err := s.selfFor(sess, meta)
		if err != nil {
			return "", err
		}
		return run(&call{ctx: r.Context(), sess: sess, self: self, args: args})
	}()
	isError := err != nil
	if isError {
		text = s.errorText(err)
	}
	return rpcResult(id, jsonx.NewObject(
		"content", []any{jsonx.NewObject("type", "text", "text", text)},
		"isError", isError,
	))
}

// selfFor gives the agent that a call acts for. A Codex call names its thread in _meta, and a thread that the
// hub does not know yet becomes an offline session agent until the Codex adapter links it.
func (s *Server) selfFor(sess *session, meta *jsonx.Object) (*engine.AgentRow, error) {
	e := s.opts.Engine
	if sess.harness != HarnessCodex {
		row, err := e.Agent(sess.agent)
		if err == nil && row == nil {
			err = refusef("Agent %s is gone from the hub. Reconnect the MCP server.", sess.agent)
		}
		return row, err
	}
	thread, _ := meta.String("threadId")
	if thread == "" {
		return nil, refusef("This call has no _meta.threadId, so the hub cannot tell which Codex thread sent it.")
	}
	row, err := e.Agent(thread)
	if err != nil {
		return nil, err
	}
	if row == nil {
		body := jsonx.NewObject("agent", thread, "protocol", float64(engine.HubProtocol),
			"labels", jsonx.NewObject(engine.HarnessLabel, HarnessCodex), "name", "codex-"+jsonx.Slice(thread, 6))
		if sess.cwd != "" {
			body.Set("cwd", sess.cwd)
		}
		if _, err := e.Register(sess.principal, body); err != nil {
			return nil, err
		}
	}
	return e.RequireOwnAgent(sess.principal, thread)
}

func optionalText(args *jsonx.Object, key string) (string, error) {
	v := args.Value(key)
	if v == nil || jsonx.IsNull(v) {
		return "", nil
	}
	text, ok := v.(string)
	if !ok {
		return "", refusef("%s must be a string.", key)
	}
	return text, nil
}

func requiredText(args *jsonx.Object, key string) (string, error) {
	text, err := optionalText(args, key)
	if err == nil && strings.TrimSpace(text) == "" {
		err = refusef("%s is required.", key)
	}
	return text, err
}

func optionalBool(args *jsonx.Object, key string, fallback bool) (bool, error) {
	v := args.Value(key)
	if v == nil || jsonx.IsNull(v) {
		return fallback, nil
	}
	b, ok := v.(bool)
	if !ok {
		return false, refusef("%s must be true or false.", key)
	}
	return b, nil
}

func optionalInt(args *jsonx.Object, key string, fallback, low, high int) (int, error) {
	v := args.Value(key)
	if v == nil || jsonx.IsNull(v) {
		return fallback, nil
	}
	n, ok := v.(float64)
	if !ok || n != math.Trunc(n) || n < float64(low) || n > float64(high) {
		return 0, refusef("%s must be an integer from %d to %d.", key, low, high)
	}
	return int(n), nil
}

func kindArg(args *jsonx.Object, fallback string) (string, error) {
	kind, err := optionalText(args, "kind")
	if err != nil || kind == "" {
		return fallback, err
	}
	if !slices.Contains(kinds, any(kind)) {
		return "", refusef("kind must be one of note, task, question, answer, result, status, correction.")
	}
	return kind, nil
}

// originArg reads the chat origin that the Hermes plugin adds to send. Other harnesses cannot set it.
func originArg(c *call) (*jsonx.Object, error) {
	v := c.args.Value("origin")
	if c.sess.harness != HarnessHermes || v == nil || jsonx.IsNull(v) {
		return nil, nil
	}
	raw, ok := v.(*jsonx.Object)
	if !ok {
		return nil, refusef("origin must be an object.")
	}
	origin := jsonx.NewObject()
	for _, key := range []string{"platform", "chat_id"} {
		text, err := optionalText(raw, key)
		if err != nil {
			return nil, refusef("origin.%s must be a string.", key)
		}
		origin.Set(key, text)
	}
	switch thread := raw.Value("thread_id").(type) {
	case nil:
		origin.Set("thread_id", jsonx.Null)
	case string:
		origin.Set("thread_id", thread)
	default:
		if !jsonx.IsNull(thread) {
			return nil, refusef("origin.thread_id must be a string or null.")
		}
		origin.Set("thread_id", jsonx.Null)
	}
	return origin, nil
}

// resolve finds the recipients of a selector: a plain id or name, id:<id>, name:<name>, or topic:<topic>.
func (s *Server) resolve(self *engine.AgentRow, selector string) ([]*engine.AgentRow, error) {
	selector = strings.TrimSpace(selector)
	if selector == "" {
		return nil, refusef("target is required.")
	}
	if strings.ContainsAny(selector, "|+!") || strings.HasPrefix(selector, "role:") || selector == "children" || selector == "all" {
		return nil, refusef("target %s is not supported here. Use id:<id>, name:<name>, or topic:<topic>.", selector)
	}
	e := s.opts.Engine
	prefix, value, hasPrefix := strings.Cut(selector, ":")
	if !hasPrefix || (prefix != "id" && prefix != "name" && prefix != "topic") {
		prefix, value = "", selector
	}
	if prefix == "id" || prefix == "" {
		row, err := e.Agent(value)
		if err != nil {
			return nil, err
		}
		if row != nil && row.ID != self.ID {
			return []*engine.AgentRow{row}, nil
		}
		if prefix == "id" {
			return nil, refusef("No agent has id %s. Nothing was sent.", value)
		}
	}
	agents, err := e.ListAgents(true)
	if err != nil {
		return nil, err
	}
	var online, offline []engine.AgentInfo
	for _, agent := range agents {
		if agent.ID == self.ID {
			continue
		}
		match := agent.Name == value
		if prefix == "topic" {
			topics, _ := agent.Topics.([]any)
			match = agent.Online && slices.Contains(topics, any(value))
		}
		if !match {
			continue
		}
		if agent.Online || agent.Kind != engine.KindSession {
			online = append(online, agent)
		} else {
			offline = append(offline, agent)
		}
	}
	matches := online
	if prefix != "topic" && len(matches) == 0 {
		matches = offline
	}
	if len(matches) == 0 {
		if prefix == "topic" {
			return nil, refusef("No online agent follows topic %s. Nothing was sent.", value)
		}
		return nil, refusef("No agent is named %s. Use peers to find agents. Nothing was sent.", value)
	}
	if prefix != "topic" && len(matches) > 1 {
		names := make([]string, len(matches))
		for i, agent := range matches {
			names[i] = "id:" + agent.ID
		}
		return nil, refusef("%s matches %d agents: %s. Use id:<id>. Nothing was sent.", value, len(matches), strings.Join(names, ", "))
	}
	rows := make([]*engine.AgentRow, 0, len(matches))
	for _, agent := range matches {
		row, err := e.Agent(agent.ID)
		if err != nil {
			return nil, err
		}
		if row != nil {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

type outgoing struct {
	messageID   string
	contextID   string
	taskID      string
	text        string
	kind        string
	subject     string
	wake        string
	expectReply bool
	replyTo     string
	data        any
	origin      *jsonx.Object
}

// line is the delivery of one message to one recipient.
type line struct {
	name   string
	id     string
	state  string
	note   string
	taskID string
}

// deliver sends one message to each target. A waiter learns each task at once, so that a fast reply finds it.
func (s *Server) deliver(c *call, targets []*engine.AgentRow, out outgoing, w *waiter) []line {
	e := s.opts.Engine
	meta := jsonx.NewObject("kind", out.kind, "wake", out.wake)
	if out.subject != "" {
		meta.Set("subject", out.subject)
	}
	if out.expectReply {
		meta.Set("expectReply", true)
	}
	if out.replyTo != "" {
		meta.Set("replyTo", out.replyTo)
	}
	if out.data != nil {
		meta.Set("data", out.data)
	}
	lines := make([]line, len(targets))
	var wg sync.WaitGroup
	for i, target := range targets {
		message := jsonx.NewObject("messageId", out.messageID, "contextId", out.contextID, "role", "ROLE_USER",
			"parts", []any{jsonx.NewObject("text", out.text)}, "metadata", jsonx.NewObject(engine.BusExtensionURI, meta))
		if out.taskID != "" {
			message.Set("taskId", out.taskID)
		}
		result, err := e.Send(engine.SendInput{
			AgentID: target.ID, Caller: c.caller(), Message: message, MCP: true, Origin: out.origin,
		})
		lines[i] = line{name: target.Name, id: target.ID}
		if err != nil {
			lines[i].state = a2aapi.Failed
			lines[i].note = s.errorText(err)
			continue
		}
		if result.Task != nil {
			lines[i].taskID = result.Task.ID
			if w != nil {
				s.watchTasks(w, []string{result.Task.ID})
			}
			if builtin := s.opts.Builtins[target.ID]; builtin != nil {
				builtin.Start(e, result.Task.ID, message)
			}
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			lines[i].state = a2aapi.Delivery(c.ctx, e, target, result, s)
			bound, _ := s.Served(target.ID)
			if lines[i].state == a2aapi.Unconfirmed && bound {
				lines[i].note = "pushed to its session, which does not confirm"
			}
			if lines[i].state != a2aapi.Queued {
				return
			}
			if bound && out.wake == "never" {
				lines[i].note = "stored in the hub inbox of " + target.Name
			} else if target.Kind == engine.KindSession {
				lines[i].note = "offline; it gets the message when it connects"
			} else {
				lines[i].note = "stored in the hub inbox of " + target.Name
			}
		}()
	}
	wg.Wait()
	return lines
}

func handedOver(lines []line) bool {
	return slices.ContainsFunc(lines, func(l line) bool { return l.state != a2aapi.Failed })
}

func (s *Server) send(c *call) (string, error) {
	target, err := requiredText(c.args, "target")
	if err != nil {
		return "", err
	}
	text, err := requiredText(c.args, "text")
	if err != nil {
		return "", err
	}
	kind, err := kindArg(c.args, "note")
	if err != nil {
		return "", err
	}
	subject, err := optionalText(c.args, "subject")
	if err != nil {
		return "", err
	}
	wake, err := optionalText(c.args, "wake")
	if err != nil {
		return "", err
	}
	if wake == "" {
		wake = "now"
	} else if wake != "now" && wake != "never" {
		return "", refusef("wake must be now or never.")
	}
	expectReply, err := optionalBool(c.args, "expectReply", false)
	if err != nil {
		return "", err
	}
	if v := c.args.Value("wait"); c.sess.harness != HarnessHermes && v != nil && !jsonx.IsNull(v) {
		return "", refusef("wait is not available here. A reply wakes you and arrives in your inbox. Nothing was sent.")
	}
	wait, err := optionalInt(c.args, "wait", 0, 0, MaxWait)
	if err != nil {
		return "", err
	}
	origin, err := originArg(c)
	if err != nil {
		return "", err
	}
	targets, err := s.resolve(c.self, target)
	if err != nil {
		return "", err
	}
	e := s.opts.Engine
	out := outgoing{
		messageID: e.NewID(), contextID: e.NewID(), text: text, kind: kind, subject: subject, wake: wake,
		expectReply: expectReply || kind == "question", data: c.args.Value("data"), origin: origin,
	}
	var w *waiter
	if wait > 0 {
		w = s.addWaiter(c.self.ID, out.messageID, out.contextID)
		defer s.removeWaiter(w)
	}
	lines := s.deliver(c, targets, out, w)
	result := formatSend("Sent to "+target, out.messageID, out.contextID, lines)
	if !handedOver(lines) {
		return "", toolError(result)
	}
	if w == nil {
		return result, nil
	}
	var taskIDs []string
	for _, l := range lines {
		if l.taskID != "" {
			taskIDs = append(taskIDs, l.taskID)
		}
	}
	return result + "\n\n" + s.await(c.ctx, w, taskIDs, time.Duration(wait)*time.Second), nil
}

var replyStates = map[string]string{"result": "completed", "answer": "completed", "question": "input-required", "status": "working"}

func (s *Server) findMessage(self, id string) (*engine.InboxItem, error) {
	item, err := s.opts.Engine.Message(self, id)
	var hubErr *engine.Error
	if errors.As(err, &hubErr) {
		switch hubErr.Reason {
		case engine.ReasonNotFound:
			return nil, refusef("No message %s.", id)
		case engine.ReasonConflict:
			return nil, refusef("Message id %s is ambiguous. Use more characters.", id)
		}
	}
	return item, err
}

func (s *Server) reply(c *call) (string, error) {
	messageID, err := requiredText(c.args, "messageId")
	if err != nil {
		return "", err
	}
	text, err := requiredText(c.args, "text")
	if err != nil {
		return "", err
	}
	kind, err := kindArg(c.args, "answer")
	if err != nil {
		return "", err
	}
	e := s.opts.Engine
	question, err := s.findMessage(c.self.ID, messageID)
	if err != nil {
		return "", err
	}
	if question.Direction == "out" || question.From.Agent == c.self.ID {
		return "", refusef("Message %s was sent by you. Use send to write to its recipient.", question.MessageID)
	}
	if _, err := e.MarkRead(c.self.ID, []any{question.MessageID}); err != nil {
		return "", err
	}
	headline := "Replied to " + question.From.Name
	live := question.TaskID != nil && question.TaskState != "" && !engine.IsTerminal(engine.State(wireState(question.TaskState)))
	if live {
		taskID := *question.TaskID
		owed, err := e.Owed(c.self.ID)
		if err != nil {
			return "", err
		}
		owes := slices.ContainsFunc(owed.Tasks, func(t engine.PendingTask) bool { return t.TaskID == taskID })
		if state := replyStates[kind]; owes && state != "" && c.self.Kind == engine.KindSession {
			body := jsonx.NewObject("state", state, "text", text)
			if state == "completed" {
				body.Set("artifact", jsonx.NewObject("name", "result", "text", text))
			}
			updated, err := e.LinkUpdate(c.self.ID, taskID, body)
			if err == nil {
				note := "task " + state
				messageID := taskID
				if updated.Status.Message != nil {
					messageID, _ = updated.Status.Message.String("messageId")
				}
				return formatSend(headline, messageID, updated.ContextID, []line{{
					name: question.From.Name, id: question.From.Agent, state: a2aapi.Queued, note: note,
				}}), nil
			}
			var hubErr *engine.Error
			if !errors.As(err, &hubErr) || hubErr.Reason != engine.ReasonTaskTerminal {
				return "", err
			}
		} else if question.From.Agent != "" {
			pending, err := e.Pending(c.self.ID)
			if err != nil {
				return "", err
			}
			if slices.ContainsFunc(pending.Tasks, func(t engine.PendingTask) bool { return t.TaskID == taskID }) {
				return s.replyTo(c, headline, question, outgoing{text: text, kind: kind, taskID: taskID})
			}
		}
	}
	if question.From.Agent == "" {
		return "", refusef("Message %s came from %s, which is not a hub agent, and its task is closed. A reply is not possible.",
			question.MessageID, question.From.Name)
	}
	return s.replyTo(c, headline, question, outgoing{text: text, kind: kind, replyTo: question.MessageID})
}

func wireState(linkState string) string {
	if state, ok := engine.WireState(linkState); ok {
		return string(state)
	}
	return linkState
}

func (s *Server) replyTo(c *call, headline string, question *engine.InboxItem, out outgoing) (string, error) {
	target, err := s.opts.Engine.Agent(question.From.Agent)
	if err != nil {
		return "", err
	}
	if target == nil {
		return "", refusef("Agent %s is gone from the hub.", question.From.Agent)
	}
	out.messageID = s.opts.Engine.NewID()
	out.contextID = question.ContextID
	out.wake = "now"
	lines := s.deliver(c, []*engine.AgentRow{target}, out, nil)
	result := formatSend(headline, out.messageID, out.contextID, lines)
	if !handedOver(lines) {
		return "", toolError(result)
	}
	return result, nil
}

func (s *Server) inbox(c *call) (string, error) {
	e := s.opts.Engine
	unreadOnly, err := optionalBool(c.args, "unreadOnly", true)
	if err != nil {
		return "", err
	}
	limit, err := optionalInt(c.args, "limit", 10, 1, 50)
	if err != nil {
		return "", err
	}
	messageID, err := optionalText(c.args, "messageId")
	if err != nil {
		return "", err
	}
	threadID, err := optionalText(c.args, "threadId")
	if err != nil {
		return "", err
	}
	var items []engine.InboxItem
	switch {
	case messageID != "":
		item, err := s.findMessage(c.self.ID, messageID)
		if err != nil {
			return "", err
		}
		items = []engine.InboxItem{*item}
	case threadID != "":
		thread, err := e.Thread(c.self.ID, threadID)
		var hubErr *engine.Error
		if errors.As(err, &hubErr) && hubErr.Reason == engine.ReasonNotFound {
			return "", refusef("No thread %s.", threadID)
		}
		if errors.As(err, &hubErr) && hubErr.Reason == engine.ReasonConflict {
			return "", refusef("Thread id %s is ambiguous. Use more characters.", threadID)
		}
		if err != nil {
			return "", err
		}
		items = thread.Messages
	default:
		items, err = e.MCPInbox(c.self.ID, unreadOnly, limit)
		if err != nil {
			return "", err
		}
		slices.Reverse(items)
		if len(items) == 0 {
			if unreadOnly {
				return "No unread messages.", nil
			}
			return "No messages.", nil
		}
	}
	var unread []any
	for _, item := range items {
		if item.Direction == "in" && !item.Read {
			unread = append(unread, item.MessageID)
		}
	}
	if len(unread) > 0 {
		if _, err := e.MarkRead(c.self.ID, unread); err != nil {
			return "", err
		}
	}
	return formatItems(items), nil
}

func (s *Server) setStatus(c *call) (string, error) {
	text, err := optionalText(c.args, "text")
	if err != nil {
		return "", err
	}
	clean, err := s.opts.Engine.SetStatus(c.self.ID, text)
	if err != nil {
		return "", err
	}
	if clean == "" {
		return "Status cleared.", nil
	}
	return "Status set: " + clean, nil
}

func (s *Server) peers(c *call) (string, error) {
	filter, err := optionalText(c.args, "filter")
	if err != nil {
		return "", err
	}
	agents, err := s.opts.Engine.ListAgents(false)
	if err != nil {
		return "", err
	}
	needle := strings.ToLower(strings.TrimSpace(filter))
	var shown []engine.AgentInfo
	for _, agent := range agents {
		if agent.ID == c.self.ID || (needle != "" && !strings.Contains(searchText(agent), needle)) {
			continue
		}
		shown = append(shown, agent)
	}
	if len(shown) == 0 {
		if needle != "" {
			return "No online agent matches " + filter + ".", nil
		}
		return "No other agent is online.", nil
	}
	out := make([]string, len(shown))
	for i, agent := range shown {
		out[i] = describeAgent(agent)
	}
	return strings.Join(out, "\n"), nil
}
