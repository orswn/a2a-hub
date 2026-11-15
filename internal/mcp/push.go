package mcp

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/orswn/a2a-hub/internal/a2aapi"
	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/external"
	"github.com/orswn/a2a-hub/internal/jsonx"
)

// channelSink turns the link events of a Claude Code agent into channel notifications. Claude Code never acks
// one, so the hub acks each event itself once it is written or skipped, and a new stream does not repeat it.
type channelSink struct {
	engine   *engine.Engine
	agent    string
	replaced bool
}

func (c *channelSink) Replaced() []byte {
	c.replaced = true
	return nil
}

func (c *channelSink) Frame(event engine.LinkEvent) ([]byte, bool) {
	var content, messageID, senderKind, kind string
	switch event.Type {
	case "message":
		var m engine.MessageEvent
		if json.Unmarshal(event.Payload, &m) != nil || m.Wake != "now" {
			return nil, true
		}
		content, messageID, senderKind, kind = m.From.Name+": "+m.Text, m.MessageID, m.From.Kind, m.Kind
	case "update":
		var u engine.UpdateEvent
		if json.Unmarshal(event.Payload, &u) != nil || u.MessageID == "" || u.Text == "" || (u.Kind != "result" && u.Kind != "question") {
			return nil, true
		}
		content, messageID, kind = u.From.Name+": "+u.Text, u.MessageID, u.Kind
		if row, err := c.engine.Agent(u.From.Agent); err == nil && row != nil {
			senderKind = row.Kind
		}
	default:
		return nil, true
	}
	// A message that the model already read through a tool is not pushed again after a reconnect.
	if unread, err := c.engine.Unread(c.agent, messageID); err == nil && !unread {
		return nil, true
	}
	meta := jsonx.NewObject("message_id", messageID, "sender_kind", senderKind, "kind", kind)
	notification := jsonx.NewObject("jsonrpc", "2.0", "method", "notifications/claude/channel",
		"params", jsonx.NewObject("content", content, "meta", meta))
	frame := append([]byte("event: message\ndata: "), jsonx.Stringify(notification)...)
	return append(frame, "\n\n"...), true
}

type waiter struct {
	agent     string
	messageID string
	contextID string
	// claimed is the one stored message that this waiter keeps from the webhook.
	claimed  *engine.InboxNotice
	tasks    map[string]bool
	wake     chan struct{}
	offTasks []func()
}

func (w *waiter) signal() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (s *Server) addWaiter(agent, messageID, contextID string) *waiter {
	w := &waiter{agent: agent, messageID: messageID, contextID: contextID, tasks: map[string]bool{}, wake: make(chan struct{}, 1)}
	s.waitMu.Lock()
	defer s.waitMu.Unlock()
	s.waiters[w] = struct{}{}
	return w
}

func (s *Server) removeWaiter(w *waiter) {
	s.waitMu.Lock()
	delete(s.waiters, w)
	offs := w.offTasks
	w.offTasks = nil
	claimed := w.claimed
	s.waitMu.Unlock()
	for _, off := range offs {
		off()
	}
	if claimed != nil {
		// The wait may end without reading the claimed message. push skips it if it was read.
		s.enqueue(*claimed)
	}
}

func (s *Server) watchTasks(w *waiter, taskIDs []string) {
	s.waitMu.Lock()
	defer s.waitMu.Unlock()
	for _, id := range taskIDs {
		w.tasks[id] = true
		w.offTasks = append(w.offTasks, s.opts.Engine.OnTask(id, func(event engine.TaskEvent) {
			if a2aapi.EndsWait(event.State) {
				w.signal()
			}
		}))
	}
}

// claim gives a stored message to a waiting send that it answers. Each waiter holds back at most one message from
// the webhook, because the send returns only the first reply. The thread match of a task message covers a task
// reply that comes before the waiter knows the task.
func (s *Server) claim(n engine.InboxNotice) bool {
	if n.Kind == "status" {
		return false
	}
	s.waitMu.Lock()
	defer s.waitMu.Unlock()
	for w := range s.waiters {
		if w.claimed != nil || w.agent != n.AgentID {
			continue
		}
		if (n.ReplyTo != "" && n.ReplyTo == w.messageID) || (n.TaskID != "" && w.tasks[n.TaskID]) ||
			(n.TaskID != "" && n.ContextID != "" && n.ContextID == w.contextID) {
			w.claimed = &n
			w.signal()
			return true
		}
	}
	return false
}

// await waits for the first reply to a send and gives its text for the tool result.
func (s *Server) await(ctx context.Context, w *waiter, taskIDs []string, wait time.Duration) string {
	e := s.opts.Engine
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		item, err := e.FirstReply(w.agent, w.messageID, taskIDs)
		if err != nil {
			return s.errorText(err)
		}
		if item != nil {
			if _, err := e.MarkRead(w.agent, []any{item.MessageID}); err != nil {
				return s.errorText(err)
			}
			return "First reply:\n" + formatItem(*item)
		}
		for _, id := range taskIDs {
			if state, err := e.TaskState(id); err == nil && a2aapi.EndsWait(state) {
				return "Task " + id + " is " + engine.LinkState(state) + " with no reply text."
			}
		}
		select {
		case <-w.wake:
		case <-timer.C:
			return noReplyText
		case <-ctx.Done():
			return noReplyText
		}
	}
}

func (s *Server) onInbox(n engine.InboxNotice) {
	if !s.claim(n) {
		s.enqueue(n)
	}
}

func (s *Server) enqueue(n engine.InboxNotice) {
	if n.Wake != "now" || s.opts.Webhooks == nil || s.opts.Webhooks.Webhook(n.AgentID) == nil {
		return
	}
	select {
	case s.pushes <- n:
	default:
		s.opts.Log("webhook queue full: message " + n.MessageID + " for " + n.AgentID + " stays in its inbox")
	}
}

// sendPushes posts the webhooks one at a time, so a chat gets the replies in order.
func (s *Server) sendPushes() {
	defer close(s.done)
	for {
		select {
		case <-s.stop:
			return
		case n := <-s.pushes:
			s.push(n)
		}
	}
}

func originText(origin *jsonx.Object, key string) string {
	text, _ := origin.String(key)
	return text
}

// WebhookBody is the JSON body of a webhook post. Every origin field is a string, "" when unknown, because the
// Hermes route template renders null as text.
func WebhookBody(item engine.InboxItem, origin *jsonx.Object, harness, inReplyTo string) []byte {
	taskID := ""
	if item.TaskID != nil {
		taskID = *item.TaskID
	}
	return jsonx.Stringify(jsonx.NewObject(
		"event", "a2a-hub.message",
		"origin", jsonx.NewObject("platform", originText(origin, "platform"), "chat_id", originText(origin, "chat_id"),
			"thread_id", originText(origin, "thread_id")),
		"from", jsonx.NewObject("id", item.From.Agent, "name", item.From.Name, "harness", harness),
		"kind", item.Kind,
		"message_id", item.MessageID,
		"in_reply_to", inReplyTo,
		"hub_thread_id", item.ContextID,
		"task_id", taskID,
		"state", item.TaskState,
		"text", item.Text,
	))
}

// Sign gives the generic HMAC V2 signature of the Hermes webhook platform: hex HMAC-SHA256 of
// "<timestamp>.<body>".
func Sign(secret, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp + "."))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func (s *Server) push(n engine.InboxNotice) {
	e := s.opts.Engine
	hook := s.opts.Webhooks.Webhook(n.AgentID)
	if hook == nil {
		return
	}
	thread, err := e.MCPThread(n.AgentID, n.ContextID, n.ReplyTo)
	if err != nil || thread == nil {
		return
	}
	item, err := e.Message(n.AgentID, n.MessageID)
	if err != nil {
		s.opts.Log("webhook skipped for message " + n.MessageID + ": " + err.Error())
		return
	}
	if item.Read {
		return
	}
	harness := ""
	if item.From.Agent != "" {
		if row, err := e.Agent(item.From.Agent); err == nil && row != nil {
			harness = harnessLabel(row.Labels)
		}
	}
	inReplyTo := ""
	if item.ReplyTo != nil {
		inReplyTo = *item.ReplyTo
	} else if item.TaskID != nil {
		inReplyTo, _ = e.TaskOpener(*item.TaskID)
	}
	s.postWebhook(*hook, item.MessageID, n.AgentID, WebhookBody(*item, thread.Origin, harness, inReplyTo))
}

func harnessLabel(labels string) string {
	parsed, err := jsonx.ParseString(labels)
	if err != nil {
		return ""
	}
	obj, _ := parsed.(*jsonx.Object)
	harness, _ := obj.String(engine.HarnessLabel)
	return harness
}

// postWebhook tries a webhook post again on a network error or a 5xx answer. The message stays in the inbox either
// way.
func (s *Server) postWebhook(hook external.Webhook, messageID, agentID string, body []byte) {
	requestID := messageID
	for try := 1; ; try++ {
		status, err := s.postOnce(hook, requestID, body)
		if err == nil && status < 300 {
			return
		}
		retry := err != nil || status >= 500
		if !retry || try > len(s.opts.RetryDelays) {
			reason := "status " + strconv.Itoa(status)
			if err != nil {
				reason = err.Error()
			}
			s.opts.Log("webhook delivery failed; the message stays in the inbox: message " + messageID + " for " +
				agentID + ", " + reason)
			return
		}
		// Hermes records a request id before it delivers. After a 5xx it did not deliver, and the same id would
		// get 200 duplicate. After a network error it may have delivered, so the id stays.
		if err == nil {
			requestID = messageID + ".r" + strconv.Itoa(try+1)
		}
		select {
		case <-s.stop:
			return
		case <-time.After(s.opts.RetryDelays[try-1]):
		}
	}
}

func (s *Server) postOnce(hook external.Webhook, requestID string, body []byte) (int, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-s.stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, hook.URL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	timestamp := strconv.FormatInt(s.opts.Now().Unix(), 10)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Request-ID", requestID)
	request.Header.Set("X-Webhook-Timestamp", timestamp)
	request.Header.Set("X-Webhook-Signature-V2", Sign(hook.Secret, timestamp, body))
	response, err := s.opts.Client.Do(request)
	if err != nil {
		return 0, err
	}
	_ = response.Body.Close()
	return response.StatusCode, nil
}
