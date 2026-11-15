package engine

import (
	"github.com/orswn/a2a-hub/internal/jsonx"
)

// InboxFrom is the sender of an inbox item. Its key order differs from Sender.
type InboxFrom struct {
	Name  string `json:"name"`
	Kind  string `json:"kind"`
	Agent string `json:"agent,omitempty"`
}

// InboxTo is the recipient of an inbox item.
type InboxTo struct {
	Agent string `json:"agent"`
	Name  string `json:"name"`
}

// InboxItem is one inbox or sent message.
type InboxItem struct {
	MessageID   string    `json:"messageId"`
	TaskID      *string   `json:"taskId"`
	ContextID   string    `json:"contextId"`
	Direction   string    `json:"direction"`
	From        InboxFrom `json:"from"`
	To          InboxTo   `json:"to"`
	Kind        string    `json:"kind"`
	Wake        any       `json:"wake"`
	Subject     any       `json:"subject"`
	Priority    any       `json:"priority"`
	ExpectReply bool      `json:"expectReply"`
	ReplyTo     *string   `json:"replyTo"`
	Text        string    `json:"text"`
	CreatedAt   string    `json:"createdAt"`
	Read        bool      `json:"read"`
	Data        any       `json:"data,omitempty"`
	TaskState   string    `json:"taskState,omitempty"`
}

func (e *Engine) inboxItem(row *messageItemRow, me string) InboxItem {
	message := mustObject(row.Message)
	meta := parseObject(&row.Meta)
	item := InboxItem{
		MessageID:   row.ID,
		TaskID:      row.TaskID,
		ContextID:   row.ContextID,
		Direction:   "out",
		From:        InboxFrom{Name: row.SenderName, Kind: row.SenderKind, Agent: deref(row.SenderAgent)},
		To:          InboxTo{Agent: row.AgentID, Name: deref(firstOf(row.ToName, &row.AgentID))},
		Kind:        row.Kind,
		Wake:        orDefault(meta.Value("wake"), "now"),
		Subject:     orDefault(meta.Value("subject"), nil),
		Priority:    orDefault(meta.Value("priority"), nil),
		ExpectReply: meta.Value("expectReply") == true,
		ReplyTo:     row.ReplyTo,
		Text:        MessageText(message),
		CreatedAt:   row.CreatedAt,
		Read:        row.ReadAt != nil,
		Data:        meta.Value("data"),
	}
	if row.AgentID == me {
		item.Direction = "in"
	}
	if row.TaskState != nil && *row.TaskState != "" {
		item.TaskState = LinkState(*row.TaskState)
	}
	return item
}

func (e *Engine) messageItems(query string, params ...any) []*messageItemRow {
	var rows []*messageItemRow
	e.each(query, params, func(scan func(...any)) { rows = append(rows, scanMessageItem(scan)) })
	return rows
}

// Inbox lists the newest messages to the agent.
func (e *Engine) Inbox(agentID string, unreadOnly bool, limit int) ([]InboxItem, error) {
	return call(e, func() []InboxItem {
		unread := ""
		if unreadOnly {
			unread = " AND m.read_at IS NULL"
		}
		items := []InboxItem{}
		for _, row := range e.messageItems(messageSelect+" WHERE m.agent_id = ?"+unread+" ORDER BY m.seq DESC LIMIT ?", agentID, limit) {
			items = append(items, e.inboxItem(row, agentID))
		}
		return items
	})
}

// MarkRead marks inbox messages as read and returns how many were unread.
func (e *Engine) MarkRead(agentID string, ids any) (int64, error) {
	return call(e, func() int64 {
		list := stringList(ids, "ids")
		return txValue(e, func() int64 {
			var changes int64
			at := e.iso()
			for _, id := range list {
				n, _ := e.run("UPDATE messages SET read_at = ? WHERE agent_id = ? AND id = ? AND read_at IS NULL", at, agentID, id)
				changes += n
			}
			return changes
		})
	})
}

// MessageCandidate is one match of an ambiguous message id prefix.
type MessageCandidate struct {
	MessageID string `json:"messageId"`
	ContextID string `json:"contextId"`
	To        string `json:"to"`
	From      string `json:"from"`
	CreatedAt string `json:"createdAt"`
}

// MessageCandidates is the extra field of the 409 body of an ambiguous message id.
type MessageCandidates struct {
	Candidates []MessageCandidate `json:"candidates"`
}

// Message reads one inbox or sent message by id or prefix. A prefix that matches more than one row gives 409
// with the candidates.
func (e *Engine) Message(agentID, idOrPrefix string) (*InboxItem, error) {
	return call(e, func() *InboxItem {
		if idOrPrefix == "" {
			throw(ReasonInvalid, "message id is required")
		}
		rows := e.messageItems(messageSelect+" WHERE (m.agent_id = ? OR m.sender_agent = ?) AND substr(m.id, 1, ?) = ? ORDER BY m.seq DESC LIMIT 50",
			agentID, agentID, jsonx.Len(idOrPrefix), idOrPrefix)
		var exact []*messageItemRow
		for _, row := range rows {
			if row.ID == idOrPrefix {
				exact = append(exact, row)
			}
		}
		matches := rows
		if len(exact) > 0 {
			matches = exact
		}
		if len(matches) == 0 {
			throw(ReasonNotFound, "no message %s", idOrPrefix)
		}
		if len(matches) > 1 {
			details := MessageCandidates{Candidates: []MessageCandidate{}}
			for _, row := range matches {
				from := row.SenderName
				if row.SenderAgent != nil {
					from = *row.SenderAgent
				}
				details.Candidates = append(details.Candidates, MessageCandidate{
					MessageID: row.ID, ContextID: row.ContextID, To: row.AgentID, From: from, CreatedAt: row.CreatedAt,
				})
			}
			panic(newError(ReasonConflict, details, "message id %s is ambiguous", idOrPrefix))
		}
		item := e.inboxItem(matches[0], agentID)
		return &item
	})
}

// ThreadCandidate is one match of an ambiguous context id prefix.
type ThreadCandidate struct {
	ContextID string `json:"contextId"`
	Messages  int64  `json:"messages"`
	LastAt    string `json:"lastAt"`
}

// ThreadCandidates is the extra field of the 409 body of an ambiguous context id.
type ThreadCandidates struct {
	Candidates []ThreadCandidate `json:"candidates"`
}

// ThreadResult is every message of one context.
type ThreadResult struct {
	ContextID string      `json:"contextId"`
	Messages  []InboxItem `json:"messages"`
}

// Thread lists every message this agent sent or got in one context, oldest first.
func (e *Engine) Thread(agentID, contextOrPrefix string) (*ThreadResult, error) {
	return call(e, func() *ThreadResult {
		if contextOrPrefix == "" {
			throw(ReasonInvalid, "context id is required")
		}
		var contexts []ThreadCandidate
		e.each(`SELECT context_id, count(*) AS n, max(created_at) AS last FROM messages
			 WHERE (agent_id = ? OR sender_agent = ?) AND substr(context_id, 1, ?) = ?
			 GROUP BY context_id ORDER BY last DESC LIMIT 50`,
			args(agentID, agentID, jsonx.Len(contextOrPrefix), contextOrPrefix), func(scan func(...any)) {
				var c ThreadCandidate
				scan(&c.ContextID, &c.Messages, &c.LastAt)
				contexts = append(contexts, c)
			})
		matches := contexts
		for _, c := range contexts {
			if c.ContextID == contextOrPrefix {
				matches = []ThreadCandidate{c}
				break
			}
		}
		if len(matches) == 0 {
			throw(ReasonNotFound, "no thread %s", contextOrPrefix)
		}
		if len(matches) > 1 {
			panic(newError(ReasonConflict, ThreadCandidates{Candidates: matches}, "context id %s is ambiguous", contextOrPrefix))
		}
		only := matches[0]
		result := &ThreadResult{ContextID: only.ContextID, Messages: []InboxItem{}}
		for _, row := range e.messageItems(messageSelect+" WHERE (m.agent_id = ? OR m.sender_agent = ?) AND m.context_id = ? ORDER BY m.seq",
			agentID, agentID, only.ContextID) {
			result.Messages = append(result.Messages, e.inboxItem(row, agentID))
		}
		return result
	})
}

// PendingReply is an open question.
type PendingReply struct {
	MessageID string  `json:"messageId"`
	ContextID string  `json:"contextId"`
	TaskID    *string `json:"taskId"`
	Agent     string  `json:"agent"`
	Name      string  `json:"name"`
	CreatedAt string  `json:"createdAt"`
}

// PendingTask is a live task.
type PendingTask struct {
	TaskID    string  `json:"taskId"`
	ContextID string  `json:"contextId"`
	Agent     *string `json:"agent"`
	Name      string  `json:"name"`
	State     string  `json:"state"`
	CreatedAt string  `json:"createdAt"`
}

// PendingResult lists open questions and live tasks. Agent and Name name the other side: the recipient in
// Pending, the sender in Owed.
type PendingResult struct {
	ExpectReply []PendingReply `json:"expectReply"`
	Tasks       []PendingTask  `json:"tasks"`
}

// Pending is what this agent waits for: its open questions and the live tasks it sent.
func (e *Engine) Pending(agentID string) (*PendingResult, error) {
	return call(e, func() *PendingResult {
		return e.openItems(agentID, "sender_agent", "recipient_agent", "sender_agent", "agent_id")
	})
}

// Owed is what others wait for from this agent: open questions to it and the live tasks it got.
func (e *Engine) Owed(agentID string) (*PendingResult, error) {
	return call(e, func() *PendingResult {
		return e.openItems(agentID, "recipient_agent", "sender_agent", "agent_id", "sender_agent")
	})
}

func (e *Engine) openItems(agentID, replyMe, replyOther, taskMe, taskOther string) *PendingResult {
	result := &PendingResult{ExpectReply: []PendingReply{}, Tasks: []PendingTask{}}
	e.each(`SELECT e.message_id, e.context_id, e.task_id, e.`+replyOther+` AS other, a.name AS other_name, e.created_at
			 FROM expect_reply e LEFT JOIN agents a ON a.id = e.`+replyOther+`
			 WHERE e.`+replyMe+` = ? AND e.settled_at IS NULL ORDER BY e.created_at, e.message_id`,
		args(agentID), func(scan func(...any)) {
			var item PendingReply
			var name *string
			scan(&item.MessageID, &item.ContextID, &item.TaskID, &item.Agent, &name, &item.CreatedAt)
			item.Name = deref(firstOf(name, &item.Agent))
			result.ExpectReply = append(result.ExpectReply, item)
		})
	e.each(`SELECT t.id, t.context_id, t.state, t.created_at, t.sender_principal, t.`+taskOther+` AS other, a.name AS other_name
			 FROM tasks t LEFT JOIN agents a ON a.id = t.`+taskOther+`
			 WHERE t.`+taskMe+` = ? AND t.state NOT IN (`+terminalSQL+`) ORDER BY t.created_at, t.id`,
		args(agentID), func(scan func(...any)) {
			var item PendingTask
			var state State
			var principal string
			var name *string
			scan(&item.TaskID, &item.ContextID, &state, &item.CreatedAt, &principal, &item.Agent, &name)
			item.Name = deref(firstOf(name, item.Agent, &principal))
			item.State = LinkState(state)
			result.Tasks = append(result.Tasks, item)
		})
	return result
}

// SweepResult counts the rows that a sweep removed.
type SweepResult struct {
	Tasks    int64 `json:"tasks"`
	Messages int64 `json:"messages"`
	Events   int64 `json:"events"`
	Replies  int64 `json:"replies"`
	Agents   int64 `json:"agents"`
}

// Sweep removes old final tasks, old messages, acked events, settled questions, and agents that were offline
// too long with all their rows.
func (e *Engine) Sweep() (SweepResult, error) {
	return call(e, func() SweepResult {
		return txValue(e, func() SweepResult {
			cutoff := ISO(e.now() - RetentionMS)
			ephemeralCutoff := ISO(e.now() - EphemeralTTLMS)
			var removed SweepResult
			removed.Tasks, _ = e.run("DELETE FROM tasks WHERE updated_at < ? AND state IN ("+terminalSQL+")", cutoff)
			removed.Messages, _ = e.run("DELETE FROM messages WHERE created_at < ?", cutoff)
			removed.Events, _ = e.run("DELETE FROM link_events WHERE acked_at IS NOT NULL AND created_at < ?", cutoff)
			removed.Replies, _ = e.run("DELETE FROM expect_reply WHERE settled_at IS NOT NULL AND settled_at < ?", cutoff)
			offlineCutoff := ISO(e.now() - OfflineTTLMS)
			var gone []string
			e.each(`SELECT id FROM agents WHERE kind = 'session' AND online = 0 AND offline_at IS NOT NULL
				 AND ((ephemeral = 1 AND offline_at < ?) OR offline_at < ?)`, args(ephemeralCutoff, offlineCutoff),
				func(scan func(...any)) {
					var id string
					scan(&id)
					gone = append(gone, id)
				})
			// The id may be registered again by another host, which must not see the old rows.
			for _, id := range gone {
				n, _ := e.run("DELETE FROM messages WHERE agent_id = ? OR sender_agent = ?", id, id)
				removed.Messages += n
				n, _ = e.run("DELETE FROM tasks WHERE agent_id = ? OR sender_agent = ?", id, id)
				removed.Tasks += n
				n, _ = e.run("DELETE FROM expect_reply WHERE sender_agent = ? OR recipient_agent = ?", id, id)
				removed.Replies += n
				n, _ = e.run("DELETE FROM link_events WHERE agent_id = ?", id)
				removed.Events += n
				n, _ = e.run("DELETE FROM agents WHERE id = ?", id)
				removed.Agents += n
			}
			return removed
		})
	})
}
