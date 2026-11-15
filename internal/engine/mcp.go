package engine

import (
	"strings"

	"github.com/orswn/a2a-hub/internal/jsonx"
)

// InboxNotice tells inbox listeners about a stored message.
type InboxNotice struct {
	AgentID   string
	MessageID string
	ContextID string
	TaskID    string
	ReplyTo   string
	Kind      string
	Wake      string
}

// OnInbox calls listener after each commit that stored a message for an agent. The rules of OnTask apply.
func (e *Engine) OnInbox(listener func(InboxNotice)) func() {
	var id int
	e.listeners(func() { id = e.inboxListeners.add(listener) })
	return func() { e.listeners(func() { e.inboxListeners.remove(id) }) }
}

func (e *Engine) inboxNotice(notice InboxNotice) func() {
	var fns []func(InboxNotice)
	e.listeners(func() { fns = e.inboxListeners.snapshot() })
	return func() {
		for _, fn := range fns {
			e.safely(func() { fn(notice) })
		}
	}
}

// MCPThreadInfo is a thread that an external agent started with a send through the MCP endpoint.
type MCPThreadInfo struct {
	ContextID string
	// Origin is the origin of the first such send in the thread. nil means that send had none.
	Origin *jsonx.Object
}

// mcpThread finds the MCP thread of an external agent by context id, else by the message that replyTo names.
func (e *Engine) mcpThread(agentID, contextID, replyTo string) *MCPThreadInfo {
	var info *MCPThreadInfo
	scan := func(scan func(...any)) {
		var meta string
		info = &MCPThreadInfo{}
		scan(&info.ContextID, &meta)
		if origin, ok := mustObject(meta).Object("origin"); ok {
			info.Origin = origin
		}
	}
	const query = `SELECT context_id, meta FROM messages WHERE sender_agent = ? AND json_extract(meta, '$.mcp') = 1 AND `
	if contextID != "" {
		e.each(query+"context_id = ? ORDER BY seq LIMIT 1", args(agentID, contextID), scan)
	}
	if info == nil && replyTo != "" {
		var context string
		if e.one(query+"id = ? ORDER BY seq LIMIT 1", args(agentID, replyTo), &context, new(string)) {
			e.each(query+"context_id = ? ORDER BY seq LIMIT 1", args(agentID, context), scan)
		}
	}
	return info
}

// MCPThread finds the thread that an external agent started through the MCP endpoint and that a message in
// contextID, or a reply to replyTo, continues. nil means none.
func (e *Engine) MCPThread(agentID, contextID, replyTo string) (*MCPThreadInfo, error) {
	return call(e, func() *MCPThreadInfo { return e.mcpThread(agentID, contextID, replyTo) })
}

// MCPInbox lists the newest messages to the agent like Inbox. For an external agent it leaves out the tasks
// that the hub dispatched to it over A2A.
func (e *Engine) MCPInbox(agentID string, unreadOnly bool, limit int) ([]InboxItem, error) {
	return call(e, func() []InboxItem {
		where := " WHERE m.agent_id = ? AND (m.task_id IS NULL OR t.agent_id IS NULL OR t.agent_id != m.agent_id OR a.kind != ?)"
		if unreadOnly {
			where += " AND m.read_at IS NULL"
		}
		items := []InboxItem{}
		for _, row := range e.messageItems(messageSelect+where+" ORDER BY m.seq DESC LIMIT ?", agentID, KindExternal, limit) {
			items = append(items, e.inboxItem(row, agentID))
		}
		return items
	})
}

// FirstReply gives the oldest unread message to the agent that replies to messageID or carries one of the
// task ids, leaving out status notes. nil means none.
func (e *Engine) FirstReply(agentID, messageID string, taskIDs []string) (*InboxItem, error) {
	return call(e, func() *InboxItem {
		params := []any{agentID, messageID}
		match := "m.reply_to = ?"
		if len(taskIDs) > 0 {
			match += " OR m.task_id IN (" + strings.Repeat("?, ", len(taskIDs)-1) + "?)"
			for _, id := range taskIDs {
				params = append(params, id)
			}
		}
		rows := e.messageItems(messageSelect+" WHERE m.agent_id = ? AND m.read_at IS NULL AND m.kind != 'status' AND ("+match+") ORDER BY m.seq LIMIT 1", params...)
		if len(rows) == 0 {
			return nil
		}
		item := e.inboxItem(rows[0], agentID)
		return &item
	})
}

// TaskOpener gives the id of the first message of a task, the one that made it. "" means none.
func (e *Engine) TaskOpener(taskID string) (string, error) {
	return call(e, func() string {
		var id string
		e.one("SELECT m.id FROM messages m JOIN tasks t ON t.id = m.task_id WHERE m.task_id = ? AND m.agent_id = t.agent_id ORDER BY m.seq LIMIT 1",
			args(taskID), &id)
		return id
	})
}

// Unread reports whether the agent got a message with this id that it did not read yet.
func (e *Engine) Unread(agentID, messageID string) (bool, error) {
	return call(e, func() bool {
		return e.one("SELECT 1 FROM messages WHERE agent_id = ? AND id = ? AND read_at IS NULL", args(agentID, messageID), new(int64))
	})
}
