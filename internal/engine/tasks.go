package engine

import (
	"context"
	"encoding/base64"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/orswn/a2a-hub/internal/auth"
	"github.com/orswn/a2a-hub/internal/jsonx"
)

// ---------- tasks: reads ----------

func (e *Engine) taskRow(id string) *taskRow {
	rows := e.tasks("SELECT "+taskCols+" FROM tasks WHERE id = ?", id)
	if len(rows) == 0 {
		return nil
	}
	return rows[0]
}

func (e *Engine) tasks(query string, params ...any) []*taskRow {
	var rows []*taskRow
	e.each(query, params, func(scan func(...any)) {
		var row taskRow
		scan(row.dest()...)
		rows = append(rows, &row)
	})
	return rows
}

func (e *Engine) visible(task *taskRow, agentID string, caller Caller) bool {
	if task.AgentID != agentID {
		return false
	}
	if caller.Agent != "" {
		return deref(task.SenderAgent) == caller.Agent && task.SenderAgent != nil || task.AgentID == caller.Agent
	}
	if task.SenderPrincipal == caller.Principal.Name {
		return true
	}
	if caller.Principal.Kind != auth.KindHost {
		return false
	}
	agent := e.agent(task.AgentID)
	return agent != nil && agent.Host != nil && *agent.Host == caller.Principal.Name
}

func (e *Engine) visibleTask(agentID string, caller Caller, id string) *taskRow {
	var row *taskRow
	if id != "" {
		row = e.taskRow(id)
	}
	if row == nil || !e.visible(row, agentID, caller) {
		throw(ReasonTaskNotFound, "Task not found: %s", id)
	}
	return row
}

func (e *Engine) toWire(row *taskRow, history, artifacts bool) *Task {
	task := &Task{ID: row.ID, ContextID: row.ContextID, Status: Status{State: row.State, Timestamp: row.StatusAt}}
	task.Status.Message = parseObject(row.StatusMessage)
	if artifacts {
		task.Artifacts = []*jsonx.Object{}
		e.each("SELECT artifact FROM artifacts WHERE task_id = ? ORDER BY seq", args(row.ID), func(scan func(...any)) {
			var text string
			scan(&text)
			task.Artifacts = append(task.Artifacts, mustObject(text))
		})
	}
	if history {
		task.History = []*jsonx.Object{}
		e.each("SELECT message FROM task_messages WHERE task_id = ? ORDER BY seq", args(row.ID), func(scan func(...any)) {
			var text string
			scan(&text)
			task.History = append(task.History, mustObject(text))
		})
	}
	if metadata := parseObject(row.Metadata); metadata.Len() > 0 {
		task.Metadata = metadata
	}
	return task
}

// ApplyHistoryLength keeps the last historyLength messages. Zero or less drops the history. nil keeps all.
func ApplyHistoryLength(task *Task, historyLength *int) *Task {
	if historyLength == nil {
		return task
	}
	out := *task
	if *historyLength <= 0 {
		out.History = nil
		return &out
	}
	history := task.History
	if history == nil {
		history = []*jsonx.Object{}
	}
	out.History = history[max(0, len(history)-*historyLength):]
	return &out
}

// GetTask reads a task that the caller may see.
func (e *Engine) GetTask(agentID string, caller Caller, id string, historyLength *int) (*Task, error) {
	return call(e, func() *Task {
		return ApplyHistoryLength(e.toWire(e.visibleTask(agentID, caller, id), true, true), historyLength)
	})
}

// WatchTask reads a task like GetTask and registers fn for its later changes in the same call, so no change
// falls between the read and the watch. The result removes fn. The listener rules of OnTask apply.
func (e *Engine) WatchTask(agentID string, caller Caller, id string, historyLength *int, fn func(TaskEvent)) (*Task, func(), error) {
	var off func()
	task, err := call(e, func() *Task {
		task := ApplyHistoryLength(e.toWire(e.visibleTask(agentID, caller, id), true, true), historyLength)
		off = e.addTaskListener(id, fn)
		return task
	})
	if err != nil {
		return nil, nil, err
	}
	return task, off, nil
}

// TaskState reads the state of a task. "" means no such task.
func (e *Engine) TaskState(id string) (State, error) {
	return call(e, func() State {
		var state State
		e.one("SELECT state FROM tasks WHERE id = ?", args(id), &state)
		return state
	})
}

// ReadTask reads a task without a scope check. nil means no such task.
func (e *Engine) ReadTask(id string) (*Task, error) {
	return call(e, func() *Task {
		if row := e.taskRow(id); row != nil {
			return e.toWire(row, true, true)
		}
		return nil
	})
}

// ListTasksInput filters and pages ListTasks.
type ListTasksInput struct {
	ContextID            string
	State                State
	StatusTimestampAfter string
	PageSize             int
	PageToken            string
	HistoryLength        *int
	IncludeArtifacts     bool
}

// ListTasksResult is one page of tasks, newest status first.
type ListTasksResult struct {
	Tasks         []*Task `json:"tasks"`
	NextPageToken string  `json:"nextPageToken"`
	PageSize      int     `json:"pageSize"`
	TotalSize     int64   `json:"totalSize"`
}

// ListTasks lists the tasks of an agent that the caller may see.
func (e *Engine) ListTasks(agentID string, caller Caller, input ListTasksInput) (*ListTasksResult, error) {
	return call(e, func() *ListTasksResult {
		where := []string{"agent_id = ?"}
		params := []any{agentID}
		if caller.Agent != "" {
			where = append(where, "(sender_agent = ? OR agent_id = ?)")
			params = append(params, caller.Agent, caller.Agent)
		} else if agent := e.agent(agentID); caller.Principal.Kind == auth.KindHost && agent != nil && agent.Host != nil && *agent.Host == caller.Principal.Name {
			// every task of an agent bound to this host is visible
		} else {
			where = append(where, "sender_principal = ?")
			params = append(params, caller.Principal.Name)
		}
		if input.ContextID != "" {
			where = append(where, "context_id = ?")
			params = append(params, input.ContextID)
		}
		if input.State != "" {
			where = append(where, "state = ?")
			params = append(params, string(input.State))
		}
		if input.StatusTimestampAfter != "" {
			ms, ok := ParseTimestamp(input.StatusTimestampAfter)
			if !ok {
				throw(ReasonInvalid, "statusTimestampAfter must be a valid ISO 8601 timestamp")
			}
			where = append(where, "status_at >= ?")
			params = append(params, ISO(ms))
		}
		filter := strings.Join(where, " AND ")
		totalSize := e.count("SELECT count(*) AS n FROM tasks WHERE "+filter, params...)
		pageFilter := filter
		pageParams := append([]any{}, params...)
		if input.PageToken != "" {
			statusAt, id := decodePageToken(input.PageToken)
			pageFilter += " AND (status_at < ? OR (status_at = ? AND id < ?))"
			pageParams = append(pageParams, statusAt, statusAt, id)
		}
		rows := e.tasks("SELECT "+taskCols+" FROM tasks WHERE "+pageFilter+" ORDER BY status_at DESC, id DESC LIMIT ?",
			append(pageParams, input.PageSize+1)...)
		page := rows[:min(len(rows), input.PageSize)]
		result := &ListTasksResult{Tasks: []*Task{}, PageSize: input.PageSize, TotalSize: totalSize}
		for _, row := range page {
			result.Tasks = append(result.Tasks, ApplyHistoryLength(e.toWire(row, true, input.IncludeArtifacts), input.HistoryLength))
		}
		if len(rows) > input.PageSize && len(page) > 0 {
			last := page[len(page)-1]
			result.NextPageToken = encodePageToken(last.StatusAt, last.ID)
		}
		return result
	})
}

func encodePageToken(statusAt, id string) string {
	return base64.RawURLEncoding.EncodeToString(jsonx.Stringify([]any{statusAt, id}))
}

// decodePageToken reads a token like Buffer.from(token, "base64url"): it stops at "=", takes "+" and "/" as
// "-" and "_", and skips every other character outside the alphabet.
func decodePageToken(token string) (string, string) {
	if i := strings.IndexByte(token, '='); i >= 0 {
		token = token[:i]
	}
	var clean strings.Builder
	for _, c := range token {
		switch {
		case c == '+':
			clean.WriteByte('-')
		case c == '/':
			clean.WriteByte('_')
		case c == '-' || c == '_' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9':
			clean.WriteRune(c)
		}
	}
	text := clean.String()
	if len(text)%4 == 1 {
		text = text[:len(text)-1]
	}
	data, err := base64.RawURLEncoding.DecodeString(text)
	if err == nil {
		if !utf8.Valid(data) {
			data = []byte(strings.ToValidUTF8(string(data), "\uFFFD"))
		}
		if v, err := jsonx.Parse(data); err == nil {
			if list, ok := v.([]any); ok && len(list) >= 2 {
				statusAt, ok1 := list[0].(string)
				id, ok2 := list[1].(string)
				if ok1 && ok2 {
					return statusAt, id
				}
			}
		}
	}
	throw(ReasonInvalid, "pageToken is not valid")
	return "", ""
}

// ---------- tasks: writes ----------

func (e *Engine) senderOf(caller Caller) Sender {
	if caller.Agent != "" {
		if row := e.agent(caller.Agent); row != nil {
			return Sender{Agent: row.ID, Name: row.Name, Kind: row.Kind}
		}
	}
	return Sender{Name: caller.Principal.Name, Kind: string(caller.Principal.Kind)}
}

func (e *Engine) emitTask(task *taskRow, response StreamResponse, state State) {
	var configs []string
	e.each("SELECT id FROM push_configs WHERE task_id = ?", args(task.ID), func(scan func(...any)) {
		var id string
		scan(&id)
		configs = append(configs, id)
	})
	if len(configs) > 0 {
		payload := stringify(response)
		at := e.now()
		for _, config := range configs {
			e.run("INSERT INTO push_outbox (task_id, config_id, payload, attempts, next_at) VALUES (?, ?, ?, 0, ?)",
				task.ID, config, payload, at)
			e.run(`DELETE FROM push_outbox WHERE task_id = ? AND config_id = ? AND id NOT IN
					 (SELECT id FROM push_outbox WHERE task_id = ? AND config_id = ? ORDER BY id DESC LIMIT ?)`,
				task.ID, config, task.ID, config, PushBacklogMax)
		}
	}
	event := TaskEvent{TaskID: task.ID, AgentID: task.AgentID, State: state, Response: response}
	e.afterCommit(func() func() {
		runTask := e.taskNotice(event)
		if len(configs) == 0 {
			return runTask
		}
		runPush := e.notice(&e.pushListeners)
		return func() {
			runTask()
			runPush()
		}
	})
}

func (e *Engine) appendHistory(taskID string, message *jsonx.Object) {
	next := e.count("SELECT max(seq) AS n FROM task_messages WHERE task_id = ?", taskID) + 1
	e.run("INSERT INTO task_messages (task_id, seq, message) VALUES (?, ?, ?)", taskID, next, stringify(message))
}

func (e *Engine) writeStatus(task *taskRow, state State, message *jsonx.Object) Status {
	at := e.iso()
	var text *string
	if message != nil {
		text = strPtr(stringify(message))
	}
	e.run("UPDATE tasks SET state = ?, status_message = ?, status_at = ?, updated_at = ? WHERE id = ?",
		string(state), text, at, at, task.ID)
	status := Status{State: state, Timestamp: at, Message: message}
	e.emitTask(task, StreamResponse{StatusUpdate: &StatusUpdate{TaskID: task.ID, ContextID: task.ContextID, Status: status}}, state)
	return status
}

func (e *Engine) insertInbox(agentID string, sender Sender, caller Caller, contextID string, taskID *string, meta Meta, message *jsonx.Object) {
	var senderAgent, replyTo *string
	if sender.Agent != "" {
		senderAgent = &sender.Agent
	}
	if meta.ReplyTo != "" {
		replyTo = &meta.ReplyTo
	}
	messageID, _ := message.String("messageId")
	e.run(`INSERT INTO messages (id, agent_id, sender_agent, sender_principal, sender_name, sender_kind, context_id, task_id, kind, meta, message, created_at, reply_to)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		messageID, agentID, senderAgent, caller.Principal.Name, sender.Name, sender.Kind, contextID, taskID,
		meta.Kind, stringify(meta), stringify(message), e.iso(), replyTo)
}

// PushConfigInput is a push config in a send or a create. ID and TaskID may be empty.
type PushConfigInput struct {
	ID             string
	TaskID         string
	URL            string
	Token          string
	Authentication *Authentication
}

// SendInput is one message to an agent.
type SendInput struct {
	AgentID string
	Caller  Caller
	// Message is the A2A message in ProtoJSON. Unknown fields stay.
	Message    *jsonx.Object
	PushConfig *PushConfigInput
	// Watch, when set and the send makes or continues a task, gets the later changes of that task. It is
	// registered inside the call, so it misses no change that another caller commits after the send, and it
	// gets none of the send's own events. SendResult.Unwatch removes it. The listener rules of OnTask apply.
	Watch func(TaskEvent)
}

// SendResult holds the task, or for a message without a task the "stored" reply.
type SendResult struct {
	Task    *Task
	Message *jsonx.Object
	Created bool
	// EventSeq is the seq of the link event, nil when the target has no mailbox.
	EventSeq *int64
	Meta     Meta
	// Unwatch removes SendInput.Watch. It is a no-op when there is no watch.
	Unwatch func() `json:"-"`
}

// MessageEvent is the payload of a message link event.
type MessageEvent struct {
	MessageID   string  `json:"messageId"`
	ContextID   string  `json:"contextId"`
	From        Sender  `json:"from"`
	Kind        string  `json:"kind"`
	Wake        string  `json:"wake"`
	Text        string  `json:"text"`
	TaskID      string  `json:"taskId,omitempty"`
	TaskState   string  `json:"taskState,omitempty"`
	Subject     *string `json:"subject,omitempty"`
	Priority    string  `json:"priority,omitempty"`
	ExpectReply bool    `json:"expectReply,omitempty"`
	ReplyTo     string  `json:"replyTo,omitempty"`
	Data        any     `json:"data,omitempty"`
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

// Send delivers a message: a follow-up of a task, a new task, or a plain message for the inbox.
func (e *Engine) Send(input SendInput) (*SendResult, error) {
	return call(e, func() *SendResult { return e.send(input) })
}

func (e *Engine) send(input SendInput) *SendResult {
	caller := input.Caller
	incoming := input.Message
	if !truthy(incoming.Value("messageId")) {
		throw(ReasonInvalid, "message.messageId is required")
	}
	meta := parseBusMetadata(incoming.Value("metadata"))
	if input.PushConfig != nil {
		e.validatePushURL(input.PushConfig.URL)
	}
	result := txValue(e, func() *SendResult {
		agent := e.requireAgent(input.AgentID)
		sender := e.senderOf(caller)
		var task *taskRow
		created := false
		var contextID string
		var stored *jsonx.Object
		incomingContext := str(incoming.Value("contextId"))
		var followUp *taskRow
		if taskID := str(incoming.Value("taskId")); taskID != "" {
			followUp = e.visibleTask(agent.ID, caller, taskID)
		} else {
			followUp = e.openTaskInContext(agent, caller, incomingContext)
		}
		switch {
		case followUp != nil:
			task = followUp
			if IsTerminal(task.State) {
				throw(ReasonTaskTerminal, "Task %s is in a terminal state and cannot accept messages", task.ID)
			}
			if incomingContext != "" && incomingContext != task.ContextID {
				throw(ReasonInvalid, "contextId %s does not match task %s", incomingContext, task.ID)
			}
			// The external agent starts a new remote task per message, and a busy context would hold it unsent.
			if agent.Kind == "external" && !IsInterrupted(task.State) {
				throw(ReasonInvalid, "Task %s of external agent %s takes a follow-up only when it waits for input", task.ID, agent.Name)
			}
			contextID = task.ContextID
			stored = incoming.Clone().Set("taskId", task.ID).Set("contextId", contextID)
			e.appendHistory(task.ID, stored)
			e.run("UPDATE tasks SET updated_at = ? WHERE id = ?", e.iso(), task.ID)
			if IsInterrupted(task.State) {
				var statusText *jsonx.Object
				if agent.StatusText != "" {
					statusText = e.statusTextMessage(task, agent.StatusText)
				}
				e.writeStatus(task, StateWorking, statusText)
				copied := *task
				copied.State = StateWorking
				task = &copied
			}
		case taskKinds[meta.Kind]:
			at := e.iso()
			id := e.NewID()
			contextID = incomingContext
			if contextID == "" {
				contextID = e.NewID()
			}
			stored = incoming.Clone().Set("taskId", id).Set("contextId", contextID)
			var senderAgent *string
			if sender.Agent != "" {
				senderAgent = &sender.Agent
			}
			e.run(`INSERT INTO tasks (id, agent_id, sender_agent, sender_principal, context_id, state, status_message, status_at, kind, metadata, created_at, updated_at)
					 VALUES (?, ?, ?, ?, ?, 'TASK_STATE_SUBMITTED', NULL, ?, ?, NULL, ?, ?)`,
				id, agent.ID, senderAgent, caller.Principal.Name, contextID, at, meta.Kind, at, at)
			e.appendHistory(id, stored)
			task = e.taskRow(id)
			created = true
		default:
			if agent.Kind == "external" {
				throw(ReasonInvalid, "external agents take only task, question, or correction messages")
			}
			contextID = incomingContext
			if contextID == "" {
				contextID = e.NewID()
			}
			stored = incoming.Clone().Set("contextId", contextID)
		}
		if task != nil && input.PushConfig != nil {
			config := *input.PushConfig
			config.TaskID = task.ID
			e.savePushConfig(task, caller, config)
		}
		if task != nil && created {
			e.emitTask(task, StreamResponse{Task: e.toWire(task, true, true)}, task.State)
		}
		var taskID *string
		if task != nil {
			taskID = &task.ID
		}
		e.insertInbox(agent.ID, sender, caller, contextID, taskID, meta, stored)
		storedID, _ := stored.String("messageId")
		if task != nil && agent.Kind == "external" {
			e.queueExternal(task, "send", &storedID)
		}
		payload := MessageEvent{
			MessageID: storedID, ContextID: contextID, From: sender, Kind: meta.Kind, Wake: meta.Wake,
			Text: MessageText(stored), Subject: meta.Subject, Priority: meta.Priority, ExpectReply: meta.ExpectReply,
			ReplyTo: meta.ReplyTo, Data: meta.Data,
		}
		if task != nil {
			payload.TaskID = task.ID
			payload.TaskState = LinkState(task.State)
		}
		eventSeq := e.emit(agent.ID, "message", payload)
		if meta.ExpectReply && caller.Agent != "" {
			e.run(`INSERT INTO expect_reply (sender_agent, message_id, recipient_agent, context_id, task_id, created_at) VALUES (?, ?, ?, ?, ?, ?)
					 ON CONFLICT (sender_agent, message_id, recipient_agent) DO NOTHING`,
				caller.Agent, storedID, agent.ID, contextID, taskID, e.iso())
		}
		if meta.ReplyTo != "" {
			if caller.Agent != "" {
				e.run(`UPDATE expect_reply SET settled_at = ?
						 WHERE sender_agent = ? AND message_id = ? AND recipient_agent = ? AND settled_at IS NULL`,
					e.iso(), agent.ID, meta.ReplyTo, caller.Agent)
			} else {
				e.run("UPDATE expect_reply SET settled_at = ? WHERE sender_agent = ? AND message_id = ? AND settled_at IS NULL",
					e.iso(), agent.ID, meta.ReplyTo)
			}
		}
		if task != nil {
			row := e.taskRow(task.ID)
			if row == nil {
				row = task
			}
			return &SendResult{Task: e.toWire(row, true, true), Created: created, EventSeq: eventSeq, Meta: meta}
		}
		reply := jsonx.NewObject(
			"messageId", e.NewID(),
			"contextId", contextID,
			"role", "ROLE_AGENT",
			"parts", []any{jsonx.NewObject("text", "stored")},
		)
		return &SendResult{Message: reply, Created: created, EventSeq: eventSeq, Meta: meta}
	})
	result.Unwatch = func() {}
	if input.Watch != nil && result.Task != nil {
		result.Unwatch = e.addTaskListener(result.Task.ID, input.Watch)
	}
	return result
}

// objectBody reads a value that must be an object. An array passes with no fields, as typeof gives "object"
// for it in JavaScript.
func objectBody(v any, refusal string) *jsonx.Object {
	switch t := v.(type) {
	case *jsonx.Object:
		if t != nil {
			return t
		}
	case []any:
		return &jsonx.Object{}
	}
	throw(ReasonInvalid, "%s", refusal)
	return nil
}

// truthy is JavaScript truthiness for a parsed JSON value.
func truthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case string:
		return t != ""
	case float64:
		return t != 0 && t == t
	}
	return !jsonx.IsNull(v)
}

// openTaskInContext finds the one interrupted task between the same sender and target in the context. A peer
// or an external agent cannot carry a hub task id, so its follow-up names only the context.
func (e *Engine) openTaskInContext(agent *AgentRow, caller Caller, contextID string) *taskRow {
	if contextID == "" || caller.Principal == auth.Anonymous || agent.Kind == "builtin" {
		return nil
	}
	if caller.Principal.Kind != auth.KindPeer && agent.Kind != "external" {
		return nil
	}
	var callerAgent *string
	if caller.Agent != "" {
		callerAgent = &caller.Agent
	}
	rows := e.tasks(`SELECT `+taskCols+` FROM tasks WHERE agent_id = ? AND context_id = ? AND sender_principal = ?
			 AND ((? IS NULL AND sender_agent IS NULL) OR sender_agent = ?)
			 AND state IN ('TASK_STATE_INPUT_REQUIRED', 'TASK_STATE_AUTH_REQUIRED') LIMIT 2`,
		agent.ID, contextID, caller.Principal.Name, callerAgent, callerAgent)
	if len(rows) == 1 {
		return rows[0]
	}
	return nil
}

// ArtifactChange is one artifact of a TaskChange.
type ArtifactChange struct {
	Artifact *jsonx.Object
	Append   bool
	// LastChunk false sends no lastChunk flag. nil means true.
	LastChunk *bool
}

// TaskChange is one change from the agent side.
type TaskChange struct {
	State State
	// Message is an agent message: it goes to the history and to status.message.
	Message   *jsonx.Object
	Artifacts []ArtifactChange
}

// UpdateFrom names the agent of an update event.
type UpdateFrom struct {
	Agent string `json:"agent"`
	Name  string `json:"name"`
}

// UpdatedArtifact names an artifact in an update event.
type UpdatedArtifact struct {
	Name string `json:"name,omitempty"`
	Text string `json:"text,omitempty"`
}

// UpdateEvent is the payload of an update link event.
type UpdateEvent struct {
	TaskID    string            `json:"taskId"`
	ContextID string            `json:"contextId"`
	State     string            `json:"state"`
	From      UpdateFrom        `json:"from"`
	MessageID string            `json:"messageId,omitempty"`
	Kind      string            `json:"kind,omitempty"`
	Text      string            `json:"text,omitempty"`
	Artifacts []UpdatedArtifact `json:"artifacts,omitempty"`
}

// ApplyChange applies one change from the agent side.
func (e *Engine) ApplyChange(taskID string, change TaskChange) (*Task, error) {
	return call(e, func() *Task { return e.applyChange(taskID, change) })
}

func (e *Engine) applyChange(taskID string, change TaskChange) *Task {
	return txValue(e, func() *Task {
		task := e.taskRow(taskID)
		if task == nil {
			throw(ReasonTaskNotFound, "Task not found: %s", taskID)
		}
		if IsTerminal(task.State) {
			throw(ReasonTaskTerminal, "Task %s is already %s", taskID, task.State)
		}
		agent := e.requireAgent(task.AgentID)
		var updated []UpdatedArtifact
		for _, item := range change.Artifacts {
			artifact := e.mergeArtifact(task, item.Artifact, item.Append)
			update := &ArtifactUpdate{TaskID: task.ID, ContextID: task.ContextID, Artifact: item.Artifact, Append: item.Append}
			update.LastChunk = item.LastChunk == nil || *item.LastChunk
			e.emitTask(task, StreamResponse{ArtifactUpdate: update}, task.State)
			var entry UpdatedArtifact
			entry.Name, _ = artifact.String("name")
			entry.Text = MessageText(artifact)
			updated = append(updated, entry)
		}
		state := task.State
		message := change.Message
		if message != nil {
			message = message.Clone().Set("taskId", task.ID).Set("contextId", task.ContextID)
			e.appendHistory(task.ID, message)
		}
		if change.State != "" {
			state = change.State
			statusMessage := message
			if statusMessage == nil && state == StateWorking && agent.StatusText != "" {
				statusMessage = e.statusTextMessage(task, agent.StatusText)
			}
			e.writeStatus(task, state, statusMessage)
		} else {
			e.run("UPDATE tasks SET updated_at = ? WHERE id = ?", e.iso(), task.ID)
		}
		if task.SenderAgent != nil && *task.SenderAgent != "" {
			update := UpdateEvent{
				TaskID: task.ID, ContextID: task.ContextID, State: LinkState(state),
				From: UpdateFrom{Agent: agent.ID, Name: agent.Name},
			}
			text := MessageText(message)
			if message != nil && text != "" {
				meta := updateMeta(state)
				principal := auth.Principal{Name: deref(agent.Host), Kind: auth.KindHost}
				if agent.Host == nil {
					principal.Name = agent.ID
				}
				if agent.Kind == "external" {
					principal = auth.Principal{Name: agent.Name, Kind: auth.KindPeer}
				}
				e.insertInbox(*task.SenderAgent, Sender{Agent: agent.ID, Name: agent.Name, Kind: agent.Kind},
					Caller{Principal: principal, Agent: agent.ID}, task.ContextID, &task.ID, meta, message)
				update.MessageID, _ = message.String("messageId")
				update.Kind = meta.Kind
				update.Text = text
			}
			update.Artifacts = updated
			e.emit(*task.SenderAgent, "update", update)
		}
		if task.SenderAgent != nil && *task.SenderAgent != "" && (IsTerminal(state) || IsInterrupted(state)) {
			e.settleTask(task)
		}
		row := e.taskRow(task.ID)
		if row == nil {
			row = task
		}
		return e.toWire(row, true, true)
	})
}

func (e *Engine) mergeArtifact(task *taskRow, artifact *jsonx.Object, appendParts bool) *jsonx.Object {
	artifactID := artifact.Value("artifactId")
	if !truthy(artifactID) {
		throw(ReasonInvalid, "artifact.artifactId is required")
	}
	var existing string
	if e.one("SELECT artifact FROM artifacts WHERE task_id = ? AND artifact_id = ?", args(task.ID, artifactID), &existing) {
		merged := artifact
		if appendParts {
			previous := mustObject(existing)
			parts, _ := previous.Value("parts").([]any)
			added, _ := artifact.Value("parts").([]any)
			merged = previous.Clone().Set("parts", append(append([]any{}, parts...), added...))
		}
		e.run("UPDATE artifacts SET artifact = ? WHERE task_id = ? AND artifact_id = ?", stringify(merged), task.ID, artifactID)
		return merged
	}
	seq := e.count("SELECT max(seq) AS n FROM artifacts WHERE task_id = ?", task.ID) + 1
	e.run("INSERT INTO artifacts (task_id, artifact_id, seq, artifact) VALUES (?, ?, ?, ?)", task.ID, artifactID, seq, stringify(artifact))
	return artifact
}

// LinkUpdate is POST /link/<agent>/tasks/<taskId>: a state, a text, and an artifact from a session agent.
func (e *Engine) LinkUpdate(agentID, taskID string, body any) (*Task, error) {
	return call(e, func() *Task {
		obj := objectBody(body, "body must be an object")
		state, ok := obj.Value("state").(string)
		if !ok || !linkPostStates[state] {
			throw(ReasonInvalid, "state must be working, input-required, completed, failed, or rejected")
		}
		task := e.taskRow(taskID)
		if task == nil || task.AgentID != agentID {
			throw(ReasonTaskNotFound, "Task not found: %s", taskID)
		}
		if IsTerminal(task.State) {
			throw(ReasonTaskTerminal, "Task %s is already %s", taskID, task.State)
		}
		change := TaskChange{State: linkToWire[state]}
		if text := obj.Value("text"); text != nil {
			s, ok := text.(string)
			if !ok {
				throw(ReasonInvalid, "text must be a string")
			}
			if s != "" {
				change.Message = e.agentMessage(s)
			}
		}
		if raw := obj.Value("artifact"); raw != nil {
			spec := objectBody(raw, "artifact must be an object")
			parts := []any{}
			if text, ok := spec.String("text"); ok {
				parts = append(parts, jsonx.NewObject("text", text))
			}
			if data := spec.Value("data"); data != nil {
				parts = append(parts, jsonx.NewObject("data", data, "mediaType", "application/json"))
			}
			if len(parts) == 0 {
				throw(ReasonInvalid, "artifact needs text or data")
			}
			artifact := jsonx.NewObject("artifactId", e.NewID(), "parts", parts)
			if name, ok := spec.String("name"); ok && name != "" {
				artifact.Set("name", name)
			}
			change.Artifacts = []ArtifactChange{{Artifact: artifact}}
		}
		e.touch(agentID)
		return e.applyChange(taskID, change)
	})
}

func (e *Engine) agentMessage(text string) *jsonx.Object {
	return jsonx.NewObject("messageId", e.NewID(), "role", "ROLE_AGENT", "parts", []any{jsonx.NewObject("text", text)})
}

// AgentMessage makes an agent message with one text part and a new id.
func (e *Engine) AgentMessage(text string) *jsonx.Object {
	out, _ := call(e, func() *jsonx.Object { return e.agentMessage(text) })
	return out
}

// CancelEvent is the payload of a cancel link event.
type CancelEvent struct {
	TaskID string `json:"taskId"`
}

// Cancel cancels a task that the caller may see.
func (e *Engine) Cancel(agentID string, caller Caller, id string) (*Task, error) {
	return call(e, func() *Task {
		return txValue(e, func() *Task {
			task := e.visibleTask(agentID, caller, id)
			if IsTerminal(task.State) {
				throw(ReasonNotCancelable, "Task not cancelable: %s", id)
			}
			e.writeStatus(task, StateCanceled, nil)
			e.emit(task.AgentID, "cancel", CancelEvent{TaskID: task.ID})
			if agent := e.agent(task.AgentID); agent != nil && agent.Kind == "external" {
				e.run("DELETE FROM external_outbox WHERE task_id = ? AND op = 'send'", task.ID)
				e.queueExternal(task, "cancel", nil)
			}
			if task.SenderAgent != nil && *task.SenderAgent != "" {
				agent := e.requireAgent(task.AgentID)
				e.emit(*task.SenderAgent, "update", UpdateEvent{
					TaskID: task.ID, ContextID: task.ContextID, State: "canceled",
					From: UpdateFrom{Agent: agent.ID, Name: agent.Name},
				})
				e.settleTask(task)
			}
			row := e.taskRow(task.ID)
			if row == nil {
				row = task
			}
			return e.toWire(row, true, true)
		})
	})
}

func (e *Engine) settleTask(task *taskRow) {
	e.run("UPDATE expect_reply SET settled_at = ? WHERE sender_agent = ? AND task_id = ? AND settled_at IS NULL",
		e.iso(), task.SenderAgent, task.ID)
}

// WaitForTask returns once the task reaches a state that stop accepts, the timeout passes, or ctx is done.
func (e *Engine) WaitForTask(ctx context.Context, taskID string, stop func(State) bool, timeout time.Duration) {
	reached := make(chan struct{}, 1)
	off := e.OnTask(taskID, func(event TaskEvent) {
		if stop(event.State) {
			select {
			case reached <- struct{}{}:
			default:
			}
		}
	})
	defer off()
	state, err := e.TaskState(taskID)
	if err != nil || state == "" || stop(state) {
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-reached:
	case <-timer.C:
	case <-ctx.Done():
	}
}

// updateMeta is the inbox metadata of a task update with text. A plain progress note does not wake the sender.
func updateMeta(state State) Meta {
	if IsTerminal(state) {
		return Meta{Kind: "result", Wake: "now"}
	}
	if IsInterrupted(state) {
		return Meta{Kind: "question", Wake: "now"}
	}
	return Meta{Kind: "status", Wake: "never"}
}
