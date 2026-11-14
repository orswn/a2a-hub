package engine

import (
	"github.com/orswn/a2a-hub/internal/jsonx"
)

// AgentRow is one row of the agents table. A nil pointer is SQL NULL.
type AgentRow struct {
	ID           string
	Kind         string
	Host         *string
	Name         string
	Role         *string
	Cwd          *string
	Labels       string
	Parent       *string
	Topics       string
	Ephemeral    int64
	Online       int64
	State        string
	Activity     *string
	StatusText   string
	StatusAt     *string
	RegisteredAt string
	LastSeenAt   *string
	OfflineAt    *string
}

const agentCols = "id, kind, host, name, role, cwd, labels, parent, topics, ephemeral, online, state, activity, " +
	"status_text, status_at, registered_at, last_seen_at, offline_at"

func scanAgent(scan func(...any)) *AgentRow {
	var r AgentRow
	scan(&r.ID, &r.Kind, &r.Host, &r.Name, &r.Role, &r.Cwd, &r.Labels, &r.Parent, &r.Topics, &r.Ephemeral, &r.Online,
		&r.State, &r.Activity, &r.StatusText, &r.StatusAt, &r.RegisteredAt, &r.LastSeenAt, &r.OfflineAt)
	return &r
}

type taskRow struct {
	ID              string
	AgentID         string
	SenderAgent     *string
	SenderPrincipal string
	ContextID       string
	State           State
	StatusMessage   *string
	StatusAt        string
	Kind            string
	Metadata        *string
	CreatedAt       string
	UpdatedAt       string
}

const taskCols = "id, agent_id, sender_agent, sender_principal, context_id, state, status_message, status_at, kind, " +
	"metadata, created_at, updated_at"

func (r *taskRow) dest() []any {
	return []any{&r.ID, &r.AgentID, &r.SenderAgent, &r.SenderPrincipal, &r.ContextID, &r.State, &r.StatusMessage,
		&r.StatusAt, &r.Kind, &r.Metadata, &r.CreatedAt, &r.UpdatedAt}
}

type messageItemRow struct {
	Seq             int64
	ID              string
	AgentID         string
	SenderAgent     *string
	SenderPrincipal string
	SenderName      string
	SenderKind      string
	ContextID       string
	TaskID          *string
	Kind            string
	Meta            string
	Message         string
	CreatedAt       string
	ReadAt          *string
	ReplyTo         *string
	ToName          *string
	TaskState       *State
}

const messageSelect = "SELECT m.seq, m.id, m.agent_id, m.sender_agent, m.sender_principal, m.sender_name, m.sender_kind, " +
	"m.context_id, m.task_id, m.kind, m.meta, m.message, m.created_at, m.read_at, m.reply_to, a.name AS to_name, " +
	"t.state AS task_state FROM messages m LEFT JOIN agents a ON a.id = m.agent_id LEFT JOIN tasks t ON t.id = m.task_id"

func scanMessageItem(scan func(...any)) *messageItemRow {
	var r messageItemRow
	scan(&r.Seq, &r.ID, &r.AgentID, &r.SenderAgent, &r.SenderPrincipal, &r.SenderName, &r.SenderKind, &r.ContextID,
		&r.TaskID, &r.Kind, &r.Meta, &r.Message, &r.CreatedAt, &r.ReadAt, &r.ReplyTo, &r.ToName, &r.TaskState)
	return &r
}

type pushConfigRow struct {
	TaskID          string
	ID              string
	URL             string
	Token           *string
	AuthScheme      *string
	AuthCredentials *string
	Principal       string
	CreatedAt       string
}

const pushConfigCols = "task_id, id, url, token, auth_scheme, auth_credentials, principal, created_at"

func (r *pushConfigRow) dest() []any {
	return []any{&r.TaskID, &r.ID, &r.URL, &r.Token, &r.AuthScheme, &r.AuthCredentials, &r.Principal, &r.CreatedAt}
}

// mustParse reads a JSON column. The hub wrote it, so a parse failure is a broken database.
func mustParse(text string) any {
	v, err := jsonx.ParseString(text)
	if err != nil {
		panic(dbError{err})
	}
	return v
}

// parseObject reads a JSON object column. NULL, "", and a non-object give nil.
func parseObject(text *string) *jsonx.Object {
	if text == nil || *text == "" {
		return nil
	}
	obj, _ := mustParse(*text).(*jsonx.Object)
	return obj
}

func mustObject(text string) *jsonx.Object {
	obj, _ := mustParse(text).(*jsonx.Object)
	return obj
}

// stringify writes a value like JSON.stringify.
func stringify(v any) string {
	text, err := jsonx.MarshalString(v)
	if err != nil {
		panic(dbError{err})
	}
	return text
}
