package engine

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/orswn/a2a-hub/internal/auth"
	"github.com/orswn/a2a-hub/internal/jsonx"
)

// Protocol constants shared with the clients.
const (
	HubProtocol     = 2
	BusExtensionURI = "https://github.com/orswn/pimon/a2a/bus/v1"
	AgentHeader     = "x-pimon-agent"
	StatusTextMax   = 160
)

// BusKinds lists the message kinds of the bus extension.
var BusKinds = []string{"note", "question", "answer", "task", "result", "correction", "status", "event"}

// taskKinds are the kinds that make a task.
var taskKinds = map[string]bool{"task": true, "question": true, "correction": true}

const dayMS = 24 * 60 * 60 * 1000

// Retention and limits, in milliseconds where the name says MS.
const (
	RetentionMS     = 30 * dayMS
	EphemeralTTLMS  = dayMS
	OfflineTTLMS    = 90 * dayMS
	PushBacklogMax  = 50
	OfflineListMS   = 7 * dayMS
	StaleControlMS  = 30_000
	terminalSQL     = "'TASK_STATE_COMPLETED', 'TASK_STATE_FAILED', 'TASK_STATE_CANCELED', 'TASK_STATE_REJECTED'"
	interruptedSQL  = "'TASK_STATE_INPUT_REQUIRED', 'TASK_STATE_AUTH_REQUIRED'"
	agentIDMaxChars = 128
)

var agentIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// State is an A2A 1.0 task state in its ProtoJSON name.
type State string

// The task states.
const (
	StateSubmitted     State = "TASK_STATE_SUBMITTED"
	StateWorking       State = "TASK_STATE_WORKING"
	StateCompleted     State = "TASK_STATE_COMPLETED"
	StateFailed        State = "TASK_STATE_FAILED"
	StateCanceled      State = "TASK_STATE_CANCELED"
	StateInputRequired State = "TASK_STATE_INPUT_REQUIRED"
	StateRejected      State = "TASK_STATE_REJECTED"
	StateAuthRequired  State = "TASK_STATE_AUTH_REQUIRED"
)

var linkToWire = map[string]State{
	"submitted":      StateSubmitted,
	"working":        StateWorking,
	"input-required": StateInputRequired,
	"completed":      StateCompleted,
	"failed":         StateFailed,
	"canceled":       StateCanceled,
	"rejected":       StateRejected,
	"auth-required":  StateAuthRequired,
}

var wireToLink = func() map[State]string {
	out := map[State]string{}
	for link, wire := range linkToWire {
		out[wire] = link
	}
	return out
}()

var linkPostStates = map[string]bool{"working": true, "input-required": true, "completed": true, "failed": true, "rejected": true}

// LinkState gives the link protocol name of a state, such as "input-required".
func LinkState(s State) string { return wireToLink[s] }

// WireState gives the A2A state of a link protocol state name.
func WireState(link string) (State, bool) {
	s, ok := linkToWire[link]
	return s, ok
}

// IsTerminal reports a final state: completed, failed, canceled, or rejected.
func IsTerminal(s State) bool {
	return s == StateCompleted || s == StateFailed || s == StateCanceled || s == StateRejected
}

// IsInterrupted reports a state that waits for the sender: input-required or auth-required.
func IsInterrupted(s State) bool { return s == StateInputRequired || s == StateAuthRequired }

// Reason is the kind of an Error.
type Reason string

// The error reasons.
const (
	ReasonInvalid       Reason = "invalid"
	ReasonForbidden     Reason = "forbidden"
	ReasonNotFound      Reason = "not-found"
	ReasonTaskNotFound  Reason = "task-not-found"
	ReasonTaskTerminal  Reason = "task-terminal"
	ReasonNotCancelable Reason = "not-cancelable"
	ReasonConflict      Reason = "conflict"
	ReasonUpgrade       Reason = "upgrade"
)

var reasonStatus = map[Reason]int{
	ReasonInvalid:       http.StatusBadRequest,
	ReasonForbidden:     http.StatusForbidden,
	ReasonNotFound:      http.StatusNotFound,
	ReasonTaskNotFound:  http.StatusNotFound,
	ReasonTaskTerminal:  http.StatusConflict,
	ReasonNotCancelable: http.StatusConflict,
	ReasonConflict:      http.StatusConflict,
	ReasonUpgrade:       http.StatusUpgradeRequired,
}

// Error is a refusal that the caller sees. Message and Details go to the client.
type Error struct {
	Reason  Reason
	Status  int
	Message string
	// Details are extra fields of the error body, a struct that encodes to a JSON object, or nil.
	Details any
}

func (e *Error) Error() string { return e.Message }

func newError(reason Reason, details any, format string, args ...any) *Error {
	return &Error{Reason: reason, Status: reasonStatus[reason], Message: fmt.Sprintf(format, args...), Details: details}
}

// throw stops the current engine call with a hub error, like a throw in the TypeScript engine.
func throw(reason Reason, format string, args ...any) {
	panic(newError(reason, nil, format, args...))
}

// UUIDv7 makes a version 7 UUID with the time now in milliseconds.
func UUIDv7(now int64) string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(now)) //nolint:gosec // G115: the 48-bit time field takes the low bytes, as in writeUIntBE.
	copy(b[:6], ts[2:])
	b[6] = b[6]&0x0f | 0x70
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// ISO formats milliseconds since the epoch like Date.prototype.toISOString.
func ISO(ms int64) string {
	return time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z")
}

// DefaultAgentName is "<host>-" and the first 6 hex digits of the sha256 of the id.
func DefaultAgentName(host, id string) string {
	sum := sha256.Sum256([]byte(id))
	return host + "-" + hex.EncodeToString(sum[:])[:6]
}

// MessageText joins the text parts of a message or an artifact with newlines.
func MessageText(message *jsonx.Object) string {
	parts, _ := message.Value("parts").([]any)
	var texts []string
	for _, part := range parts {
		if obj, ok := part.(*jsonx.Object); ok {
			if text, ok := obj.String("text"); ok {
				texts = append(texts, text)
			}
		}
	}
	return strings.Join(texts, "\n")
}

// isJSSpace is the \s class of a JavaScript regular expression and the set that String.prototype.trim removes.
func isJSSpace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0xa0, 0x1680, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000, 0xfeff:
		return true
	}
	return r >= 0x2000 && r <= 0x200a || unicode.Is(unicode.Zs, r)
}

// CleanStatusText puts the status text on one line and cuts it to StatusTextMax UTF-16 code units: a run of
// white space with a line break becomes one space, as /\s*[\r\n]+\s*/g does.
func CleanStatusText(text string) string {
	var b strings.Builder
	runes := []rune(text)
	for i := 0; i < len(runes); {
		if !isJSSpace(runes[i]) {
			b.WriteRune(runes[i])
			i++
			continue
		}
		j := i
		lineBreak := false
		for j < len(runes) && isJSSpace(runes[j]) {
			lineBreak = lineBreak || runes[j] == '\r' || runes[j] == '\n'
			j++
		}
		if lineBreak {
			b.WriteByte(' ')
		} else {
			b.WriteString(string(runes[i:j]))
		}
		i = j
	}
	clean := strings.TrimFunc(b.String(), isJSSpace)
	return strings.TrimFunc(jsonx.Slice(clean, StatusTextMax), isJSSpace)
}

// Caller is the principal of a request and, from the agent header, the agent it acts for.
type Caller struct {
	Principal auth.Principal
	// Agent comes from the agent header, already checked to be bound to the principal's host.
	Agent string
}

// Sender is the from field of a link message event.
type Sender struct {
	Agent string `json:"agent,omitempty"`
	Name  string `json:"name"`
	Kind  string `json:"kind"`
}

// Status is the status of a task.
type Status struct {
	State     State         `json:"state"`
	Timestamp string        `json:"timestamp"`
	Message   *jsonx.Object `json:"message,omitempty"`
}

// Task is an A2A task. Messages and artifacts stay open JSON objects, so unknown fields survive.
type Task struct {
	ID        string          `json:"id"`
	ContextID string          `json:"contextId"`
	Status    Status          `json:"status"`
	Artifacts []*jsonx.Object `json:"artifacts,omitzero"`
	History   []*jsonx.Object `json:"history,omitzero"`
	Metadata  *jsonx.Object   `json:"metadata,omitempty"`
}

// StatusUpdate is a TaskStatusUpdateEvent.
type StatusUpdate struct {
	TaskID    string `json:"taskId"`
	ContextID string `json:"contextId"`
	Status    Status `json:"status"`
}

// ArtifactUpdate is a TaskArtifactUpdateEvent.
type ArtifactUpdate struct {
	TaskID    string        `json:"taskId"`
	ContextID string        `json:"contextId"`
	Artifact  *jsonx.Object `json:"artifact"`
	Append    bool          `json:"append,omitempty"`
	LastChunk bool          `json:"lastChunk,omitempty"`
}

// StreamResponse holds exactly one of its fields.
type StreamResponse struct {
	Task           *Task           `json:"task,omitempty"`
	Message        *jsonx.Object   `json:"message,omitempty"`
	StatusUpdate   *StatusUpdate   `json:"statusUpdate,omitempty"`
	ArtifactUpdate *ArtifactUpdate `json:"artifactUpdate,omitempty"`
}

// TaskEvent tells task listeners about a committed change.
type TaskEvent struct {
	TaskID   string
	AgentID  string
	State    State
	Response StreamResponse
}

// LinkEvent is one event of an agent's link mailbox. Payload is the stored JSON text.
type LinkEvent struct {
	Seq     int64           `json:"seq"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// Authentication of a push config.
type Authentication struct {
	Scheme      string `json:"scheme"`
	Credentials string `json:"credentials,omitempty"`
}

// PushConfig is a TaskPushNotificationConfig.
type PushConfig struct {
	ID             string          `json:"id"`
	TaskID         string          `json:"taskId"`
	URL            string          `json:"url"`
	Token          string          `json:"token,omitempty"`
	Authentication *Authentication `json:"authentication,omitempty"`
}

// Meta is the bus extension metadata of a message after the defaults.
type Meta struct {
	Kind         string  `json:"kind"`
	Wake         string  `json:"wake"`
	Priority     string  `json:"priority,omitempty"`
	Subject      *string `json:"subject,omitempty"`
	ExpectReply  bool    `json:"expectReply,omitempty"`
	DeliveryWait bool    `json:"deliveryWait,omitempty"`
	ReplyTo      string  `json:"replyTo,omitempty"`
	// Data is any parsed JSON value. JSON null stays, a nil means no data.
	Data any `json:"data,omitempty"`
}

// ParseBusMetadata reads the bus extension entry of message metadata and fills the defaults.
func ParseBusMetadata(metadata any) (Meta, error) {
	var meta Meta
	err := catch(func() { meta = parseBusMetadata(metadata) })
	return meta, err
}

func parseBusMetadata(metadata any) Meta {
	obj, _ := metadata.(*jsonx.Object)
	raw := obj.Value(BusExtensionURI)
	if raw == nil || jsonx.IsNull(raw) {
		return Meta{Kind: "task", Wake: "now"}
	}
	value, ok := raw.(*jsonx.Object)
	if !ok {
		throw(ReasonInvalid, "bus metadata must be an object")
	}
	kind, ok := orDefault(value.Value("kind"), "task").(string)
	if !ok || !contains(BusKinds, kind) {
		throw(ReasonInvalid, "bus metadata kind must be one of %s", strings.Join(BusKinds, ", "))
	}
	wake := orDefault(value.Value("wake"), "now")
	if wake != "now" && wake != "never" {
		throw(ReasonInvalid, "bus metadata wake must be now or never")
	}
	priority := value.Value("priority")
	if priority != nil && priority != "normal" && priority != "important" && priority != "urgent" {
		throw(ReasonInvalid, "bus metadata priority must be normal, important, or urgent")
	}
	meta := Meta{Kind: kind, Wake: wake.(string)}
	if priority != nil {
		meta.Priority = priority.(string)
	}
	if subject, ok := value.String("subject"); ok {
		meta.Subject = &subject
	}
	meta.ExpectReply = value.Value("expectReply") == true
	meta.DeliveryWait = value.Value("deliveryWait") == true
	if replyTo, ok := value.String("replyTo"); ok {
		meta.ReplyTo = replyTo
	}
	meta.Data = value.Value("data")
	return meta
}

// orDefault is the ?? operator: null and undefined give the default.
func orDefault(v any, fallback any) any {
	if v == nil || jsonx.IsNull(v) {
		return fallback
	}
	return v
}

func contains(list []string, item string) bool {
	for _, x := range list {
		if x == item {
			return true
		}
	}
	return false
}

// absent is true for undefined and null, the values that the TypeScript checks treat as not given.
func absent(v any) bool { return v == nil || jsonx.IsNull(v) }

func requireString(value any, field string, maxLen int) string {
	s, ok := value.(string)
	if !ok || s == "" {
		throw(ReasonInvalid, "%s must be a non-empty string", field)
	}
	if jsonx.Len(s) > maxLen {
		throw(ReasonInvalid, "%s is longer than %d chars", field, maxLen)
	}
	return s
}

// optionalString gives nil for undefined, null, and "".
func optionalString(value any, field string, maxLen int) *string {
	if absent(value) || value == "" {
		return nil
	}
	s := requireString(value, field, maxLen)
	return &s
}

func stringList(value any, field string) []string {
	if absent(value) {
		return []string{}
	}
	list, ok := value.([]any)
	out := []string{}
	seen := map[string]bool{}
	for _, item := range list {
		s, isString := item.(string)
		if !isString || s == "" {
			ok = false
			break
		}
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	if !ok {
		throw(ReasonInvalid, "%s must be a list of non-empty strings", field)
	}
	return out
}

func labelMap(value any) *jsonx.Object {
	if absent(value) {
		return &jsonx.Object{}
	}
	obj, ok := value.(*jsonx.Object)
	if !ok {
		throw(ReasonInvalid, "labels must be an object")
	}
	out := &jsonx.Object{}
	for _, key := range obj.Keys() {
		s, ok := obj.Value(key).(string)
		if !ok {
			throw(ReasonInvalid, "labels values must be strings")
		}
		out.Set(key, s)
	}
	return out
}

func strPtr(s string) *string { return &s }

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
