package engine

import (
	"slices"
	"strconv"
	"strings"

	"github.com/orswn/a2a-hub/internal/auth"
	"github.com/orswn/a2a-hub/internal/jsonx"
)

// AgentStatus is the status line of an agent.
type AgentStatus struct {
	Text string  `json:"text"`
	At   *string `json:"at"`
}

// AgentInfo is an agent as GET /agents lists it.
type AgentInfo struct {
	ID         string        `json:"id"`
	Name       string        `json:"name"`
	Kind       string        `json:"kind"`
	Host       *string       `json:"host"`
	Cwd        *string       `json:"cwd"`
	Role       *string       `json:"role"`
	Labels     *jsonx.Object `json:"labels"`
	Parent     *string       `json:"parent"`
	Topics     any           `json:"topics"`
	Online     bool          `json:"online"`
	State      string        `json:"state"`
	Activity   *string       `json:"activity"`
	Status     AgentStatus   `json:"status"`
	LastSeenAt *string       `json:"lastSeenAt"`
}

// RegisterResult answers a register or a child registration.
type RegisterResult struct {
	Agent string `json:"agent"`
	Name  string `json:"name"`
	Host  string `json:"host"`
}

// RefResult is the result of ResolveRef: one agent, or the ids of every online match of an ambiguous name.
type RefResult struct {
	Agent     *AgentRow
	Ambiguous []string
}

func (e *Engine) agent(id string) *AgentRow {
	var row *AgentRow
	e.each("SELECT "+agentCols+" FROM agents WHERE id = ?", args(id), func(scan func(...any)) { row = scanAgent(scan) })
	return row
}

func (e *Engine) agents(query string, params ...any) []*AgentRow {
	var rows []*AgentRow
	e.each(query, params, func(scan func(...any)) { rows = append(rows, scanAgent(scan)) })
	return rows
}

// Agent reads one agent row. nil means no such agent.
func (e *Engine) Agent(id string) (*AgentRow, error) {
	return call(e, func() *AgentRow { return e.agent(id) })
}

func (e *Engine) requireAgent(id string) *AgentRow {
	row := e.agent(id)
	if row == nil {
		throw(ReasonNotFound, "unknown agent %s", id)
	}
	return row
}

// RequireAgent reads an agent or fails with not-found.
func (e *Engine) RequireAgent(id string) (*AgentRow, error) {
	return call(e, func() *AgentRow { return e.requireAgent(id) })
}

func (e *Engine) requireOwnAgent(principal auth.Principal, id string) *AgentRow {
	row := e.agent(id)
	if row == nil || row.Kind != KindSession || principal.Kind != auth.KindHost || row.Host == nil || *row.Host != principal.Name {
		throw(ReasonForbidden, "agent %s is not bound to %s", id, principal.Name)
	}
	return row
}

// RequireOwnAgent reads an agent that must exist and be bound to the host principal.
func (e *Engine) RequireOwnAgent(principal auth.Principal, id string) (*AgentRow, error) {
	return call(e, func() *AgentRow { return e.requireOwnAgent(principal, id) })
}

// AgentInfo gives the listed form of an agent row.
func (e *Engine) AgentInfo(row *AgentRow) AgentInfo {
	labels, _ := parseJSONOr(row.Labels, &jsonx.Object{}).(*jsonx.Object)
	if labels == nil {
		labels = &jsonx.Object{}
	}
	return AgentInfo{
		ID:         row.ID,
		Name:       row.Name,
		Kind:       row.Kind,
		Host:       row.Host,
		Cwd:        row.Cwd,
		Role:       row.Role,
		Labels:     labels,
		Parent:     row.Parent,
		Topics:     parseJSONOr(row.Topics, []any{}),
		Online:     row.Online == 1,
		State:      row.State,
		Activity:   row.Activity,
		Status:     AgentStatus{Text: row.StatusText, At: row.StatusAt},
		LastSeenAt: row.LastSeenAt,
	}
}

func parseJSONOr(text string, fallback any) any {
	if text == "" {
		return fallback
	}
	return mustParse(text)
}

// ListAgents lists the online agents, and with includeOffline also those offline for less than OfflineListMS.
func (e *Engine) ListAgents(includeOffline bool) ([]AgentInfo, error) {
	return call(e, func() []AgentInfo {
		var rows []*AgentRow
		if includeOffline {
			since := ISO(e.now() - OfflineListMS)
			rows = e.agents("SELECT "+agentCols+" FROM agents WHERE online = 1 OR offline_at IS NULL OR offline_at >= ? ORDER BY name, id", since)
		} else {
			rows = e.agents("SELECT " + agentCols + " FROM agents WHERE online = 1 ORDER BY name, id")
		}
		out := make([]AgentInfo, 0, len(rows))
		for _, row := range rows {
			out = append(out, e.AgentInfo(row))
		}
		return out
	})
}

// OnlineCount is the number of online agents.
func (e *Engine) OnlineCount() (int64, error) {
	return call(e, func() int64 { return e.count("SELECT count(*) AS n FROM agents WHERE online = 1") })
}

// EnsureBuiltin makes or updates a built-in agent row. It is always online.
func (e *Engine) EnsureBuiltin(id, name string) error {
	return e.do(func() {
		at := e.iso()
		e.run(`INSERT INTO agents (id, kind, host, name, online, registered_at, last_seen_at)
			 VALUES (?, 'builtin', NULL, ?, 1, ?, ?)
			 ON CONFLICT (id) DO UPDATE SET kind = 'builtin', host = NULL, name = excluded.name, online = 1, offline_at = NULL`,
			id, name, at, at)
	})
}

// EnsureExternal makes or renames an external agent row. A new row starts offline: the gateway sets it online
// when the agent answers.
func (e *Engine) EnsureExternal(id, name string) error {
	return e.do(func() {
		if !agentIDPattern.MatchString(id) {
			throw(ReasonInvalid, "external agent id %s has invalid characters", id)
		}
		e.tx(func() {
			existing := e.agent(id)
			if existing != nil && existing.Kind != KindExternal {
				throw(ReasonConflict, "agent id %s is in use by a %s agent", id, existing.Kind)
			}
			if existing != nil {
				e.run("UPDATE agents SET name = ? WHERE id = ?", name, id)
				return
			}
			at := e.iso()
			e.run(`INSERT INTO agents (id, kind, host, name, online, registered_at, offline_at)
				 VALUES (?, 'external', NULL, ?, 0, ?, ?)`, id, name, at, at)
		})
	})
}

// UpgradeDetails are the extra fields of the 426 error body.
type UpgradeDetails struct {
	Expected  int   `json:"expected"`
	Supported []int `json:"supported"`
}

// Register binds an agent to the host principal. A field that the body leaves out keeps its value.
func (e *Engine) Register(principal auth.Principal, body *jsonx.Object) (RegisterResult, error) {
	return call(e, func() RegisterResult { return e.register(principal, body) })
}

func (e *Engine) register(principal auth.Principal, body *jsonx.Object) RegisterResult {
	if principal.Kind != auth.KindHost {
		throw(ReasonForbidden, "only host principals may register agents")
	}
	protocol, ok := body.Value("protocol").(float64)
	if !ok || !slices.ContainsFunc(SupportedProtocols, func(p int) bool { return float64(p) == protocol }) {
		panic(newError(ReasonUpgrade, UpgradeDetails{Expected: HubProtocol, Supported: SupportedProtocols},
			"hub protocol %s required", protocolList(SupportedProtocols)))
	}
	id := requireString(body.Value("agent"), "agent", 128)
	if !agentIDPattern.MatchString(id) {
		throw(ReasonInvalid, "agent id has invalid characters")
	}
	givenName := optionalString(body.Value("name"), "name", 128)
	givenCwd := optionalString(body.Value("cwd"), "cwd", 4096)
	givenRole := optionalString(body.Value("role"), "role", 256)
	givenParent := optionalString(body.Value("parent"), "parent", 128)
	var givenLabels, givenTopics *string
	if v := body.Value("labels"); v != nil {
		givenLabels = strPtr(stringify(labelMap(v)))
	}
	if v := body.Value("topics"); v != nil {
		givenTopics = strPtr(stringify(stringList(v, "topics")))
	}
	var givenEphemeral *int64
	if v := body.Value("ephemeral"); v != nil {
		n := int64(0)
		if v == true {
			n = 1
		}
		givenEphemeral = &n
	}
	return txValue(e, func() RegisterResult {
		existing := e.agent(id)
		if existing != nil && (existing.Kind != KindSession || existing.Host == nil || *existing.Host != principal.Name) {
			throw(ReasonForbidden, "agent %s is bound to another host", id)
		}
		if existing == nil {
			existing = &AgentRow{Labels: "{}", Topics: "[]"}
		} else {
			copied := *existing
			existing = &copied
		}
		name := firstOf(givenName, existingName(existing), strPtr(DefaultAgentName(principal.Name, id)))
		cwd := firstOf(givenCwd, existing.Cwd)
		role := firstOf(givenRole, existing.Role)
		parent := firstOf(givenParent, existing.Parent)
		fallback := ""
		if protocol == LegacyProtocol {
			fallback = LegacyHarness
		}
		labels := withHarness(deref(firstOf(givenLabels, &existing.Labels)), fallback)
		topics := deref(firstOf(givenTopics, &existing.Topics))
		ephemeral := existing.Ephemeral
		if givenEphemeral != nil {
			ephemeral = *givenEphemeral
		}
		at := e.iso()
		if existing.ID != "" {
			e.run(`UPDATE agents SET name = ?, cwd = ?, role = ?, parent = ?, labels = ?, topics = ?, ephemeral = ?, last_seen_at = ?
				 WHERE id = ?`, *name, cwd, role, parent, labels, topics, ephemeral, at, id)
		} else {
			e.run(`INSERT INTO agents (id, kind, host, name, cwd, role, parent, labels, topics, ephemeral, online, registered_at, last_seen_at, offline_at)
				 VALUES (?, 'session', ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)`,
				id, principal.Name, *name, cwd, role, parent, labels, topics, ephemeral, at, at, at)
		}
		return RegisterResult{Agent: id, Name: *name, Host: principal.Name}
	})
}

// protocolList names the protocols as "2 or 3", or as "1, 2, or 3" for three or more.
func protocolList(protocols []int) string {
	names := make([]string, len(protocols))
	for i, protocol := range protocols {
		names[i] = strconv.Itoa(protocol)
	}
	switch len(names) {
	case 1:
		return names[0]
	case 2:
		return names[0] + " or " + names[1]
	}
	return strings.Join(names[:len(names)-1], ", ") + ", or " + names[len(names)-1]
}

// harnessOf is the harness label of stored labels, or "" when they have none.
func harnessOf(labels string) string {
	obj, _ := mustParse(labels).(*jsonx.Object)
	name, _ := obj.String(HarnessLabel)
	return name
}

// withHarness checks that session labels name the harness. Without it, they get fallback, and an empty fallback
// refuses them.
func withHarness(labels, fallback string) string {
	if harnessOf(labels) != "" {
		return labels
	}
	if fallback == "" {
		throw(ReasonInvalid, "labels.%s is required with hub protocol %d", HarnessLabel, HubProtocol)
	}
	obj, _ := mustParse(labels).(*jsonx.Object)
	if obj == nil {
		obj = &jsonx.Object{}
	}
	return stringify(obj.Set(HarnessLabel, fallback))
}

func existingName(row *AgentRow) *string {
	if row.ID == "" {
		return nil
	}
	return &row.Name
}

// firstOf is the ?? chain: the first non-nil value.
func firstOf(values ...*string) *string {
	for _, v := range values {
		if v != nil {
			return v
		}
	}
	return nil
}

// AddChild makes an offline row for a child before its process registers, so the parent can send it the
// first task.
func (e *Engine) AddChild(principal auth.Principal, parentID string, body any) (RegisterResult, error) {
	return call(e, func() RegisterResult {
		obj := objectBody(body, "body must be an object")
		id := requireString(obj.Value("id"), "id", 128)
		if !agentIDPattern.MatchString(id) {
			throw(ReasonInvalid, "agent id has invalid characters")
		}
		name := deref(firstOf(optionalString(obj.Value("name"), "name", 128), strPtr(DefaultAgentName(principal.Name, id))))
		cwd := optionalString(obj.Value("cwd"), "cwd", 4096)
		role := optionalString(obj.Value("role"), "role", 256)
		givenLabels := stringify(labelMap(obj.Value("labels")))
		return txValue(e, func() RegisterResult {
			// A child runs in the harness of its parent unless the body names another one.
			fallback := LegacyHarness
			if parent := e.agent(parentID); parent != nil && harnessOf(parent.Labels) != "" {
				fallback = harnessOf(parent.Labels)
			}
			labels := withHarness(givenLabels, fallback)
			existing := e.agent(id)
			at := e.iso()
			if existing != nil {
				if existing.Kind != KindSession || existing.Host == nil || *existing.Host != principal.Name || existing.Parent == nil || *existing.Parent != parentID {
					throw(ReasonConflict, "agent %s exists with another parent or host", id)
				}
				e.run("UPDATE agents SET name = ?, cwd = ?, role = ?, labels = ? WHERE id = ?", name, cwd, role, labels, id)
			} else {
				e.run(`INSERT INTO agents (id, kind, host, name, cwd, role, parent, labels, online, registered_at, offline_at)
					 VALUES (?, 'session', ?, ?, ?, ?, ?, ?, 0, ?, ?)`, id, principal.Name, name, cwd, role, parentID, labels, at, at)
			}
			return RegisterResult{Agent: id, Name: name, Host: principal.Name}
		})
	})
}

// ResolveRef resolves an id, or "@name" to a built-in or external agent with that name, else to the one online
// session agent with it. More than one online session agent with the name gives Ambiguous. nil means no match.
func (e *Engine) ResolveRef(ref string) (*RefResult, error) {
	return call(e, func() *RefResult {
		if len(ref) == 0 || ref[0] != '@' {
			if row := e.agent(ref); row != nil {
				return &RefResult{Agent: row}
			}
			return nil
		}
		name := ref[1:]
		if fixed := e.agents("SELECT "+agentCols+" FROM agents WHERE name = ? AND kind IN ('builtin', 'external') ORDER BY id LIMIT 1", name); len(fixed) > 0 {
			return &RefResult{Agent: fixed[0]}
		}
		online := e.agents("SELECT "+agentCols+" FROM agents WHERE name = ? AND kind = 'session' AND online = 1 ORDER BY last_seen_at DESC, id", name)
		switch len(online) {
		case 0:
			return nil
		case 1:
			return &RefResult{Agent: online[0]}
		}
		ids := make([]string, len(online))
		for i, row := range online {
			ids[i] = row.ID
		}
		return &RefResult{Ambiguous: ids}
	})
}

func (e *Engine) touch(id string) {
	e.run("UPDATE agents SET last_seen_at = ? WHERE id = ?", e.iso(), id)
}

// Touch sets last_seen_at.
func (e *Engine) Touch(id string) error { return e.do(func() { e.touch(id) }) }

// SetOnline sets the presence of an agent and tells its parent, children, and peers about a change.
func (e *Engine) SetOnline(id string, online bool) error {
	return e.do(func() { e.setOnline(id, online) })
}

func (e *Engine) setOnline(id string, online bool) {
	e.tx(func() {
		row := e.agent(id)
		if row == nil || row.Kind == KindBuiltin {
			return
		}
		at := e.iso()
		if online {
			e.run("UPDATE agents SET online = 1, last_seen_at = ?, offline_at = NULL WHERE id = ?", at, id)
		} else {
			e.run("UPDATE agents SET online = 0, offline_at = ?, state = 'idle', activity = NULL WHERE id = ?", at, id)
		}
		if (row.Online == 1) != online {
			e.emitPresence(row, online)
		}
	})
}

// PresenceEvent is the payload of a presence link event.
type PresenceEvent struct {
	Agent    string `json:"agent"`
	Name     string `json:"name"`
	Online   bool   `json:"online"`
	Relation string `json:"relation"`
}

// emitPresence tells the parent and children first, then every agent with an open question or a live task with
// this agent, either way.
func (e *Engine) emitPresence(row *AgentRow, online bool) {
	var order []string
	relation := map[string]string{}
	set := func(id, rel string) {
		if _, ok := relation[id]; !ok {
			order = append(order, id)
		}
		relation[id] = rel
	}
	if row.Parent != nil && *row.Parent != "" {
		set(*row.Parent, "child")
	}
	e.each("SELECT id FROM agents WHERE parent = ?", args(row.ID), func(scan func(...any)) {
		var id string
		scan(&id)
		set(id, "parent")
	})
	e.each(`SELECT recipient_agent AS id FROM expect_reply WHERE sender_agent = ? AND settled_at IS NULL
			 UNION SELECT sender_agent FROM expect_reply WHERE recipient_agent = ? AND settled_at IS NULL
			 UNION SELECT agent_id FROM tasks WHERE sender_agent = ? AND state NOT IN (`+terminalSQL+`)
			 UNION SELECT sender_agent FROM tasks WHERE agent_id = ? AND sender_agent IS NOT NULL AND state NOT IN (`+terminalSQL+`)`,
		args(row.ID, row.ID, row.ID, row.ID), func(scan func(...any)) {
			var id string
			scan(&id)
			if _, ok := relation[id]; !ok {
				set(id, "peer")
			}
		})
	for _, target := range order {
		if target == row.ID {
			continue
		}
		e.emit(target, "presence", PresenceEvent{Agent: row.ID, Name: row.Name, Online: online, Relation: relation[target]})
	}
}

// SetState stores busy or idle and the activity line.
func (e *Engine) SetState(id string, state, activity any) error {
	return e.do(func() {
		if state != "busy" && state != "idle" {
			throw(ReasonInvalid, "state must be busy or idle")
		}
		text := optionalString(activity, "activity", 256)
		e.run("UPDATE agents SET state = ?, activity = ?, last_seen_at = ? WHERE id = ?", state, text, e.iso(), id)
	})
}

// SetTopics adds and removes topics and returns the new list.
func (e *Engine) SetTopics(id string, subscribe, unsubscribe any) ([]string, error) {
	return call(e, func() []string {
		add := stringList(subscribe, "subscribe")
		remove := map[string]bool{}
		for _, topic := range stringList(unsubscribe, "unsubscribe") {
			remove[topic] = true
		}
		return txValue(e, func() []string {
			row := e.requireAgent(id)
			var current []string
			if list, ok := parseJSONOr(row.Topics, []any{}).([]any); ok {
				for _, item := range list {
					if s, ok := item.(string); ok {
						current = append(current, s)
					}
				}
			}
			seen := map[string]bool{}
			topics := []string{}
			for _, topic := range append(current, add...) {
				if seen[topic] {
					continue
				}
				seen[topic] = true
				if !remove[topic] {
					topics = append(topics, topic)
				}
			}
			e.run("UPDATE agents SET topics = ? WHERE id = ?", stringify(topics), id)
			return topics
		})
	})
}

// ControlFrom names the sender of a control event.
type ControlFrom struct {
	Agent string `json:"agent"`
	Name  string `json:"name"`
}

// ControlEvent is the payload of a control link event.
type ControlEvent struct {
	Action string      `json:"action"`
	From   ControlFrom `json:"from"`
}

// Control asks an online child to close.
func (e *Engine) Control(fromID string, target, action any) error {
	return e.do(func() {
		targetID := requireString(target, "target", 128)
		if action != "close" {
			throw(ReasonInvalid, "action must be close")
		}
		e.tx(func() {
			from := e.requireAgent(fromID)
			row := e.agent(targetID)
			if row == nil {
				throw(ReasonNotFound, "unknown agent %s", targetID)
			}
			if row.Parent == nil || *row.Parent != fromID || row.Kind != KindSession {
				throw(ReasonForbidden, "%s is not a child of %s", targetID, fromID)
			}
			if row.Online != 1 {
				throw(ReasonConflict, "offline")
			}
			e.emit(row.ID, "control", ControlEvent{Action: "close", From: ControlFrom{Agent: from.ID, Name: from.Name}})
		})
	})
}

// SetStatus stores the status text and returns it cleaned. Every working task of the agent shows it in
// status.message.
func (e *Engine) SetStatus(id string, text any) (string, error) {
	return call(e, func() string {
		s, ok := text.(string)
		if !ok {
			throw(ReasonInvalid, "text must be a string")
		}
		clean := CleanStatusText(s)
		return txValue(e, func() string {
			row := e.requireAgent(id)
			if row.StatusText == clean {
				return clean
			}
			at := e.iso()
			e.run("UPDATE agents SET status_text = ?, status_at = ?, last_seen_at = ? WHERE id = ?", clean, at, at, id)
			for _, task := range e.tasks("SELECT "+taskCols+" FROM tasks WHERE agent_id = ? AND state = 'TASK_STATE_WORKING' ORDER BY created_at", id) {
				if clean == "" {
					if current := parseObject(task.StatusMessage); current != nil && isStatusTextMessage(current) {
						e.writeStatus(task, StateWorking, nil)
					}
				} else {
					e.writeStatus(task, StateWorking, e.statusTextMessage(task, clean))
				}
			}
			return clean
		})
	})
}

func (e *Engine) statusTextMessage(task *taskRow, text string) *jsonx.Object {
	return jsonx.NewObject(
		"messageId", e.NewID(),
		"contextId", task.ContextID,
		"taskId", task.ID,
		"role", "ROLE_AGENT",
		"parts", []any{jsonx.NewObject("text", text)},
		// Protocol 2 clients read status messages too, so the mark keeps the URI that they know.
		"metadata", jsonx.NewObject(LegacyBusExtensionURI, jsonx.NewObject("statusText", true)),
	)
}

func isStatusTextMessage(message *jsonx.Object) bool {
	metadata, _ := message.Object("metadata")
	_, entry := BusEntry(metadata)
	meta, _ := entry.(*jsonx.Object)
	return meta.Value("statusText") == true
}
