package engine

import (
	"errors"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/orswn/a2a-hub/internal/auth"
	"github.com/orswn/a2a-hub/internal/jsonx"
)

// ---------- register and host binding ----------

func TestRegisterBindsAnAgentToTheFirstHostAndRefusesAnotherHost(t *testing.T) {
	h := setup(t)
	equalJSON(t, "register", h.register("a1", "name", "worker"), RegisterResult{Agent: "a1", Name: "worker", Host: "mars"})
	if r := got(h.registerAs(venus, "a1")).reason(t); r != ReasonForbidden {
		t.Errorf("venus = %q", r)
	}
	if name := h.register("a1", "name", "renamed").Name; name != "renamed" {
		t.Errorf("name = %q", name)
	}
}

func TestRegisterRefusesAPeerPrincipalAndAWrongProtocol(t *testing.T) {
	h := setup(t)
	if r := got(h.registerAs(hermes, "a1")).reason(t); r != ReasonForbidden {
		t.Errorf("peer = %q", r)
	}
	_, err := h.engine.Register(mars, obj("agent", "a1", "protocol", 1))
	var hubErr *Error
	if !errors.As(err, &hubErr) || hubErr.Reason != ReasonUpgrade || hubErr.Status != 426 || jsonOf(t, hubErr.Details) != `{"expected":3,"supported":[2,3]}` ||
		hubErr.Error() != "hub protocol 2 or 3 required" {
		t.Errorf("protocol 1 = %v", err)
	}
	for _, protocol := range []any{2.5, 4, "3", nil} {
		if r := got(h.engine.Register(mars, obj("agent", "a1", "protocol", protocol))).reason(t); r != ReasonUpgrade {
			t.Errorf("protocol %v = %q", protocol, r)
		}
	}
}

func TestProtocolListNamesEachProtocol(t *testing.T) {
	cases := map[string][]int{"3": {3}, "2 or 3": {2, 3}, "1, 2, or 3": {1, 2, 3}}
	for want, protocols := range cases {
		if got := protocolList(protocols); got != want {
			t.Errorf("%v = %q, want %q", protocols, got, want)
		}
	}
}

func labelsOf(t *testing.T, h *testEnv, id string) string {
	t.Helper()
	return got(h.engine.RequireAgent(id)).ok(t).Labels
}

func TestRegisterAcceptsProtocol2AndGivesItTheLegacyHarness(t *testing.T) {
	h := setup(t)
	got(h.engine.Register(mars, obj("agent", "old", "protocol", LegacyProtocol, "labels", obj("team", "core")))).ok(t)
	row := got(h.engine.RequireAgent("old")).ok(t)
	if row.Kind != KindSession || row.Labels != `{"team":"core","harness":"`+LegacyHarness+`"}` {
		t.Errorf("kind %s, labels %s", row.Kind, row.Labels)
	}
	got(h.engine.Register(mars, obj("agent", "old", "protocol", LegacyProtocol))).ok(t)
	if labels := labelsOf(t, h, "old"); labels != `{"team":"core","harness":"`+LegacyHarness+`"}` {
		t.Errorf("labels after a register without labels = %s", labels)
	}
	got(h.engine.Register(mars, obj("agent", "named", "protocol", LegacyProtocol, "labels", obj("harness", "codex")))).ok(t)
	if labels := labelsOf(t, h, "named"); labels != `{"harness":"codex"}` {
		t.Errorf("labels with a harness = %s", labels)
	}
}

func TestRegisterWithProtocol3NeedsTheHarnessLabel(t *testing.T) {
	h := setup(t)
	for _, labels := range []any{nil, obj("team", "core"), obj("harness", "")} {
		body := obj("agent", "new", "protocol", HubProtocol)
		if labels != nil {
			body.Set("labels", labels)
		}
		_, err := h.engine.Register(mars, body)
		if reason(t, err) != ReasonInvalid || err.Error() != "labels.harness is required with hub protocol 3" {
			t.Errorf("labels %s: %v", jsonx.Stringify(labels), err)
		}
	}
	if _, err := h.engine.RequireAgent("new"); reason(t, err) != ReasonNotFound {
		t.Errorf("a refused register wrote the agent: %v", err)
	}
	got(h.engine.Register(mars, obj("agent", "new", "protocol", HubProtocol, "labels", obj("harness", "claude-code", "team", "core")))).ok(t)
	got(h.engine.Register(mars, obj("agent", "new", "protocol", HubProtocol))).ok(t)
	if labels := labelsOf(t, h, "new"); labels != `{"harness":"claude-code","team":"core"}` {
		t.Errorf("labels after a register without labels = %s", labels)
	}
	if r := got(h.engine.Register(mars, obj("agent", "new", "protocol", HubProtocol, "labels", obj("team", "x")))).reason(t); r != ReasonInvalid {
		t.Errorf("new labels without the harness = %q", r)
	}
}

func TestAddChildGivesAChildWithoutAHarnessTheLegacyHarness(t *testing.T) {
	h := setup(t)
	h.register("parent")
	got(h.engine.AddChild(mars, "parent", obj("id", "kid"))).ok(t)
	got(h.engine.AddChild(mars, "parent", obj("id", "coder", "labels", obj("harness", "codex")))).ok(t)
	if labels := labelsOf(t, h, "kid"); labels != `{"harness":"`+LegacyHarness+`"}` {
		t.Errorf("kid labels = %s", labels)
	}
	if labels := labelsOf(t, h, "coder"); labels != `{"harness":"codex"}` {
		t.Errorf("coder labels = %s", labels)
	}
	if kind := got(h.engine.RequireAgent("kid")).ok(t).Kind; kind != KindSession {
		t.Errorf("kind = %s", kind)
	}
}

func TestParseBusMetadataReadsBothURIsAndPrefersTheCurrentOne(t *testing.T) {
	cases := []struct {
		metadata *jsonx.Object
		kind     string
	}{
		{obj(BusExtensionURI, obj("kind", "note")), "note"},
		{obj(LegacyBusExtensionURI, obj("kind", "answer")), "answer"},
		{obj(LegacyBusExtensionURI, obj("kind", "answer"), BusExtensionURI, obj("kind", "note")), "note"},
		{obj("other", obj("kind", "note")), "task"},
	}
	for _, c := range cases {
		meta, err := ParseBusMetadata(c.metadata)
		if err != nil || meta.Kind != c.kind {
			t.Errorf("%s: kind %q, %v; want %q", jsonx.Stringify(c.metadata), meta.Kind, err, c.kind)
		}
	}
}

func TestRegisterDerivesTheDefaultNameFromTheHostAndAHashOfTheID(t *testing.T) {
	h := setup(t)
	name := h.register("agent-xyz").Name
	if name != DefaultAgentName("mars", "agent-xyz") || !regexp.MustCompile(`^mars-[0-9a-f]{6}$`).MatchString(name) {
		t.Errorf("name = %q", name)
	}
	// Node: createHash("sha256").update("agent-xyz").digest("hex").slice(0, 6)
	if name != "mars-a49a9b" {
		t.Errorf("default name = %q", name)
	}
}

func TestRequireOwnAgentAllowsAHostToActOnlyForItsOwnAgents(t *testing.T) {
	h := setup(t)
	h.register("a1")
	if row := got(h.engine.RequireOwnAgent(mars, "a1")).ok(t); row.ID != "a1" {
		t.Errorf("id = %q", row.ID)
	}
	for _, c := range []struct {
		principal auth.Principal
		id        string
	}{{venus, "a1"}, {hermes, "a1"}, {mars, "nope"}} {
		if r := got(h.engine.RequireOwnAgent(c.principal, c.id)).reason(t); r != ReasonForbidden {
			t.Errorf("%s %s = %q", c.principal.Name, c.id, r)
		}
	}
}

// ---------- alias resolution ----------

func resolved(t *testing.T, h *testEnv, ref string) string {
	t.Helper()
	r := got(h.engine.ResolveRef(ref)).ok(t)
	switch {
	case r == nil:
		return ""
	case r.Agent != nil:
		return r.Agent.ID
	}
	return "ambiguous:" + strings.Join(r.Ambiguous, ",")
}

func TestResolveRefResolvesIDsBuiltinNamesAndTheOneOnlinePiAgent(t *testing.T) {
	h := setup(t)
	ok(t, h.engine.EnsureBuiltin("tck", "tck"))
	h.register("a1", "name", "worker")
	if got := resolved(t, h, "@worker"); got != "" {
		t.Errorf("@worker offline = %q", got)
	}
	ok(t, h.engine.SetOnline("a1", true))
	for ref, want := range map[string]string{"@worker": "a1", "@tck": "tck", "a1": "a1", "missing": ""} {
		if got := resolved(t, h, ref); got != want {
			t.Errorf("%s = %q, want %q", ref, got, want)
		}
	}
}

func TestResolveRefPrefersABuiltinAgentOverAPiAgentWithTheSameName(t *testing.T) {
	h := setup(t)
	h.register("a1", "name", "echo")
	ok(t, h.engine.SetOnline("a1", true))
	ok(t, h.engine.EnsureBuiltin("echo", "echo"))
	if got := resolved(t, h, "@echo"); got != "echo" {
		t.Errorf("@echo = %q", got)
	}
}

func TestResolveRefReportsEveryOnlineMatchWhenTwoPiAgentsShareAName(t *testing.T) {
	h := setup(t)
	h.register("a1", "name", "worker")
	h.register("a2", "name", "worker")
	ok(t, h.engine.SetOnline("a1", true))
	h.advance(10)
	ok(t, h.engine.SetOnline("a2", true))
	if got := resolved(t, h, "@worker"); got != "ambiguous:a2,a1" {
		t.Errorf("@worker = %q", got)
	}
	ok(t, h.engine.SetOnline("a2", false))
	if got := resolved(t, h, "@worker"); got != "a1" {
		t.Errorf("@worker = %q", got)
	}
}

// ---------- task lifecycle ----------

func historyIDs(task *Task) []string {
	var ids []string
	for _, m := range task.History {
		id, _ := m.String("messageId")
		ids = append(ids, id)
	}
	return ids
}

func TestSendCreatesASubmittedTaskWithTheMessageAsHistoryAndALinkEvent(t *testing.T) {
	h := setup(t)
	h.register("a1")
	result := h.send("a1", fromHermes, message("hi"))
	if !result.Created || result.Task.Status.State != StateSubmitted || result.Task.ContextID == "" {
		t.Errorf("result = %+v", result)
	}
	if ids := historyIDs(result.Task); !slices.Equal(ids, []string{"m-hi"}) {
		t.Errorf("history = %q", ids)
	}
	events := h.eventsOf("message")
	if len(events) == 0 || events[0].agent != "a1" {
		t.Fatalf("events = %+v", events)
	}
	matchJSON(t, "payload", payload(t, events[0].event), obj(
		"messageId", "m-hi", "taskId", result.Task.ID, "kind", "task", "wake", "now", "text", "hi",
		"taskState", "submitted", "from", obj("name", "hermes", "kind", "peer"),
	))
	inbox := got(h.engine.Inbox("a1", true, 10)).ok(t)
	if len(inbox) != 1 || inbox[0].MessageID != "m-hi" {
		t.Errorf("inbox = %+v", inbox)
	}
}

func TestSendStoresANoteWithoutATaskAndReturnsAStoredMessage(t *testing.T) {
	h := setup(t)
	h.register("a1")
	result := h.send("a1", fromHermes, withMeta(message("fyi", "contextId", "ctx"), "kind", "note", "wake", "never"))
	if result.Task != nil {
		t.Errorf("task = %+v", result.Task)
	}
	matchJSON(t, "message", result.Message, obj("role", "ROLE_AGENT", "contextId", "ctx", "parts", []any{obj("text", "stored")}))
	matchJSON(t, "payload", payload(t, h.linkEvents()[0].event), obj("kind", "note", "wake", "never", "contextId", "ctx"))
}

func TestTaskMovesThroughWorkingAndInputRequiredToCompletedAndStaysFinal(t *testing.T) {
	h := setup(t)
	h.register("a1")
	id := h.taskID("a1", fromHermes, message("go"))
	var states []State
	h.engine.OnTask(id, func(event TaskEvent) { states = append(states, event.State) })
	h.linkUpdate("a1", id, "state", "working")
	h.linkUpdate("a1", id, "state", "input-required", "text", "which file?")
	followUp := h.send("a1", fromHermes, message("main.ts", "taskId", id))
	if followUp.Task.Status.State != StateWorking {
		t.Errorf("follow-up state = %s", followUp.Task.Status.State)
	}
	done := h.linkUpdate("a1", id, "state", "completed", "text", "done", "artifact", obj("name", "out", "text", "x"))
	if done.Status.State != StateCompleted {
		t.Errorf("state = %s", done.Status.State)
	}
	equalJSON(t, "status parts", done.Status.Message.Value("parts"), []any{obj("text", "done")})
	matchJSON(t, "artifact", done.Artifacts[0], obj("name", "out", "parts", []any{obj("text", "x")}))
	var roles []string
	for _, m := range done.History {
		role, _ := m.String("role")
		roles = append(roles, role)
	}
	if !slices.Equal(roles, []string{"ROLE_USER", "ROLE_AGENT", "ROLE_USER", "ROLE_AGENT"}) {
		t.Errorf("roles = %q", roles)
	}
	// The artifact update comes before the final status update.
	if want := []State{StateWorking, StateInputRequired, StateWorking, StateWorking, StateCompleted}; !slices.Equal(states, want) {
		t.Errorf("states = %q", states)
	}
	if r := got(h.engine.LinkUpdate("a1", id, obj("state", "working"))).reason(t); r != ReasonTaskTerminal {
		t.Errorf("update after completed = %q", r)
	}
	if r := got(h.engine.Cancel("a1", fromHermes, id)).reason(t); r != ReasonNotCancelable {
		t.Errorf("cancel after completed = %q", r)
	}
	if r := got(h.engine.Send(SendInput{AgentID: "a1", Caller: fromHermes, Message: message("more", "taskId", id)})).reason(t); r != ReasonTaskTerminal {
		t.Errorf("send after completed = %q", r)
	}
}

func TestLinkUpdateTreatsFailedAndRejectedAsFinalStatesToo(t *testing.T) {
	h := setup(t)
	h.register("a1")
	for state, want := range map[string]State{"failed": StateFailed, "rejected": StateRejected} {
		id := h.taskID("a1", fromHermes, message(state))
		if got := h.linkUpdate("a1", id, "state", state).Status.State; got != want {
			t.Errorf("%s = %s", state, got)
		}
		if r := got(h.engine.LinkUpdate("a1", id, obj("state", "working"))).reason(t); r != ReasonTaskTerminal {
			t.Errorf("update after %s = %q", state, r)
		}
	}
}

func TestSendChecksFollowUpRulesUnknownTaskOtherAgentAndContextMismatch(t *testing.T) {
	h := setup(t)
	h.register("a1")
	h.register("a2")
	task := h.send("a1", fromHermes, message("go")).Task
	send := func(agentID string, extra ...any) Reason {
		return got(h.engine.Send(SendInput{AgentID: agentID, Caller: fromHermes, Message: message("x", extra...)})).reason(t)
	}
	for _, c := range []struct {
		r     Reason
		agent string
		extra []any
	}{
		{ReasonTaskNotFound, "a1", []any{"taskId", "missing"}},
		{ReasonTaskNotFound, "a2", []any{"taskId", task.ID}},
		{ReasonInvalid, "a1", []any{"taskId", task.ID, "contextId", "other"}},
		{"", "a1", []any{"taskId", task.ID, "contextId", task.ContextID}},
	} {
		if r := send(c.agent, c.extra...); r != c.r {
			t.Errorf("send(%s, %v) = %q, want %q", c.agent, c.extra, r, c.r)
		}
	}
}

func TestSendDoesNotChangeStateOnAFollowUpToAWorkingTask(t *testing.T) {
	h := setup(t)
	h.register("a1")
	id := h.taskID("a1", fromHermes, message("go"))
	h.linkUpdate("a1", id, "state", "working")
	var states []State
	h.engine.OnTask(id, func(event TaskEvent) { states = append(states, event.State) })
	result := h.send("a1", fromHermes, message("also", "taskId", id))
	if result.Task.Status.State != StateWorking || len(result.Task.History) != 2 || len(states) != 0 {
		t.Errorf("state = %s, history = %d, events = %q", result.Task.Status.State, len(result.Task.History), states)
	}
}

func TestCancelTellsTheRecipientAndTheSenderAgentAndRefusesASecondCancel(t *testing.T) {
	h := setup(t)
	h.register("a1")
	h.register("sender")
	caller := Caller{Principal: mars, Agent: "sender"}
	id := h.taskID("a1", caller, message("go"))
	canceled := got(h.engine.Cancel("a1", caller, id)).ok(t)
	if canceled.Status.State != StateCanceled {
		t.Errorf("state = %s", canceled.Status.State)
	}
	cancels := h.eventsOf("cancel")
	if len(cancels) != 1 || cancels[0].agent != "a1" || string(cancels[0].event.Payload) != `{"taskId":"`+id+`"}` {
		t.Errorf("cancel events = %+v", cancels)
	}
	updates := h.eventsOf("update")
	if len(updates) == 0 || updates[0].agent != "sender" {
		t.Fatalf("update events = %+v", updates)
	}
	matchJSON(t, "update", payload(t, updates[0].event), obj("taskId", id, "state", "canceled", "from", obj("agent", "a1")))
	if r := got(h.engine.Cancel("a1", caller, id)).reason(t); r != ReasonNotCancelable {
		t.Errorf("second cancel = %q", r)
	}
}

func TestApplyChangeSendsAnUpdateLinkEventToTheSenderAgentOnEachChange(t *testing.T) {
	h := setup(t)
	h.register("a1", "name", "worker")
	h.register("sender")
	caller := Caller{Principal: mars, Agent: "sender"}
	id := h.taskID("a1", caller, message("go"))
	h.linkUpdate("a1", id, "state", "completed", "text", "ok", "artifact", obj("name", "r", "text", "body"))
	var event *LinkEvent
	for _, e := range h.linkEvents() {
		if e.agent == "sender" {
			event = &e.event
			break
		}
	}
	if event == nil {
		t.Fatal("no event for the sender")
	}
	p := payload(t, *event)
	matchJSON(t, "update", p, obj(
		"taskId", id, "contextId", anyString, "state", "completed", "messageId", anyString, "kind", "result",
		"text", "ok", "artifacts", []any{obj("name", "r", "text", "body")}, "from", obj("agent", "a1", "name", "worker"),
	))
	if p.Len() != 8 {
		t.Errorf("payload has extra keys: %s", event.Payload)
	}
}

func TestApplyChangeKeepsTheEventsAndTheRowsUnchangedWhenAChangeFailsInTheMiddle(t *testing.T) {
	h := setup(t)
	h.register("a1")
	id := h.taskID("a1", fromHermes, message("go"))
	before := len(h.linkEvents())
	_, err := h.engine.ApplyChange(id, TaskChange{
		State:     StateWorking,
		Artifacts: []ArtifactChange{{Artifact: obj("artifactId", "", "parts", []any{})}},
	})
	if reason(t, err) != ReasonInvalid {
		t.Errorf("err = %v", err)
	}
	if state := got(h.engine.ReadTask(id)).ok(t).Status.State; state != StateSubmitted {
		t.Errorf("state = %s", state)
	}
	if after := len(h.linkEvents()); after != before {
		t.Errorf("events %d -> %d", before, after)
	}
}

// ---------- context-level follow-ups ----------

var asker = Caller{Principal: mars, Agent: "asker"}

func (h *testEnv) ask(agentID string, caller Caller, text string, contextID ...string) *SendResult {
	h.t.Helper()
	ctx := "ctx-1"
	if len(contextID) > 0 {
		ctx = contextID[0]
	}
	return h.send(agentID, caller, message(text, "contextId", ctx))
}

func TestSendContinuesTheOneInterruptedTaskWhenAPeerSendsAgainInItsContext(t *testing.T) {
	h := setup(t)
	h.register("a1")
	id := h.ask("a1", fromHermes, "plan").Task.ID
	h.linkUpdate("a1", id, "state", "input-required", "text", "which day?")
	next := h.ask("a1", fromHermes, "monday")
	if next.Created || next.Task.ID != id || next.Task.Status.State != StateWorking {
		t.Errorf("next = created %v, id %s, state %s", next.Created, next.Task.ID, next.Task.Status.State)
	}
	if ids := historyIDs(next.Task); len(ids) != 3 || ids[0] != "m-plan" || ids[2] != "m-monday" {
		t.Errorf("history = %q", ids)
	}
	events := h.linkEvents()
	matchJSON(t, "last event", payload(t, events[len(events)-1].event), obj("taskId", id, "messageId", "m-monday", "taskState", "working"))
}

func TestSendStartsANewTaskWhenTheContextHasTwoInterruptedTasksNoneOrAnotherSender(t *testing.T) {
	h := setup(t)
	h.register("a1")
	first := h.ask("a1", fromHermes, "one").Task.ID
	second := h.ask("a1", fromHermes, "two").Task.ID
	h.linkUpdate("a1", first, "state", "input-required", "text", "?")
	h.linkUpdate("a1", second, "state", "input-required", "text", "?")
	third := h.ask("a1", fromHermes, "three")
	if !third.Created || third.Task.ID == first || third.Task.ID == second {
		t.Errorf("third = %+v", third)
	}
	if !h.ask("a1", fromHermes, "other context", "ctx-2").Created {
		t.Error("other context did not create a task")
	}
	other := Caller{Principal: auth.Principal{Name: "athena", Kind: auth.KindPeer}}
	h.linkUpdate("a1", second, "state", "completed")
	if !h.ask("a1", other, "four").Created {
		t.Error("another sender did not create a task")
	}
	if n := len(got(h.engine.ReadTask(first)).ok(t).History); n != 2 {
		t.Errorf("first history = %d", n)
	}
}

func TestSendKeepsPiToPiTrafficOnExplicitTaskIDs(t *testing.T) {
	h := setup(t)
	h.register("asker")
	h.register("helper")
	id := h.ask("helper", asker, "go").Task.ID
	h.linkUpdate("helper", id, "state", "input-required", "text", "which?")
	next := h.ask("helper", asker, "that")
	if !next.Created || next.Task.ID == id {
		t.Errorf("next = %+v", next)
	}
	if state := got(h.engine.ReadTask(id)).ok(t).Status.State; state != StateInputRequired {
		t.Errorf("state = %s", state)
	}
}

func TestSendContinuesTheInterruptedTaskOfAnExternalAgentWhenItsPiSenderWritesInTheContext(t *testing.T) {
	h := setup(t)
	h.register("asker")
	h.register("asker-2")
	ok(t, h.engine.EnsureExternal("hermes", "hermes"))
	id := h.ask("hermes", asker, "research").Task.ID
	got(h.engine.ApplyChange(id, TaskChange{State: StateInputRequired, Message: h.engine.AgentMessage("which topic?")})).ok(t)
	if !h.ask("hermes", Caller{Principal: mars, Agent: "asker-2"}, "mine").Created {
		t.Error("another pi sender did not create a task")
	}
	next := h.ask("hermes", asker, "A2A")
	if next.Created || next.Task.ID != id || next.Task.Status.State != StateWorking {
		t.Errorf("next = %+v", next)
	}
}

func TestSendQueuesASendForEachTaskMessageAndCancelSwapsAWaitingSendForACancel(t *testing.T) {
	h := setup(t)
	h.register("asker", "name", "bee")
	ok(t, h.engine.EnsureExternal("hermes", "Hermes"))
	kicks := 0
	h.engine.OnExternal(func() { kicks++ })
	first := h.ask("hermes", asker, "research").Task
	id := first.ID
	if kicks != 1 {
		t.Errorf("kicks = %d", kicks)
	}
	jobs := got(h.engine.ExternalJobs()).ok(t)
	if len(jobs) != 1 || jobs[0].AgentID != "hermes" || jobs[0].TaskID != id || jobs[0].Op != "send" {
		t.Fatalf("jobs = %+v", jobs)
	}
	remote := got(h.engine.StartRemote(jobs[0])).ok(t)
	if jobs := got(h.engine.ExternalJobs()).ok(t); len(jobs) != 0 {
		t.Errorf("jobs after start = %+v", jobs)
	}
	if !got(h.engine.RemoteBusy("hermes", first.ContextID)).ok(t) {
		t.Error("the context is not busy")
	}
	ok(t, h.engine.RemoteDone(remote))
	got(h.engine.ApplyChange(id, TaskChange{State: StateInputRequired, Message: h.engine.AgentMessage("which?")})).ok(t)
	inbox := got(h.engine.Inbox("asker", true, 10)).ok(t)
	equalJSON(t, "from", inbox[0].From, obj("name", "Hermes", "kind", "external", "agent", "hermes"))
	next := h.ask("hermes", asker, "A2A")
	if next.Task.ID != id {
		t.Errorf("next task = %s", next.Task.ID)
	}
	jobs = got(h.engine.ExternalJobs()).ok(t)
	if len(jobs) != 1 || jobs[0].TaskID != id || jobs[0].Op != "send" {
		t.Fatalf("jobs = %+v", jobs)
	}
	out := got(h.engine.OutboundMessage(id, deref(jobs[0].MessageID))).ok(t)
	if out == nil || out.Text != "A2A" || out.ContextID != first.ContextID || out.SenderName != "bee" || deref(out.SenderHost) != "mars" {
		t.Errorf("outbound = %+v", out)
	}
	got(h.engine.Cancel("hermes", asker, id)).ok(t)
	jobs = got(h.engine.ExternalJobs()).ok(t)
	if len(jobs) != 1 || jobs[0].TaskID != id || jobs[0].Op != "cancel" {
		t.Errorf("jobs after cancel = %+v", jobs)
	}
}

func TestSendRefusesAMessageToAnExternalAgentThatMakesNoTask(t *testing.T) {
	h := setup(t)
	h.register("asker")
	ok(t, h.engine.EnsureExternal("hermes", "hermes"))
	_, err := h.engine.Send(SendInput{AgentID: "hermes", Caller: asker, Message: withMeta(message("fyi", "contextId", "ctx-1"), "kind", "note")})
	if reason(t, err) != ReasonInvalid || err.Error() != "external agents take only task, question, or correction messages" {
		t.Errorf("note = %v", err)
	}
	if inbox := got(h.engine.Inbox("hermes", false, 10)).ok(t); len(inbox) != 0 {
		t.Errorf("inbox = %+v", inbox)
	}
	for _, kind := range []string{"question", "correction"} {
		sent := h.send("hermes", asker, withMeta(message(kind, "contextId", "ctx-"+kind), "kind", kind))
		if !sent.Created {
			t.Errorf("%s did not create a task", kind)
		}
	}
}

func TestSendRefusesAFollowUpToAnExternalTaskThatDoesNotWaitForInput(t *testing.T) {
	h := setup(t)
	h.register("asker")
	ok(t, h.engine.EnsureExternal("hermes", "hermes"))
	id := h.ask("hermes", asker, "research").Task.ID
	follow := func(text string) (*SendResult, error) {
		return h.engine.Send(SendInput{AgentID: "hermes", Caller: asker, Message: message(text, "taskId", id)})
	}
	if _, err := follow("queued"); err == nil || err.Error() != "Task "+id+" of external agent hermes takes a follow-up only when it waits for input" {
		t.Errorf("queued = %v", err)
	}
	got(h.engine.ApplyChange(id, TaskChange{State: StateWorking})).ok(t)
	if r := got(follow("running")).reason(t); r != ReasonInvalid {
		t.Errorf("running = %q", r)
	}
	if jobs := got(h.engine.ExternalJobs()).ok(t); len(jobs) != 1 {
		t.Errorf("jobs = %d", len(jobs))
	}
	got(h.engine.ApplyChange(id, TaskChange{State: StateAuthRequired, Message: h.engine.AgentMessage("login")})).ok(t)
	if task := got(follow("done")).ok(t).Task; task.ID != id {
		t.Errorf("task = %s", task.ID)
	}
	if jobs := got(h.engine.ExternalJobs()).ok(t); len(jobs) != 2 {
		t.Errorf("jobs = %d", len(jobs))
	}
}

func TestSendLeavesAnonymousCallersAndBuiltinAgentsOnExplicitTaskIDs(t *testing.T) {
	h := setup(t)
	ok(t, h.engine.EnsureBuiltin("tck", "tck"))
	h.register("a1")
	anonymous := Caller{Principal: auth.Anonymous}
	first := h.ask("a1", anonymous, "plan").Task.ID
	h.linkUpdate("a1", first, "state", "input-required", "text", "which?")
	if !h.ask("a1", anonymous, "other").Created {
		t.Error("anonymous follow-up joined the task")
	}
	builtin := h.ask("tck", fromHermes, "plan", "ctx-2").Task.ID
	got(h.engine.ApplyChange(builtin, TaskChange{State: StateInputRequired, Message: h.engine.AgentMessage("which?")})).ok(t)
	if !h.ask("tck", fromHermes, "other", "ctx-2").Created {
		t.Error("built-in follow-up joined the task")
	}
}

func TestEnsureExternalKeepsAPiIDOutOfTheExternalRows(t *testing.T) {
	h := setup(t)
	h.register("asker")
	if err := h.engine.EnsureExternal("asker", "x"); reason(t, err) == "" {
		t.Error("EnsureExternal took a pi id")
	}
	if jobs := got(h.engine.ExternalJobs()).ok(t); len(jobs) != 0 {
		t.Errorf("jobs = %+v", jobs)
	}
	h.ask("asker", asker, "self")
	if jobs := got(h.engine.ExternalJobs()).ok(t); len(jobs) != 0 {
		t.Errorf("jobs = %+v", jobs)
	}
}

// ---------- status text ----------

func TestSetStatusPutsTheStatusTextOnEveryWorkingTaskWithoutHistoryGrowth(t *testing.T) {
	h := setup(t)
	h.register("a1")
	working := h.taskID("a1", fromHermes, message("w"))
	submitted := h.taskID("a1", fromHermes, message("s"))
	h.linkUpdate("a1", working, "state", "working")
	var updates []StreamResponse
	h.engine.OnTask(working, func(event TaskEvent) { updates = append(updates, event.Response) })
	h.advance(1000)
	if text := got(h.engine.SetStatus("a1", "  reading\n files  ")).ok(t); text != "reading files" {
		t.Errorf("text = %q", text)
	}
	task := got(h.engine.ReadTask(working)).ok(t)
	matchJSON(t, "status message", task.Status.Message, obj(
		"role", "ROLE_AGENT", "parts", []any{obj("text", "reading files")},
		"metadata", jsonx.NewObject(LegacyBusExtensionURI, obj("statusText", true)),
	))
	if task.Status.Timestamp != "2026-01-01T00:00:01.000Z" || len(task.History) != 1 || len(updates) != 1 {
		t.Errorf("timestamp %s, history %d, updates %d", task.Status.Timestamp, len(task.History), len(updates))
	}
	if msg := got(h.engine.ReadTask(submitted)).ok(t).Status.Message; msg != nil {
		t.Errorf("submitted status message = %s", jsonx.Stringify(msg))
	}
	got(h.engine.SetStatus("a1", "")).ok(t)
	if msg := got(h.engine.ReadTask(working)).ok(t).Status.Message; msg != nil {
		t.Errorf("status message after clear = %s", jsonx.Stringify(msg))
	}
}

func TestSetStatusLimitsTheTextTo160CharsAndEmitsNothingWhenItDoesNotChange(t *testing.T) {
	h := setup(t)
	h.register("a1")
	if text := got(h.engine.SetStatus("a1", strings.Repeat("x", 300))).ok(t); len(text) != 160 {
		t.Errorf("len = %d", len(text))
	}
	id := h.taskID("a1", fromHermes, message("go"))
	h.linkUpdate("a1", id, "state", "working")
	var updates []TaskEvent
	h.engine.OnTask(id, func(event TaskEvent) { updates = append(updates, event) })
	got(h.engine.SetStatus("a1", strings.Repeat("x", 200))).ok(t)
	if len(updates) != 0 {
		t.Errorf("updates = %d", len(updates))
	}
}

func TestLinkUpdateSetsTheStatusTextWhenATaskEntersWorking(t *testing.T) {
	h := setup(t)
	h.register("a1")
	got(h.engine.SetStatus("a1", "busy compiling")).ok(t)
	id := h.taskID("a1", fromHermes, message("go"))
	task := h.linkUpdate("a1", id, "state", "working")
	equalJSON(t, "parts", task.Status.Message.Value("parts"), []any{obj("text", "busy compiling")})
	if len(task.History) != 1 {
		t.Errorf("history = %d", len(task.History))
	}
}

func TestAStatusTextMessageIsKnownUnderEitherURI(t *testing.T) {
	for _, uri := range BusExtensionURIs {
		if !isStatusTextMessage(obj("metadata", obj(uri, obj("statusText", true)))) {
			t.Errorf("%s: not a status text message", uri)
		}
	}
	if isStatusTextMessage(obj("metadata", obj("other", obj("statusText", true)))) {
		t.Error("another key counts as a status text message")
	}
}
