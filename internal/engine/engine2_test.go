package engine

import (
	"errors"
	"slices"
	"testing"

	"github.com/orswn/a2a-hub/internal/auth"
)

func replyIDs(r *PendingResult) []string {
	ids := []string{}
	for _, row := range r.ExpectReply {
		ids = append(ids, row.MessageID)
	}
	return ids
}

// ---------- expect_reply ----------

func TestSendSettlesAnOpenQuestionWhenTheReplyWithReplyToArrives(t *testing.T) {
	h := setup(t)
	h.register("asker")
	h.register("helper")
	helper := Caller{Principal: mars, Agent: "helper"}
	sent := h.send("helper", asker, withMeta(message("ping"), "kind", "note", "expectReply", true))
	if ids := replyIDs(got(h.engine.Pending("asker")).ok(t)); !slices.Equal(ids, []string{"m-ping"}) {
		t.Errorf("pending = %q", ids)
	}
	contextID, _ := sent.Message.String("contextId")
	h.send("asker", helper, withMeta(message("pong", "contextId", contextID), "kind", "answer", "replyTo", "m-ping"))
	if ids := replyIDs(got(h.engine.Pending("asker")).ok(t)); len(ids) != 0 {
		t.Errorf("pending = %q", ids)
	}
}

func TestApplyChangeSettlesATaskQuestionOnInputRequiredOrAFinalState(t *testing.T) {
	h := setup(t)
	h.register("asker")
	h.register("helper")
	id := h.taskID("helper", asker, withMeta(message("q"), "expectReply", true))
	matchJSON(t, "pending", got(h.engine.Pending("asker")).ok(t), obj(
		"expectReply", []any{obj("messageId", "m-q", "taskId", id)},
		"tasks", []any{obj("taskId", id, "agent", "helper", "state", "submitted")},
	))
	h.linkUpdate("helper", id, "state", "working")
	if n := len(got(h.engine.Pending("asker")).ok(t).ExpectReply); n != 1 {
		t.Errorf("open questions while working = %d", n)
	}
	h.linkUpdate("helper", id, "state", "input-required", "text", "?")
	pending := got(h.engine.Pending("asker")).ok(t)
	if len(pending.ExpectReply) != 0 || len(pending.Tasks) != 1 {
		t.Errorf("pending = %+v", pending)
	}
	h.send("helper", asker, message("a", "taskId", id))
	h.linkUpdate("helper", id, "state", "completed")
	if n := len(got(h.engine.Pending("asker")).ok(t).Tasks); n != 0 {
		t.Errorf("tasks = %d", n)
	}
}

// ---------- scope ----------

func (h *testEnv) listIDs(agentID string, caller Caller, input ListTasksInput) []string {
	h.t.Helper()
	ids := []string{}
	for _, task := range got(h.engine.ListTasks(agentID, caller, input)).ok(h.t).Tasks {
		ids = append(ids, task.ID)
	}
	return ids
}

func TestGetTaskShowsATaskToItsSenderPrincipalAndToTheHostOfTheAgentOnly(t *testing.T) {
	h := setup(t)
	h.register("a1")
	other := Caller{Principal: auth.Principal{Name: "other", Kind: auth.KindPeer}}
	id := h.taskID("a1", fromHermes, message("go"))
	for _, caller := range []Caller{fromHermes, {Principal: mars}} {
		if task := got(h.engine.GetTask("a1", caller, id, nil)).ok(t); task.ID != id {
			t.Errorf("%s: id = %s", caller.Principal.Name, task.ID)
		}
	}
	for _, caller := range []Caller{other, {Principal: venus}} {
		if r := got(h.engine.GetTask("a1", caller, id, nil)).reason(t); r != ReasonTaskNotFound {
			t.Errorf("%s: %q", caller.Principal.Name, r)
		}
	}
	if ids := h.listIDs("a1", fromHermes, ListTasksInput{PageSize: 10}); !slices.Equal(ids, []string{id}) {
		t.Errorf("hermes list = %q", ids)
	}
	if ids := h.listIDs("a1", other, ListTasksInput{PageSize: 10}); len(ids) != 0 {
		t.Errorf("other list = %q", ids)
	}
}

func TestGetTaskLimitsACallerWithAnAgentHeaderToTasksThatAgentSentOrReceived(t *testing.T) {
	h := setup(t)
	h.register("a1")
	h.register("s1")
	h.register("s2")
	s1 := Caller{Principal: mars, Agent: "s1"}
	s2 := Caller{Principal: mars, Agent: "s2"}
	id := h.taskID("a1", s1, message("go"))
	for _, caller := range []Caller{s1, {Principal: mars, Agent: "a1"}} {
		if task := got(h.engine.GetTask("a1", caller, id, nil)).ok(t); task.ID != id {
			t.Errorf("%s: id = %s", caller.Agent, task.ID)
		}
	}
	if r := got(h.engine.GetTask("a1", s2, id, nil)).reason(t); r != ReasonTaskNotFound {
		t.Errorf("s2: %q", r)
	}
}

func TestGetTaskLimitsEachAgentRouteToTheTasksOfThatAgent(t *testing.T) {
	h := setup(t)
	h.register("a1")
	h.register("a2")
	id := h.taskID("a1", fromHermes, message("go"))
	if r := got(h.engine.GetTask("a2", fromHermes, id, nil)).reason(t); r != ReasonTaskNotFound {
		t.Errorf("a2: %q", r)
	}
	if ids := h.listIDs("a2", fromHermes, ListTasksInput{PageSize: 10}); len(ids) != 0 {
		t.Errorf("a2 list = %q", ids)
	}
}

// ---------- list tasks ----------

func TestListTasksPagesByStatusTimestampDescendingAndFilters(t *testing.T) {
	h := setup(t)
	h.register("a1")
	var ids []string
	for i := range 5 {
		h.advance(1000)
		ids = append(ids, h.taskID("a1", fromHermes, message("t"+string(rune('0'+i)), "contextId", "c"+string(rune('0'+i%2)))))
	}
	first := got(h.engine.ListTasks("a1", fromHermes, ListTasksInput{PageSize: 2})).ok(t)
	if len(first.Tasks) != 2 || first.Tasks[0].ID != ids[4] || first.Tasks[1].ID != ids[3] || first.TotalSize != 5 {
		t.Errorf("first = %+v", first)
	}
	if first.Tasks[0].Artifacts != nil {
		t.Error("artifacts without includeArtifacts")
	}
	second := h.listIDs("a1", fromHermes, ListTasksInput{PageSize: 2, PageToken: first.NextPageToken})
	if !slices.Equal(second, []string{ids[2], ids[1]}) {
		t.Errorf("second = %q", second)
	}
	secondPage := got(h.engine.ListTasks("a1", fromHermes, ListTasksInput{PageSize: 2, PageToken: first.NextPageToken})).ok(t)
	last := got(h.engine.ListTasks("a1", fromHermes, ListTasksInput{PageSize: 2, PageToken: secondPage.NextPageToken})).ok(t)
	if len(last.Tasks) != 1 || last.Tasks[0].ID != ids[0] || last.NextPageToken != "" {
		t.Errorf("last = %+v", last)
	}
	if c1 := h.listIDs("a1", fromHermes, ListTasksInput{PageSize: 10, ContextID: "c1", IncludeArtifacts: true}); !slices.Equal(c1, []string{ids[3], ids[1]}) {
		t.Errorf("c1 = %q", c1)
	}
	zero := 0
	one := got(h.engine.ListTasks("a1", fromHermes, ListTasksInput{PageSize: 1, HistoryLength: &zero, IncludeArtifacts: true})).ok(t)
	if one.Tasks[0].History != nil {
		t.Error("history with historyLength 0")
	}
	if r := got(h.engine.ListTasks("a1", fromHermes, ListTasksInput{PageSize: 1, PageToken: "bad"})).reason(t); r != ReasonInvalid {
		t.Errorf("bad token = %q", r)
	}
}

// ---------- link mailbox ----------

func eventTypes(events []LinkEvent) []string {
	types := []string{}
	for _, e := range events {
		types = append(types, e.Type)
	}
	return types
}

func TestSetOnlineKeepsEventsUntilAckedAndSendsPresenceToTheParentAndChildren(t *testing.T) {
	h := setup(t)
	h.register("parent")
	h.register("child", "parent", "parent", "name", "kid")
	ok(t, h.engine.SetOnline("child", true))
	unacked := got(h.engine.Unacked("parent")).ok(t)
	if len(unacked) != 1 || string(unacked[0].Payload) != `{"agent":"child","name":"kid","online":true,"relation":"child"}` {
		t.Errorf("parent events = %+v", unacked)
	}
	ok(t, h.engine.SetOnline("parent", true))
	if types := eventTypes(got(h.engine.Unacked("child")).ok(t)); !slices.Equal(types, []string{"presence"}) {
		t.Errorf("child events = %q", types)
	}
	got(h.engine.Ack("parent", float64(unacked[0].Seq))).ok(t)
	if n := len(got(h.engine.Unacked("parent")).ok(t)); n != 0 {
		t.Errorf("parent events after ack = %d", n)
	}
	ok(t, h.engine.SetOnline("child", true))
	if n := len(got(h.engine.Unacked("parent")).ok(t)); n != 0 {
		t.Errorf("parent events after a second online = %d", n)
	}
}

func TestControlAllowsCloseOnlyFromTheParent(t *testing.T) {
	h := setup(t)
	h.register("parent")
	h.register("child", "parent", "parent")
	h.register("stranger")
	if r := reason(t, h.engine.Control("stranger", "child", "close")); r != ReasonForbidden {
		t.Errorf("stranger = %q", r)
	}
	if r := reason(t, h.engine.Control("parent", "child", "close")); r != ReasonConflict {
		t.Errorf("offline child = %q", r)
	}
	ok(t, h.engine.SetOnline("child", true))
	ok(t, h.engine.Control("parent", "child", "close"))
	if types := eventTypes(got(h.engine.Unacked("child")).ok(t)); !slices.Equal(types, []string{"control"}) {
		t.Errorf("child events = %q", types)
	}
}

func texts(t *testing.T, events []LinkEvent) []string {
	t.Helper()
	out := []string{}
	for _, e := range events {
		text, _ := payload(t, e).String("text")
		out = append(out, text)
	}
	return out
}

func TestReplayGivesOnlyUnackedEventsAfterAGivenSeq(t *testing.T) {
	h := setup(t)
	h.register("child")
	for _, text := range []string{"one", "two", "three"} {
		h.send("child", fromHermes, withMeta(message(text), "kind", "note"))
	}
	events := got(h.engine.Unacked("child")).ok(t)
	got(h.engine.Ack("child", float64(events[0].Seq))).ok(t)
	zero := int64(0)
	for _, c := range []struct {
		after *int64
		want  []string
	}{{&events[1].Seq, []string{"three"}}, {nil, []string{"two", "three"}}, {&zero, []string{"two", "three"}}} {
		if got := texts(t, got(h.engine.Replay("child", c.after)).ok(t)); !slices.Equal(got, c.want) {
			t.Errorf("replay after %v = %q", c.after, got)
		}
	}
}

func TestReplayAcksAndSkipsControlEventsOlderThan30s(t *testing.T) {
	h := setup(t)
	h.register("parent")
	h.register("child", "parent", "parent")
	ok(t, h.engine.SetOnline("child", true))
	ok(t, h.engine.Control("parent", "child", "close"))
	h.send("child", fromHermes, withMeta(message("keep"), "kind", "note"))
	h.advance(10_000)
	if types := eventTypes(got(h.engine.Replay("child", nil)).ok(t)); !slices.Equal(types, []string{"control", "message"}) {
		t.Errorf("replay at 10 s = %q", types)
	}
	h.advance(25_000)
	if types := eventTypes(got(h.engine.Replay("child", nil)).ok(t)); !slices.Equal(types, []string{"message"}) {
		t.Errorf("replay at 35 s = %q", types)
	}
	if types := eventTypes(got(h.engine.Unacked("child")).ok(t)); !slices.Equal(types, []string{"message"}) {
		t.Errorf("unacked = %q", types)
	}
}

func TestMessageFindsAMessageByPrefixAndReportsAnAmbiguousPrefix(t *testing.T) {
	h := setup(t)
	h.register("a1")
	h.register("a2")
	h.send("a1", fromHermes, withMeta(message("x", "messageId", "abc-1"), "kind", "note"))
	h.send("a1", fromHermes, withMeta(message("y", "messageId", "abc-2"), "kind", "note"))
	h.send("a2", fromHermes, withMeta(message("z", "messageId", "abc-3"), "kind", "note"))
	matchJSON(t, "abc-1", got(h.engine.Message("a1", "abc-1")).ok(t), obj("messageId", "abc-1", "text", "x", "direction", "in"))
	_, err := h.engine.Message("a1", "abc")
	var hubErr *Error
	if !errors.As(err, &hubErr) || hubErr.Status != 409 || len(hubErr.Details.(MessageCandidates).Candidates) != 2 {
		t.Errorf("abc = %v", err)
	}
	if r := got(h.engine.Message("a1", "zzz")).reason(t); r != ReasonNotFound {
		t.Errorf("zzz = %q", r)
	}
	if n := got(h.engine.MarkRead("a1", []any{"abc-1"})).ok(t); n != 1 {
		t.Errorf("marked = %d", n)
	}
	inbox := got(h.engine.Inbox("a1", true, 10)).ok(t)
	if len(inbox) != 1 || inbox[0].MessageID != "abc-2" {
		t.Errorf("inbox = %+v", inbox)
	}
}

func TestSendKeepsOneInboxRowPerRecipientWhenAFanOutReusesAMessageID(t *testing.T) {
	h := setup(t)
	h.register("a1")
	h.register("a2")
	for _, agent := range []string{"a1", "a2"} {
		h.send(agent, fromHermes, withMeta(message("same", "messageId", "fan"), "kind", "note"))
	}
	for _, agent := range []string{"a1", "a2"} {
		if n := len(got(h.engine.Inbox(agent, false, 10)).ok(t)); n != 1 {
			t.Errorf("%s inbox = %d", agent, n)
		}
	}
}

// ---------- retention sweep ----------

func TestSweepDeletesOldFinalTasksMessagesAckedEventsSettledRowsAndStaleEphemeralAgents(t *testing.T) {
	h := setup(t)
	h.register("a1")
	h.register("eph", "ephemeral", true)
	ok(t, h.engine.SetOnline("eph", true))
	done := h.taskID("a1", fromHermes, message("old"))
	open := h.taskID("a1", fromHermes, message("open"))
	h.linkUpdate("a1", done, "state", "completed")
	got(h.engine.Ack("a1", float64(1_000_000))).ok(t)
	h.send("eph", fromHermes, withMeta(message("queued"), "kind", "note"))
	ok(t, h.engine.SetOnline("eph", false))
	h.advance(25 * 60 * 60 * 1000)
	if r := got(h.engine.Sweep()).ok(t); r.Agents != 1 || r.Tasks != 0 {
		t.Errorf("first sweep = %+v", r)
	}
	if row := got(h.engine.Agent("eph")).ok(t); row != nil {
		t.Errorf("eph = %+v", row)
	}
	if n := len(got(h.engine.Unacked("eph")).ok(t)); n != 0 {
		t.Errorf("eph events = %d", n)
	}
	h.advance(30 * day)
	r := got(h.engine.Sweep()).ok(t)
	if r.Tasks != 1 || r.Messages < 2 || r.Events < 1 {
		t.Errorf("second sweep = %+v", r)
	}
	if task := got(h.engine.ReadTask(done)).ok(t); task != nil {
		t.Error("the final task is still there")
	}
	if task := got(h.engine.ReadTask(open)).ok(t); task == nil {
		t.Error("the open task is gone")
	}
}

func TestSweepRemovesEveryRowOfASweptAgentSoANewOwnerOfTheIDSeesNothing(t *testing.T) {
	h := setup(t)
	h.register("peer-of-eph")
	h.register("eph", "ephemeral", true)
	toEph := h.taskID("eph", fromHermes, message("secret for mars"))
	fromEph := h.taskID("peer-of-eph", Caller{Principal: mars, Agent: "eph"}, withMeta(message("question"), "kind", "task", "expectReply", true))
	ok(t, h.engine.SetOnline("eph", true))
	ok(t, h.engine.SetOnline("eph", false))
	h.advance(2 * day)
	if r := got(h.engine.Sweep()).ok(t); r.Agents != 1 || r.Tasks != 2 {
		t.Errorf("sweep = %+v", r)
	}
	got(h.registerAs(venus, "eph")).ok(t)
	asVenus := Caller{Principal: venus, Agent: "eph"}
	if n := len(got(h.engine.Inbox("eph", false, 10)).ok(t)); n != 0 {
		t.Errorf("inbox = %d", n)
	}
	if r := got(h.engine.GetTask("eph", Caller{Principal: venus}, toEph, nil)).reason(t); r != ReasonTaskNotFound {
		t.Errorf("task to eph = %q", r)
	}
	if r := got(h.engine.GetTask("peer-of-eph", asVenus, fromEph, nil)).reason(t); r != ReasonTaskNotFound {
		t.Errorf("task from eph = %q", r)
	}
	empty := `{"expectReply":[],"tasks":[]}`
	if p := jsonOf(t, got(h.engine.Pending("eph")).ok(t)); p != empty {
		t.Errorf("pending = %s", p)
	}
	if p := jsonOf(t, got(h.engine.Owed("peer-of-eph")).ok(t)); p != empty {
		t.Errorf("owed = %s", p)
	}
	if r := got(h.engine.Thread("eph", "id-")).reason(t); r != ReasonNotFound {
		t.Errorf("thread = %q", r)
	}
	if n := len(got(h.engine.Unacked("eph")).ok(t)); n != 0 {
		t.Errorf("events = %d", n)
	}
}

func TestSweepRemovesANonEphemeralAgentAfter90DaysOffline(t *testing.T) {
	h := setup(t)
	h.register("old")
	h.register("recent")
	h.send("old", fromHermes, message("pending work"))
	ok(t, h.engine.SetOnline("old", true))
	ok(t, h.engine.SetOnline("old", false))
	h.advance(89 * day)
	ok(t, h.engine.SetOnline("recent", true))
	ok(t, h.engine.SetOnline("recent", false))
	if r := got(h.engine.Sweep()).ok(t); r.Agents != 0 {
		t.Errorf("sweep at 89 days = %+v", r)
	}
	h.advance(2 * day)
	if r := got(h.engine.Sweep()).ok(t); r.Agents != 1 || r.Tasks != 1 {
		t.Errorf("sweep at 91 days = %+v", r)
	}
	if row := got(h.engine.Agent("old")).ok(t); row != nil {
		t.Error("old is still there")
	}
	if row := got(h.engine.Agent("recent")).ok(t); row == nil {
		t.Error("recent is gone")
	}
}
