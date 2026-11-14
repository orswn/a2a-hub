package engine

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/orswn/a2a-hub/internal/store"
)

// ---------- push config scope ----------

func TestPushConfigIsVisibleOnlyToThePrincipalThatCreatedIt(t *testing.T) {
	h := setup(t)
	h.register("a1")
	id := h.taskID("a1", fromHermes, message("job"))
	config := PushConfigInput{TaskID: id, ID: "c1", URL: "https://hooks.example/x", Token: "SECRET"}
	got(h.engine.CreatePushConfig("a1", fromHermes, config)).ok(t)
	host := Caller{Principal: mars}
	if task := got(h.engine.GetTask("a1", host, id, nil)).ok(t); task.ID != id {
		t.Errorf("host task = %s", task.ID)
	}
	if list := got(h.engine.ListPushConfigs("a1", host, id)).ok(t); len(list) != 0 {
		t.Errorf("host list = %+v", list)
	}
	if r := got(h.engine.GetPushConfig("a1", host, id, "c1")).reason(t); r != ReasonTaskNotFound {
		t.Errorf("host get = %q", r)
	}
	if r := reason(t, h.engine.DeletePushConfig("a1", host, id, "c1")); r != ReasonTaskNotFound {
		t.Errorf("host delete = %q", r)
	}
	evil := config
	evil.URL = "https://evil.example/"
	if r := got(h.engine.CreatePushConfig("a1", host, evil)).reason(t); r != ReasonInvalid {
		t.Errorf("host overwrite = %q", r)
	}
	matchJSON(t, "c1", got(h.engine.GetPushConfig("a1", fromHermes, id, "c1")).ok(t), obj("url", "https://hooks.example/x", "token", "SECRET"))
	got(h.engine.CreatePushConfig("a1", host, PushConfigInput{TaskID: id, ID: "c2", URL: "https://hooks.example/host"})).ok(t)
	list := got(h.engine.ListPushConfigs("a1", fromHermes, id)).ok(t)
	if len(list) != 1 || list[0].ID != "c1" {
		t.Errorf("hermes list = %+v", list)
	}
	ok(t, h.engine.DeletePushConfig("a1", fromHermes, id, "c1"))
	if list := got(h.engine.ListPushConfigs("a1", fromHermes, id)).ok(t); len(list) != 0 {
		t.Errorf("hermes list after delete = %+v", list)
	}
}

// ---------- robustness ----------

func TestRegisterKeepsTopicsAndEphemeralWhenALaterRegisterLeavesThemOut(t *testing.T) {
	h := setup(t)
	h.register("a1", "topics", []string{"build"}, "ephemeral", true)
	h.register("a1", "name", "renamed")
	row := got(h.engine.Agent("a1")).ok(t)
	if row.Name != "renamed" || row.Topics != `["build"]` || row.Ephemeral != 1 {
		t.Errorf("row = %+v", row)
	}
	h.register("a1", "topics", []any{}, "ephemeral", false)
	row = got(h.engine.Agent("a1")).ok(t)
	if row.Topics != "[]" || row.Ephemeral != 0 {
		t.Errorf("row = %+v", row)
	}
}

func TestAfterCommitListenersAllRunWhenOneFailsAndTheFailureIsLogged(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, store.Memory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var lines []string
	engine, err := New(ctx, db, Options{Log: func(line string) { lines = append(lines, line) }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = engine.Close() }()
	got(engine.Register(mars, obj("agent", "a1", "protocol", HubProtocol))).ok(t)
	id := got(engine.Send(SendInput{AgentID: "a1", Caller: fromHermes, Message: message("job")})).ok(t).Task.ID
	var seen []State
	engine.OnTask(id, func(TaskEvent) { panic("listener broke") })
	engine.OnTask(id, func(event TaskEvent) { seen = append(seen, event.State) })
	engine.OnLink(func(string, LinkEvent) { panic("link listener broke") })
	got(engine.LinkUpdate("a1", id, obj("state", "working"))).ok(t)
	if !slices.Equal(seen, []State{StateWorking}) {
		t.Errorf("seen = %q", seen)
	}
	if log := strings.Join(lines, "\n"); !strings.Contains(log, "listener broke") {
		t.Errorf("log = %q", log)
	}
}

func TestWaitForTaskReleasesTheListenerAndTheTimerWhenTheWaitIsCanceled(t *testing.T) {
	h := setup(t)
	h.register("a1")
	id := h.taskID("a1", fromHermes, message("job"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		h.engine.WaitForTask(ctx, id, func(State) bool { return false }, time.Minute)
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for h.engine.ListenerCount(id) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("the wait did not add a listener")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if n := h.engine.ListenerCount(id); n != 0 {
		t.Errorf("listeners = %d", n)
	}
}

// ---------- client support ----------

var helperCaller = Caller{Principal: mars, Agent: "helper"}

func pair(t *testing.T) *testEnv {
	h := setup(t)
	h.register("asker", "name", "Asker")
	h.register("helper", "name", "Helper")
	return h
}

func TestApplyChangeTurnsATaskUpdateWithTextIntoAnInboxRowForTheSenderAgent(t *testing.T) {
	h := pair(t)
	task := h.send("helper", asker, message("go")).Task
	id := task.ID
	h.linkUpdate("helper", id, "state", "working")
	h.linkUpdate("helper", id, "state", "working", "text", "halfway")
	h.linkUpdate("helper", id, "state", "input-required", "text", "which?")
	h.send("helper", asker, message("that", "taskId", id))
	h.linkUpdate("helper", id, "state", "completed", "text", "done")
	rows := got(h.engine.Inbox("asker", false, 10)).ok(t)
	slices.Reverse(rows)
	var summary []string
	for _, row := range rows {
		summary = append(summary, row.Kind+" "+row.Wake.(string)+" "+row.Text)
	}
	if want := []string{"status never halfway", "question now which?", "result now done"}; !slices.Equal(summary, want) {
		t.Errorf("rows = %q", summary)
	}
	matchJSON(t, "result row", rows[2], obj(
		"taskId", id, "contextId", task.ContextID, "direction", "in",
		"from", obj("agent", "helper", "name", "Helper", "kind", "pi"), "to", obj("agent", "asker", "name", "Asker"),
	))
	var resultFound, plainFound bool
	for _, e := range h.linkEvents() {
		if e.agent != "asker" || e.event.Type != "update" {
			continue
		}
		p := payload(t, e.event)
		if p.Value("state") == "completed" && p.Value("kind") == "result" && p.Value("messageId") == rows[2].MessageID {
			resultFound = true
		}
		if _, hasText := p.Get("text"); !hasText && !plainFound {
			plainFound = true
			if _, has := p.Get("messageId"); has {
				t.Errorf("plain update has a messageId: %s", e.event.Payload)
			}
		}
	}
	if !resultFound || !plainFound {
		t.Errorf("result update %v, plain update %v", resultFound, plainFound)
	}
}

func TestThreadReturnsTheMessagesInOrderForBothSidesAndRefusesAnAmbiguousPrefix(t *testing.T) {
	h := pair(t)
	h.send("helper", asker, withMeta(message("q", "contextId", "ctx-a1"), "kind", "note"))
	h.send("asker", helperCaller, withMeta(message("a", "contextId", "ctx-a1"), "kind", "answer"))
	h.send("helper", asker, withMeta(message("other", "contextId", "ctx-a2"), "kind", "note"))
	var sides []string
	for _, m := range got(h.engine.Thread("asker", "ctx-a1")).ok(t).Messages {
		sides = append(sides, m.Text+" "+m.Direction)
	}
	if !slices.Equal(sides, []string{"q out", "a in"}) {
		t.Errorf("asker thread = %q", sides)
	}
	var directions []string
	for _, m := range got(h.engine.Thread("helper", "ctx-a1")).ok(t).Messages {
		directions = append(directions, m.Direction)
	}
	if !slices.Equal(directions, []string{"in", "out"}) {
		t.Errorf("helper thread = %q", directions)
	}
	_, err := h.engine.Thread("asker", "ctx-a")
	var hubErr *Error
	if !errors.As(err, &hubErr) || hubErr.Status != 409 || len(hubErr.Details.(ThreadCandidates).Candidates) != 2 {
		t.Errorf("ctx-a = %v", err)
	}
	for _, prefix := range []string{"nope", "ctx-a2x"} {
		if r := got(h.engine.Thread("asker", prefix)).reason(t); r != ReasonNotFound {
			t.Errorf("%s = %q", prefix, r)
		}
	}
}

func TestSendKeepsOneExpectReplyRowPerRecipientAndOwedListsWhatEachSideOwes(t *testing.T) {
	h := pair(t)
	h.register("third", "name", "Third")
	for _, agent := range []string{"helper", "third"} {
		h.send(agent, asker, withMeta(message("ping", "messageId", "fan-1", "contextId", "c"), "kind", "note", "expectReply", true))
	}
	agents := func(r *PendingResult) []string {
		out := []string{}
		for _, row := range r.ExpectReply {
			out = append(out, row.Agent)
		}
		return out
	}
	if a := agents(got(h.engine.Pending("asker")).ok(t)); !slices.Equal(a, []string{"helper", "third"}) {
		t.Errorf("pending = %q", a)
	}
	owed := got(h.engine.Owed("helper")).ok(t)
	if len(owed.ExpectReply) != 1 {
		t.Fatalf("owed = %+v", owed)
	}
	matchJSON(t, "owed", owed.ExpectReply[0], obj("messageId", "fan-1", "agent", "asker", "name", "Asker"))
	h.send("asker", helperCaller, withMeta(message("pong", "contextId", "c"), "kind", "answer", "replyTo", "fan-1"))
	if a := agents(got(h.engine.Pending("asker")).ok(t)); !slices.Equal(a, []string{"third"}) {
		t.Errorf("pending = %q", a)
	}
	if n := len(got(h.engine.Owed("helper")).ok(t).ExpectReply); n != 0 {
		t.Errorf("owed = %d", n)
	}
	id := h.taskID("helper", asker, message("job"))
	tasks := got(h.engine.Owed("helper")).ok(t).Tasks
	if len(tasks) != 1 {
		t.Fatalf("owed tasks = %+v", tasks)
	}
	matchJSON(t, "owed task", tasks[0], obj("taskId", id, "agent", "asker", "name", "Asker", "state", "submitted"))
	h.send("helper", fromHermes, message("peer job"))
	tasks = got(h.engine.Owed("helper")).ok(t).Tasks
	last := tasks[len(tasks)-1]
	if last.Agent != nil || last.Name != "hermes" {
		t.Errorf("peer task = %+v", last)
	}
}

func TestSetOnlineSendsPresenceToAgentsWithAnOpenQuestionOrALiveTaskEitherWay(t *testing.T) {
	h := pair(t)
	h.register("asker2")
	h.register("idle")
	h.send("helper", asker, message("job"))
	h.send("asker2", helperCaller, withMeta(message("q"), "kind", "note", "expectReply", true))
	ok(t, h.engine.SetOnline("helper", true))
	var got []string
	for _, e := range h.eventsOf("presence") {
		relation, _ := payload(t, e.event).String("relation")
		got = append(got, e.agent+" "+relation)
	}
	slices.Sort(got)
	if !slices.Equal(got, []string{"asker peer", "asker2 peer"}) {
		t.Errorf("presence = %q", got)
	}
}

func TestInboxAndMessageItemsCarryTheBusFields(t *testing.T) {
	h := pair(t)
	h.send("helper", asker, withMeta(message("fyi", "messageId", "rich"),
		"kind", "note", "wake", "never", "priority", "urgent", "expectReply", true, "replyTo", "x", "subject", "s", "data", obj("n", 1)))
	expected := func(direction string) any {
		return obj(
			"messageId", "rich", "kind", "note", "wake", "never", "priority", "urgent", "expectReply", true, "replyTo", "x",
			"subject", "s", "data", obj("n", 1), "from", obj("agent", "asker", "name", "Asker", "kind", "pi"),
			"to", obj("agent", "helper", "name", "Helper"), "direction", direction,
		)
	}
	matchJSON(t, "inbox", got(h.engine.Inbox("helper", true, 10)).ok(t)[0], expected("in"))
	matchJSON(t, "message", got(h.engine.Message("asker", "rich")).ok(t), expected("out"))
}

func TestAddChildPreRegistersAChildThatALaterRegisterFromTheSameHostTakesOver(t *testing.T) {
	h := setup(t)
	h.register("parent")
	added := got(h.engine.AddChild(mars, "parent", obj("id", "kid", "name", "Kid", "role", "tester", "labels", obj("model", "m1")))).ok(t)
	equalJSON(t, "added", added, RegisterResult{Agent: "kid", Name: "Kid", Host: "mars"})
	info := func() AgentInfo { return h.engine.AgentInfo(got(h.engine.RequireAgent("kid")).ok(t)) }
	matchJSON(t, "info", info(), obj("online", false, "parent", "parent", "role", "tester", "labels", obj("model", "m1")))
	id := h.taskID("kid", Caller{Principal: mars, Agent: "parent"}, message("first"))
	matchJSON(t, "event", payload(t, got(h.engine.Unacked("kid")).ok(t)[0]), obj("taskId", id, "text", "first"))
	h.register("other")
	if r := got(h.engine.AddChild(mars, "other", obj("id", "kid"))).reason(t); r != ReasonConflict {
		t.Errorf("other parent = %q", r)
	}
	if r := got(h.engine.AddChild(venus, "parent", obj("id", "kid"))).reason(t); r != ReasonConflict {
		t.Errorf("venus = %q", r)
	}
	equalJSON(t, "register", h.register("kid", "cwd", "/w"), RegisterResult{Agent: "kid", Name: "Kid", Host: "mars"})
	matchJSON(t, "info", info(), obj("parent", "parent", "cwd", "/w", "role", "tester"))
	if r := got(h.registerAs(venus, "kid")).reason(t); r != ReasonForbidden {
		t.Errorf("venus register = %q", r)
	}
}

// ---------- task state on items ----------

func TestInboxMessageAndThreadItemsCarryTheCurrentStateOfTheLinkedTask(t *testing.T) {
	h := setup(t)
	h.register("asker")
	h.register("helper")
	task := h.send("helper", asker, message("go", "messageId", "go-1")).Task
	h.send("helper", asker, withMeta(message("fyi", "messageId", "note-1"), "kind", "note"))
	if s := got(h.engine.Message("helper", "go-1")).ok(t).TaskState; s != "submitted" {
		t.Errorf("before = %q", s)
	}
	h.linkUpdate("helper", task.ID, "state", "completed", "text", "done")
	if s := got(h.engine.Message("helper", "go-1")).ok(t).TaskState; s != "completed" {
		t.Errorf("after = %q", s)
	}
	matchJSON(t, "inbox", got(h.engine.Inbox("asker", false, 10)).ok(t)[0], obj("text", "done", "taskState", "completed"))
	var states []string
	for _, m := range got(h.engine.Thread("asker", task.ContextID)).ok(t).Messages {
		states = append(states, m.TaskState)
	}
	if !slices.Equal(states, []string{"completed", "completed"}) {
		t.Errorf("thread states = %q", states)
	}
	note := toValue(t, got(h.engine.Message("helper", "note-1")).ok(t))
	if _, has := note.(interface{ Get(string) (any, bool) }).Get("taskState"); has {
		t.Error("the note has a taskState")
	}
}
