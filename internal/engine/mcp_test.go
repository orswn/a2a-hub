package engine

import (
	"sync"
	"testing"

	"github.com/orswn/a2a-hub/internal/jsonx"
)

// hermesMCP is the hermes external agent as it sends through the MCP endpoint.
var hermesMCP = Caller{Principal: hermes, Agent: "hermes"}

func telegramOrigin() *jsonx.Object {
	return jsonx.NewObject("platform", "telegram", "chat_id", "-100123", "thread_id", "42")
}

// mcpThread makes the external agent hermes and session p1, and a task from hermes to p1 sent through MCP.
func mcpThread(t *testing.T, h *testEnv, origin *jsonx.Object) *SendResult {
	t.Helper()
	ok(t, h.engine.EnsureExternal("hermes", "hermes"))
	h.register("p1")
	return got(h.engine.Send(SendInput{
		AgentID: "p1", Caller: hermesMCP, Message: withMeta(message("ask", "contextId", "ctx-1"), "kind", "task"),
		MCP: true, Origin: origin,
	})).ok(t)
}

func TestAReplyToAnMCPSendOfAnExternalAgentStaysInItsInbox(t *testing.T) {
	h := setup(t)
	sent := mcpThread(t, h, telegramOrigin())
	fromP1 := Caller{Principal: mars, Agent: "p1"}
	reply := h.send("hermes", fromP1, withMeta(message("answer", "contextId", "other-ctx"), "kind", "answer", "replyTo", "m-ask"))
	if reply.Task != nil || reply.Message == nil {
		t.Fatalf("reply = %+v", reply)
	}
	more := h.send("hermes", fromP1, withMeta(message("more", "contextId", "ctx-1"), "kind", "task"))
	if more.Task != nil {
		t.Fatalf("a task in the thread became a task: %+v", more.Task)
	}
	if jobs := got(h.engine.ExternalJobs()).ok(t); len(jobs) != 0 {
		t.Errorf("jobs = %+v", jobs)
	}
	inbox := got(h.engine.MCPInbox("hermes", true, 10)).ok(t)
	if len(inbox) != 2 || inbox[0].MessageID != "m-more" || inbox[1].MessageID != "m-answer" {
		t.Errorf("inbox = %+v", inbox)
	}
	if sent.Task == nil || sent.Meta.Origin == nil {
		t.Errorf("sent = %+v", sent)
	}
}

func TestATaskToAnExternalAgentOutsideAnMCPThreadIsStillDispatched(t *testing.T) {
	h := setup(t)
	mcpThread(t, h, nil)
	fromP1 := Caller{Principal: mars, Agent: "p1"}
	task := h.send("hermes", fromP1, withMeta(message("job", "contextId", "ctx-2"), "kind", "task"))
	if task.Task == nil {
		t.Fatal("no task")
	}
	if jobs := got(h.engine.ExternalJobs()).ok(t); len(jobs) != 1 {
		t.Errorf("jobs = %+v", jobs)
	}
	if r := got(h.engine.Send(SendInput{AgentID: "hermes", Caller: fromP1, Message: withMeta(message("note", "contextId", "ctx-2"), "kind", "note")})).reason(t); r != ReasonInvalid {
		t.Errorf("note outside a thread = %q", r)
	}
	if inbox := got(h.engine.MCPInbox("hermes", false, 10)).ok(t); len(inbox) != 0 {
		t.Errorf("the dispatched task shows in the MCP inbox: %+v", inbox)
	}
}

func TestAnMCPSendOfASessionDoesNotMakeAThreadForThatSession(t *testing.T) {
	h := setup(t)
	ok(t, h.engine.EnsureExternal("hermes", "hermes"))
	h.register("p1")
	h.register("p2")
	got(h.engine.Send(SendInput{AgentID: "p2", Caller: Caller{Principal: mars, Agent: "p1"},
		Message: withMeta(message("hi", "contextId", "ctx-3"), "kind", "note"), MCP: true})).ok(t)
	if thread := got(h.engine.MCPThread("hermes", "ctx-3", "")).ok(t); thread != nil {
		t.Errorf("thread = %+v", thread)
	}
}

func TestMCPThreadGivesTheOriginOfTheSendThatStartedTheThread(t *testing.T) {
	h := setup(t)
	mcpThread(t, h, telegramOrigin())
	got(h.engine.Send(SendInput{
		AgentID: "p1", Caller: hermesMCP, Message: withMeta(message("later", "contextId", "ctx-1"), "kind", "note"),
		MCP: true, Origin: jsonx.NewObject("platform", "telegram", "chat_id", "-999", "thread_id", jsonx.Null),
	})).ok(t)
	thread := got(h.engine.MCPThread("hermes", "ctx-1", "")).ok(t)
	if thread == nil {
		t.Fatal("no thread")
	}
	equalJSON(t, "origin", thread.Origin, telegramOrigin())
	byReply := got(h.engine.MCPThread("hermes", "elsewhere", "m-ask")).ok(t)
	if byReply == nil || byReply.ContextID != "ctx-1" {
		t.Errorf("by reply = %+v", byReply)
	}
	if none := got(h.engine.MCPThread("hermes", "elsewhere", "")).ok(t); none != nil {
		t.Errorf("unrelated = %+v", none)
	}
}

func TestMCPThreadWithoutOriginHasANilOrigin(t *testing.T) {
	h := setup(t)
	mcpThread(t, h, nil)
	thread := got(h.engine.MCPThread("hermes", "ctx-1", "")).ok(t)
	if thread == nil || thread.Origin != nil {
		t.Errorf("thread = %+v", thread)
	}
}

func TestInboxListenersHearEveryStoredMessageAfterTheCommit(t *testing.T) {
	h := setup(t)
	var mu sync.Mutex
	var notices []InboxNotice
	h.engine.OnInbox(func(n InboxNotice) {
		mu.Lock()
		defer mu.Unlock()
		notices = append(notices, n)
	})
	sent := mcpThread(t, h, nil)
	h.linkUpdate("p1", sent.Task.ID, "state", "completed", "text", "done")
	mu.Lock()
	defer mu.Unlock()
	if len(notices) != 2 {
		t.Fatalf("notices = %+v", notices)
	}
	first, second := notices[0], notices[1]
	if first.AgentID != "p1" || first.MessageID != "m-ask" || first.ContextID != "ctx-1" || first.Kind != "task" || first.Wake != "now" {
		t.Errorf("first = %+v", first)
	}
	if second.AgentID != "hermes" || second.TaskID != sent.Task.ID || second.Kind != "result" || second.MessageID == "" {
		t.Errorf("second = %+v", second)
	}
}

func TestFirstReplyFindsTheOldestUnreadReplyOrTaskAnswer(t *testing.T) {
	h := setup(t)
	sent := mcpThread(t, h, nil)
	if none := got(h.engine.FirstReply("hermes", "m-ask", []string{sent.Task.ID})).ok(t); none != nil {
		t.Fatalf("reply before any = %+v", none)
	}
	h.linkUpdate("p1", sent.Task.ID, "state", "working", "text", "still on it")
	if none := got(h.engine.FirstReply("hermes", "m-ask", []string{sent.Task.ID})).ok(t); none != nil {
		t.Fatalf("a status note counts as a reply: %+v", none)
	}
	h.linkUpdate("p1", sent.Task.ID, "state", "completed", "text", "done")
	reply := got(h.engine.FirstReply("hermes", "m-ask", []string{sent.Task.ID})).ok(t)
	if reply == nil || reply.Text != "done" || reply.Kind != "result" {
		t.Fatalf("reply = %+v", reply)
	}
	got(h.engine.MarkRead("hermes", []any{reply.MessageID})).ok(t)
	if again := got(h.engine.FirstReply("hermes", "m-ask", []string{sent.Task.ID})).ok(t); again != nil {
		t.Errorf("read reply = %+v", again)
	}
	h.send("hermes", Caller{Principal: mars, Agent: "p1"}, withMeta(message("by reply", "contextId", "ctx-1"), "kind", "answer", "replyTo", "m-ask"))
	byReply := got(h.engine.FirstReply("hermes", "m-ask", nil)).ok(t)
	if byReply == nil || byReply.MessageID != "m-by reply" {
		t.Errorf("by reply = %+v", byReply)
	}
}

func TestTaskOpenerGivesTheMessageThatMadeTheTask(t *testing.T) {
	h := setup(t)
	sent := mcpThread(t, h, nil)
	h.linkUpdate("p1", sent.Task.ID, "state", "input-required", "text", "which branch?")
	if id := got(h.engine.TaskOpener(sent.Task.ID)).ok(t); id != "m-ask" {
		t.Errorf("opener = %q", id)
	}
	if id := got(h.engine.TaskOpener("nope")).ok(t); id != "" {
		t.Errorf("unknown task = %q", id)
	}
}

func TestUnreadTurnsFalseWhenTheMessageIsRead(t *testing.T) {
	h := setup(t)
	mcpThread(t, h, nil)
	if !got(h.engine.Unread("p1", "m-ask")).ok(t) {
		t.Fatal("new message is not unread")
	}
	got(h.engine.MarkRead("p1", []any{"m-ask"})).ok(t)
	if got(h.engine.Unread("p1", "m-ask")).ok(t) {
		t.Error("read message is unread")
	}
	if got(h.engine.Unread("hermes", "m-ask")).ok(t) {
		t.Error("a sent message is unread for its sender")
	}
}
