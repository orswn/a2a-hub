package mcp

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/jsonx"
)

const thread = "01a1db05-1487-7dd3-8786-1b1ca9c848da"

func codexClient(h *env) *client {
	c := h.client("mars-token", HarnessCodex)
	c.initialize()
	return c
}

func TestACodexCallRegistersItsThreadAsAnOfflineCodexSession(t *testing.T) {
	h := setup(t)
	c := codexClient(h)
	if text, isError := c.call("peers", args()); !isError || !strings.Contains(text, "_meta.threadId") {
		t.Errorf("call without thread id: %v %q", isError, text)
	}
	c.ok("set_status", args("text", "reading"), "threadId", thread, "sessionId", thread)
	row, err := h.e.Agent(thread)
	if err != nil || row == nil {
		t.Fatalf("agent: %v %v", row, err)
	}
	if row.Kind != engine.KindSession || row.Online != 0 || row.Name != "codex-c848da" || row.Labels != `{"harness":"codex"}` ||
		row.StatusText != "reading" {
		t.Errorf("row = %+v", row)
	}
	// The Codex adapter registers the same thread over the link API, with its own name, and the MCP calls keep it.
	if _, err := h.e.Register(mars, jsonx.NewObject("agent", thread, "protocol", float64(engine.HubProtocol), "name", "fix tests",
		"labels", jsonx.NewObject(engine.HarnessLabel, "codex"))); err != nil {
		t.Fatal(err)
	}
	c.ok("set_status", args("text", "testing"), "threadId", thread)
	if row, _ := h.e.Agent(thread); row.Name != "fix tests" || row.StatusText != "testing" {
		t.Errorf("after the adapter registered: %+v", row)
	}
	if h.srv.SessionCount() != 1 {
		t.Errorf("sessions = %d", h.srv.SessionCount())
	}
	if bound, _ := h.srv.Served(thread); bound {
		t.Error("a Codex thread is served by its MCP session")
	}
	other := h.client("venus-token", HarnessCodex)
	other.initialize()
	if text, isError := other.call("peers", args(), "threadId", thread); !isError || !strings.Contains(text, "is not bound to venus") {
		t.Errorf("thread of another host: %v %q", isError, text)
	}
}

func TestSendResolvesTheSupportedSelectors(t *testing.T) {
	h := setup(t)
	c := codexClient(h)
	call := func(target string) (string, bool) {
		return c.call("send", args("target", target, "text", "hi"), "threadId", thread)
	}
	h.register("p1", "topics", []any{"build"})
	h.register("p2", "name", "twin", "topics", []any{"build"})
	h.register("p3", "name", "twin")
	if err := h.e.EnsureExternal("hermes", "hermes"); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		target string
		want   string
		fail   bool
	}{
		{"p1", "queued: p1 (id:p1, offline; it gets the message when it connects)", false},
		{"id:p1", "queued: p1 (id:p1,", false},
		{"name:p1", "queued: p1 (id:p1,", false},
		{"twin", "twin matches 2 agents: id:p2, id:id:p3", true},
		{"id:nope", "No agent has id nope. Nothing was sent.", true},
		{"nobody", "No agent is named nobody. Use peers to find agents. Nothing was sent.", true},
		{"topic:build", "No online agent follows topic build. Nothing was sent.", true},
		{"role:x", "target role:x is not supported here. Use id:<id>, name:<name>, or topic:<topic>.", true},
		{"p1|p2", "is not supported here", true},
		{"id:" + thread, "No agent has id " + thread, true},
	}
	cases[3].want = "twin matches 2 agents: id:p2, id:p3. Use id:<id>. Nothing was sent."
	for _, c := range cases {
		text, isError := call(c.target)
		if isError != c.fail || !strings.Contains(text, c.want) {
			t.Errorf("%s: %v %q, want %q", c.target, isError, text, c.want)
		}
	}
	h.online("p1")
	h.online("p2")
	h.online("p3")
	if text, isError := c.call("send", args("target", "topic:build", "text", "hi", "wake", "never"), "threadId", thread); isError ||
		!strings.Contains(text, "p1 (id:p1") || !strings.Contains(text, "twin (id:p2") || strings.Contains(text, "id:p3") {
		t.Errorf("topic: %v %q", isError, text)
	}
}

var sendHead = regexp.MustCompile(`^Sent to (\S+) on thread (\S+) \(messageId (\S+)\)\.\n`)

func TestSendWordingPinsEachDeliveryState(t *testing.T) {
	h := setup(t)
	if err := h.e.EnsureExternal("hermes", "hermes"); err != nil {
		t.Fatal(err)
	}
	h.register("pi-1")
	claude := claudeClient(h, "claude-1")
	claude.initialize()
	codex := codexClient(h)
	send := func(target string, extra ...any) string {
		t.Helper()
		text, _ := codex.call("send", args(append([]any{"target", target, "text", "hi"}, extra...)...), "threadId", thread)
		return text
	}
	check := func(label, text, want string) {
		t.Helper()
		match := sendHead.FindStringSubmatch(text)
		if match == nil {
			t.Fatalf("%s: %q", label, text)
		}
		if rest := text[len(match[0]):]; rest != want {
			t.Errorf("%s\n got %q\nwant %q", label, rest, want)
		}
	}
	check("offline session", send("pi-1"), "queued: pi-1 (id:pi-1, offline; it gets the message when it connects)")
	check("claude without stream", send("claude-1"), "queued: claude-3f2a9c (id:claude-1, offline; it gets the message when it connects)")
	stream := claude.open()
	eventually(t, "online", func() bool { row, _ := h.e.Agent("claude-1"); return row.Online == 1 })
	check("claude push", send("claude-1"), "unconfirmed: claude-3f2a9c (id:claude-1, pushed to its session, which does not confirm)")
	check("claude wake never", send("claude-1", "wake", "never"), "queued: claude-3f2a9c (id:claude-1, stored in the hub inbox of claude-3f2a9c)")
	check("external task", send("hermes", "kind", "task"), "delivered: hermes (id:hermes)")
	text, isError := codex.call("send", args("target", "hermes", "text", "hi"), "threadId", thread)
	if !isError {
		t.Errorf("a note to an external agent outside an MCP thread was taken: %q", text)
	}
	check("failed", text, "failed: hermes (id:hermes, The hub refused: external agents take only task, question, or correction messages)")
	stream.close()
}

func TestSendWaitGivesAReplyThatCommitsWithTheEndOfItsTask(t *testing.T) {
	h, c := hermesSetup(t, nil)
	done := false
	betweenWaitReads = func() {
		owed, _ := h.e.Owed("p1")
		if done || len(owed.Tasks) != 1 {
			return
		}
		done = true
		_, _ = h.e.LinkUpdate("p1", owed.Tasks[0].TaskID, jsonx.NewObject("state", "completed", "text", "done: 42"))
	}
	t.Cleanup(func() { betweenWaitReads = func() {} })
	text := c.ok("send", args("target", "p1", "text", "compute", "kind", "task", "wait", 10))
	mustContain(t, "wait", text, "\n\nFirst reply:\nmessageId=", "kind=result", "\ndone: 42")
}

func TestSendWaitReturnsTheFirstReplyAndMarksItRead(t *testing.T) {
	h, c := hermesSetup(t, nil)
	go func() {
		eventually(t, "task", func() bool { owed, _ := h.e.Owed("p1"); return len(owed.Tasks) == 1 })
		owed, _ := h.e.Owed("p1")
		_, _ = h.e.LinkUpdate("p1", owed.Tasks[0].TaskID, jsonx.NewObject("state", "working", "text", "on it"))
		_, _ = h.e.LinkUpdate("p1", owed.Tasks[0].TaskID, jsonx.NewObject("state", "completed", "text", "done: 42"))
	}()
	text := c.ok("send", args("target", "p1", "text", "compute", "kind", "task", "wait", 10))
	mustContain(t, "wait", text, "\n\nFirst reply:\nmessageId=", "from=p1 (session, id:p1)", "kind=result", "\ndone: 42")
	if inbox := c.ok("inbox", args()); strings.Contains(inbox, "done: 42") || !strings.Contains(inbox, "kind=status") {
		t.Errorf("inbox after wait = %q", inbox)
	}
	start := time.Now()
	text = c.ok("send", args("target", "p1", "text", "again", "kind", "task", "wait", 1))
	if !strings.HasSuffix(text, "\n\nNo reply yet; it will arrive in your inbox.") || time.Since(start) < time.Second {
		t.Errorf("timeout: %q after %v", text, time.Since(start))
	}
	if text, isError := c.call("send", args("target", "p1", "text", "x", "wait", 31)); !isError ||
		text != "wait must be an integer from 0 to 30." {
		t.Errorf("wait 31: %q", text)
	}
}

func TestSendWaitEndsOnAFinalStateWithoutText(t *testing.T) {
	h, c := hermesSetup(t, nil)
	go func() {
		eventually(t, "task", func() bool { owed, _ := h.e.Owed("p1"); return len(owed.Tasks) == 1 })
		owed, _ := h.e.Owed("p1")
		_, _ = h.e.LinkUpdate("p1", owed.Tasks[0].TaskID, jsonx.NewObject("state", "failed"))
	}()
	text := c.ok("send", args("target", "p1", "text", "compute", "kind", "task", "wait", 10))
	if !regexp.MustCompile(`\n\nTask \S+ is failed with no reply text\.$`).MatchString(text) {
		t.Errorf("wait = %q", text)
	}
}

// Claude Code and Codex get each reply as a push or a wake, so a wait would give them the reply twice.
func TestOnlyHermesIsOfferedAndAllowedSendWait(t *testing.T) {
	h, hermes := hermesSetup(t, nil)
	hasWait := func(c *client) bool {
		got := c.rpc("tools/list", nil)
		result, _ := got.body.Object("result")
		return strings.Contains(string(jsonx.Stringify(result)), `"wait":`)
	}
	if !hasWait(hermes) {
		t.Error("hermes has no wait")
	}
	codex := codexClient(h)
	claude := claudeClient(h, "claude-1")
	claude.initialize()
	for name, c := range map[string]*client{"codex": codex, "claude": claude} {
		if hasWait(c) {
			t.Errorf("%s is offered wait", name)
		}
		for _, wait := range []int{5, 0} {
			text, isError := c.call("send", args("target", "p1", "text", "x", "wait", wait), "threadId", thread)
			if !isError || text != "wait is not available here. A reply wakes you and arrives in your inbox. Nothing was sent." {
				t.Errorf("%s send with wait %d: %v %q", name, wait, isError, text)
			}
		}
	}
	if inbox, _ := h.e.Inbox("p1", false, 10); len(inbox) != 0 {
		t.Errorf("p1 got %d messages", len(inbox))
	}
	for name, c := range map[string]*client{"codex": codex, "claude": claude} {
		if text, isError := c.call("send", args("target", "p1", "text", "x", "wait", jsonx.Null), "threadId", thread); isError {
			t.Errorf("%s send with wait null, which means no wait: %q", name, text)
		}
	}
}

func TestReplyToATaskCompletesItAndReplyToANoteAnswersTheSender(t *testing.T) {
	h := setup(t)
	h.register("p1")
	c := codexClient(h)
	c.ok("peers", args(), "threadId", thread)
	task := h.sendFrom("p1", thread, "m-task", "kind", "task")
	text := c.ok("reply", args("messageId", "m-task", "text", "all green"), "threadId", thread)
	mustContain(t, "task reply", text, "Replied to p1 on thread", "queued: p1 (id:p1, task completed)")
	state, _ := h.e.TaskState(task.Task.ID)
	if state != engine.StateCompleted {
		t.Errorf("state = %s", state)
	}
	h.sendFrom("p1", thread, "m-note", "kind", "note", "contextId", "ctx-note")
	text = c.ok("reply", args("messageId", "m-note", "text", "thanks"), "threadId", thread)
	mustContain(t, "note reply", text, "Replied to p1 on thread ctx-note", "queued: p1 (id:p1, offline;")
	inbox, _ := h.e.Inbox("p1", true, 1)
	if len(inbox) != 1 || inbox[0].Text != "thanks" || inbox[0].ReplyTo == nil || *inbox[0].ReplyTo != "m-note" || inbox[0].Kind != "answer" {
		t.Errorf("p1 inbox = %+v", inbox)
	}
	if text, isError := c.call("reply", args("messageId", inbox[0].MessageID, "text", "x"), "threadId", thread); !isError ||
		!strings.Contains(text, "was sent by you") {
		t.Errorf("reply to own message: %q", text)
	}
	if text, isError := c.call("reply", args("messageId", "nope", "text", "x"), "threadId", thread); !isError || text != "No message nope." {
		t.Errorf("unknown message: %q", text)
	}
}

func TestReplyToAQuestionOnATaskThatThisAgentSentContinuesTheTask(t *testing.T) {
	h := setup(t)
	h.register("p1")
	c := codexClient(h)
	c.ok("send", args("target", "p1", "text", "deploy", "kind", "task"), "threadId", thread)
	owed, _ := h.e.Owed("p1")
	taskID := owed.Tasks[0].TaskID
	if _, err := h.e.LinkUpdate("p1", taskID, jsonx.NewObject("state", "input-required", "text", "which env?")); err != nil {
		t.Fatal(err)
	}
	inbox := c.ok("inbox", args(), "threadId", thread)
	id := regexp.MustCompile(`messageId=(\S+)`).FindStringSubmatch(inbox)[1]
	c.ok("reply", args("messageId", id, "text", "staging"), "threadId", thread)
	state, _ := h.e.TaskState(taskID)
	if state != engine.StateWorking {
		t.Errorf("state = %s", state)
	}
}

func TestInboxShowsOldestFirstAndMarksShownMessagesRead(t *testing.T) {
	h := setup(t)
	h.register("p1")
	c := codexClient(h)
	c.ok("peers", args(), "threadId", thread)
	h.sendFrom("p1", thread, "m-1", "subject", "first", "contextId", "ctx-a")
	h.sendFrom("p1", thread, "m-2", "data", jsonx.NewObject("n", float64(1)), "contextId", "ctx-a")
	text := c.ok("inbox", args(), "threadId", thread)
	parts := strings.Split(text, "\n\n---\n\n")
	if len(parts) != 2 {
		t.Fatalf("inbox = %q", text)
	}
	if !regexp.MustCompile(`^messageId=m-1 threadId=ctx-a\nfrom=p1 \(session, id:p1\)\nkind=note at=\S+\nsubject: first\ntext of m-1$`).MatchString(parts[0]) {
		t.Errorf("first = %q", parts[0])
	}
	mustContain(t, "second", parts[1], "messageId=m-2", "text of m-2\ndata: {\"n\":1}")
	if again := c.ok("inbox", args(), "threadId", thread); again != "No unread messages." {
		t.Errorf("again = %q", again)
	}
	if all := c.ok("inbox", args("unreadOnly", false, "limit", 1), "threadId", thread); !strings.HasPrefix(all, "messageId=m-2") {
		t.Errorf("limit 1 = %q", all)
	}
	if one := c.ok("inbox", args("messageId", "m-1"), "threadId", thread); !strings.HasPrefix(one, "messageId=m-1") {
		t.Errorf("by id = %q", one)
	}
	if whole := c.ok("inbox", args("threadId", "ctx-a"), "threadId", thread); strings.Count(whole, "messageId=") != 2 {
		t.Errorf("thread = %q", whole)
	}
	if text, isError := c.call("inbox", args("limit", 0), "threadId", thread); !isError || text != "limit must be an integer from 1 to 50." {
		t.Errorf("limit 0 = %q", text)
	}
}

func TestPeersListsOtherOnlineAgentsAndFilters(t *testing.T) {
	h := setup(t)
	h.register("p1", "cwd", "/src/a", "topics", []any{"build"})
	h.register("p2")
	h.online("p1")
	c := codexClient(h)
	if _, err := h.e.SetStatus("p1", "compiling"); err != nil {
		t.Fatal(err)
	}
	text := c.ok("peers", args(), "threadId", thread)
	if text != `p1 id:p1 kind=session harness=pi idle host=mars status="compiling" cwd=/src/a` {
		t.Errorf("peers = %q", text)
	}
	if text := c.ok("peers", args("filter", "BUILD"), "threadId", thread); !strings.HasPrefix(text, "p1 ") {
		t.Errorf("filter = %q", text)
	}
	if text := c.ok("peers", args("filter", "zzz"), "threadId", thread); text != "No online agent matches zzz." {
		t.Errorf("no match = %q", text)
	}
	if text := c.ok("set_status", args("text", ""), "threadId", thread); text != "Status cleared." {
		t.Errorf("clear = %q", text)
	}
}

func TestUnknownToolIsAProtocolError(t *testing.T) {
	h := setup(t)
	c := codexClient(h)
	got := c.rpc("tools/call", jsonx.NewObject("name", "nope", "arguments", jsonx.NewObject()))
	errObj, _ := got.body.Object("error")
	if errObj.Value("code") != float64(-32602) {
		t.Errorf("unknown tool: %s", jsonx.Stringify(got.body))
	}
}
