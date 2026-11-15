package mcp

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/orswn/a2a-hub/internal/a2aapi"
	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/jsonx"
)

func claudeClient(h *env, agent string) *client {
	return h.client("mars-token", HarnessClaude, AgentHeader, agent, SessionHeader, "3f2a9c1e-0000-4000-8000-000000000000",
		CwdHeader, "/home/u/work%20dir")
}

func TestServerDiscoverGetsMethodNotFoundInHTTP200SoClaudeFallsBackToInitialize(t *testing.T) {
	h := setup(t)
	c := h.client("mars-token", HarnessClaude, AgentHeader, "claude-1", "Mcp-Method", "server/discover",
		"Mcp-Protocol-Version", "2026-07-28")
	got := c.rpc("server/discover", jsonx.NewObject("_meta", jsonx.NewObject("io.modelcontextprotocol/protocolVersion", "2026-07-28")))
	if got.status != http.StatusOK {
		t.Fatalf("status = %d", got.status)
	}
	errObj, _ := got.body.Object("error")
	if code := errObj.Value("code"); code != float64(-32601) {
		t.Errorf("error = %s", jsonx.Stringify(got.body))
	}
	if got.header.Get(SessionIDHeader) != "" {
		t.Error("the probe made a session")
	}
	delete(c.headers, "Mcp-Method")
	delete(c.headers, "Mcp-Protocol-Version")
	result := c.initialize()
	if result.Value("protocolVersion") != "2025-11-25" {
		t.Errorf("version = %v", result.Value("protocolVersion"))
	}
}

func TestInitializeOffersTheClaudeChannelOnlyToClaudeCode(t *testing.T) {
	h := setup(t)
	claude := claudeClient(h, "claude-mars-4242-99").initialize()
	capabilities, _ := claude.Object("capabilities")
	equalJSON(t, "claude capabilities", capabilities, `{"tools":{},"experimental":{"claude/channel":{}}}`)
	if text, _ := claude.String("instructions"); text != Instructions {
		t.Errorf("instructions = %q", text)
	}
	codex := h.client("mars-token", HarnessCodex)
	got := codex.rpc("initialize", jsonx.NewObject("protocolVersion", "2025-06-18"))
	result, _ := got.body.Object("result")
	capabilities, _ = result.Object("capabilities")
	equalJSON(t, "codex capabilities", capabilities, `{"tools":{}}`)
	if result.Value("protocolVersion") != "2025-06-18" {
		t.Errorf("codex version = %v", result.Value("protocolVersion"))
	}
	unknown := h.client("mars-token", HarnessCodex).rpc("initialize", jsonx.NewObject("protocolVersion", "2099-01-01"))
	result, _ = unknown.body.Object("result")
	if result.Value("protocolVersion") != ProtocolVersion {
		t.Errorf("unknown version answered with %v", result.Value("protocolVersion"))
	}
}

func equalJSON(t *testing.T, label string, got any, want string) {
	t.Helper()
	if text := string(jsonx.Stringify(got)); text != want {
		t.Errorf("%s = %s, want %s", label, text, want)
	}
}

func TestInitializeRefusesAMissingOrUnknownHarness(t *testing.T) {
	h := setup(t)
	for _, harness := range []string{"", "pi", "Claude-Code"} {
		got := h.client("mars-token", harness).rpc("initialize", nil)
		if got.status != http.StatusBadRequest || got.header.Get(SessionIDHeader) != "" {
			t.Errorf("harness %q: status %d", harness, got.status)
		}
	}
	if got := h.client("mars-token", HarnessClaude).rpc("initialize", nil); got.status != http.StatusBadRequest {
		t.Errorf("claude without agent: %d", got.status)
	}
	if got := h.client("hermes-token", HarnessCodex).rpc("initialize", nil); got.status != http.StatusForbidden {
		t.Errorf("codex with a peer token: %d", got.status)
	}
	if got := h.client("hermes-token", HarnessHermes).rpc("initialize", nil); got.status != http.StatusForbidden {
		t.Errorf("hermes without its external agent: %d", got.status)
	}
	if got := h.client("mars-token", HarnessHermes).rpc("initialize", nil); got.status != http.StatusForbidden {
		t.Errorf("hermes with a host token that names no external agent: %d", got.status)
	}
}

func TestClaudeInitializeRegistersASessionAgentOfTheHost(t *testing.T) {
	h := setup(t)
	claudeClient(h, "claude-mars-4242-99").initialize()
	row, err := h.e.Agent("claude-mars-4242-99")
	if err != nil || row == nil {
		t.Fatalf("agent: %v %v", row, err)
	}
	if row.Kind != engine.KindSession || *row.Host != "mars" || row.Name != "claude-3f2a9c" || row.Online != 0 ||
		*row.Cwd != "/home/u/work dir" {
		t.Errorf("row = %+v", row)
	}
	if row.Labels != `{"harness":"claude-code","session":"3f2a9c1e-0000-4000-8000-000000000000"}` {
		t.Errorf("labels = %s", row.Labels)
	}
	// After /clear the session id changes. Peers must keep the name that they know.
	h.client("mars-token", HarnessClaude, AgentHeader, "claude-mars-4242-99", SessionHeader, "bbbbbb11-0000-4000-8000-000000000000").initialize()
	if row, _ := h.e.Agent("claude-mars-4242-99"); row.Name != "claude-3f2a9c" {
		t.Errorf("name after a new session id = %s", row.Name)
	}
	h.client("mars-token", HarnessClaude, AgentHeader, "claude-mars-5151-2").initialize()
	if row, _ := h.e.Agent("claude-mars-5151-2"); row.Name != "claude-5151" {
		t.Errorf("fallback name = %s", row.Name)
	}
	if got := h.client("venus-token", HarnessClaude, AgentHeader, "claude-mars-4242-99").rpc("initialize", nil); got.status != http.StatusForbidden {
		t.Errorf("other host: %d", got.status)
	}
}

func TestRequestsNeedTheSessionOfTheSamePrincipal(t *testing.T) {
	h := setup(t)
	c := h.client("mars-token", HarnessCodex)
	if got := c.rpc("tools/list", nil); got.status != http.StatusBadRequest {
		t.Errorf("no session: %d", got.status)
	}
	c.session = "nope"
	if got := c.rpc("tools/list", nil); got.status != http.StatusNotFound {
		t.Errorf("unknown session: %d", got.status)
	}
	c.session = ""
	c.initialize()
	if got := c.rpc("tools/list", nil); got.status != http.StatusOK {
		t.Errorf("own session: %d", got.status)
	}
	other := h.client("venus-token", HarnessCodex)
	other.session = c.session
	if got := other.rpc("tools/list", nil); got.status != http.StatusNotFound {
		t.Errorf("session of another principal: %d", got.status)
	}
	notification := c.do(http.MethodPost, []byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`))
	if notification.status != http.StatusAccepted || notification.body != nil {
		t.Errorf("notification: %d", notification.status)
	}
	if got := c.rpc("ping", nil); got.status != http.StatusOK || string(jsonx.Stringify(got.body.Value("result"))) != "{}" {
		t.Errorf("ping: %d", got.status)
	}
	unknown := c.rpc("resources/list", nil)
	errObj, _ := unknown.body.Object("error")
	if errObj.Value("code") != float64(-32601) {
		t.Errorf("unknown method: %s", jsonx.Stringify(unknown.body))
	}
	for body, code := range map[string]float64{`[{"jsonrpc":"2.0","id":1,"method":"ping"}]`: -32600, `{"jsonrpc":`: -32700} {
		got := c.do(http.MethodPost, []byte(body))
		errObj, _ := got.body.Object("error")
		if got.status != http.StatusBadRequest || errObj.Value("code") != code {
			t.Errorf("%s: %d %s", body, got.status, jsonx.Stringify(got.body))
		}
	}
}

func TestClaudeGetStreamOwnsPresenceAndSendsHeartbeats(t *testing.T) {
	h := setup(t, envOptions{grace: 100 * time.Millisecond, heartbeat: 20 * time.Millisecond})
	c := claudeClient(h, "claude-1")
	c.initialize()
	s := c.open()
	if s.status != http.StatusOK {
		t.Fatalf("GET = %d", s.status)
	}
	s.waitFor(": hb")
	if row, _ := h.e.Agent("claude-1"); row.Online != 1 {
		t.Error("offline with an open stream")
	}
	s.close()
	eventually(t, "offline after the grace", func() bool {
		row, _ := h.e.Agent("claude-1")
		return row.Online == 0
	})
	eventually(t, "session freed after the grace", func() bool { return h.srv.SessionCount() == 0 })
	if got := c.rpc("tools/list", nil); got.status != http.StatusNotFound {
		t.Errorf("request after the session ended: %d", got.status)
	}
}

func TestCodexAndHermesSessionsGetNoPushStream(t *testing.T) {
	h := setup(t)
	codex := h.client("mars-token", HarnessCodex)
	codex.initialize()
	if s := codex.open(); s.status != http.StatusMethodNotAllowed {
		t.Errorf("codex GET = %d", s.status)
	}
	if err := h.e.EnsureExternal("hermes", "hermes"); err != nil {
		t.Fatal(err)
	}
	herm := h.client("hermes-token", HarnessHermes)
	herm.initialize()
	if s := herm.open(); s.status != http.StatusMethodNotAllowed {
		t.Errorf("hermes GET = %d", s.status)
	}
}

func TestANewerClaudeSessionOfTheSameAgentTakesThePushStream(t *testing.T) {
	h := setup(t)
	first := claudeClient(h, "claude-1")
	first.initialize()
	firstStream := first.open()
	eventually(t, "online", func() bool { row, _ := h.e.Agent("claude-1"); return row.Online == 1 })
	second := claudeClient(h, "claude-1")
	second.initialize()
	if s := first.open(); s.status != http.StatusMethodNotAllowed {
		t.Errorf("older session GET = %d", s.status)
	}
	secondStream := second.open()
	firstStream.waitClosed()
	h.register("p1")
	h.online("p1")
	h.sendFrom("p1", "claude-1", "m-1", "wake", "now")
	secondStream.waitFor(`"message_id":"m-1"`)
	if text := first.ok("peers", args()); !strings.Contains(text, "p1") {
		t.Errorf("older session tools: %q", text)
	}
}

func TestDeleteEndsTheSessionAndItsPresence(t *testing.T) {
	h := setup(t)
	c := claudeClient(h, "claude-1")
	c.initialize()
	s := c.open()
	eventually(t, "online", func() bool { row, _ := h.e.Agent("claude-1"); return row.Online == 1 })
	if got := c.do(http.MethodDelete, nil); got.status != http.StatusOK {
		t.Fatalf("DELETE = %d", got.status)
	}
	s.waitClosed()
	if row, _ := h.e.Agent("claude-1"); row.Online != 0 {
		t.Error("online after DELETE")
	}
	if got := c.rpc("tools/list", nil); got.status != http.StatusNotFound {
		t.Errorf("after DELETE: %d", got.status)
	}
}

func TestAnUnusedSessionWithoutAStreamExpires(t *testing.T) {
	h := setup(t, envOptions{grace: time.Minute, heartbeat: time.Minute, idle: 50 * time.Millisecond})
	c := h.client("mars-token", HarnessCodex)
	c.initialize()
	eventually(t, "expired", func() bool { return h.srv.SessionCount() == 0 })
}

func TestAWakeNowMessageBecomesOneChannelNotificationThatIsAckedOnWrite(t *testing.T) {
	h := setup(t)
	h.register("p1")
	c := claudeClient(h, "claude-1")
	c.initialize()
	s := c.open()
	eventually(t, "online", func() bool { row, _ := h.e.Agent("claude-1"); return row.Online == 1 })
	result := h.sendFrom("p1", "claude-1", "m-now", "kind", "task", "wake", "now")
	target, _ := h.e.Agent("claude-1")
	start := time.Now()
	if delivery := a2aapi.Delivery(context.Background(), h.e, target, result, h.srv); delivery != a2aapi.Unconfirmed {
		t.Errorf("delivery = %s", delivery)
	}
	if time.Since(start) > a2aapi.DeliveryWait/2 {
		t.Error("the delivery waited for an ack")
	}
	frame := s.waitFor(`"message_id":"m-now"`)
	want := `event: message` + "\n" + `data: {"jsonrpc":"2.0","method":"notifications/claude/channel","params":{"content":"p1: text of m-now",` +
		`"meta":{"message_id":"m-now","sender_kind":"session","kind":"task"}}}` + "\n"
	if frame != want {
		t.Errorf("frame\n got %q\nwant %q", frame, want)
	}
	eventually(t, "acked", func() bool { unacked, _ := h.e.Unacked("claude-1"); return len(unacked) == 0 })
	never := h.sendFrom("p1", "claude-1", "m-never", "wake", "never")
	if delivery := a2aapi.Delivery(context.Background(), h.e, target, never, h.srv); delivery != a2aapi.Queued {
		t.Errorf("wake never delivery = %s", delivery)
	}
	h.sendFrom("p1", "claude-1", "m-after", "wake", "now")
	s.waitFor(`"message_id":"m-after"`)
	if s.has("m-never") {
		t.Error("a wake never message was pushed")
	}
	s.close()
	again := c.open()
	h.sendFrom("p1", "claude-1", "m-later", "wake", "now")
	again.waitFor(`"message_id":"m-later"`)
	if again.has("m-now") || again.has("m-after") {
		t.Error("the new stream repeated a pushed message")
	}
}

func TestAPushWaitsForTheNextStreamAndSkipsWhatTheModelAlreadyRead(t *testing.T) {
	h := setup(t)
	h.register("p1")
	c := claudeClient(h, "claude-1")
	c.initialize()
	result := h.sendFrom("p1", "claude-1", "m-queued", "wake", "now")
	target, _ := h.e.Agent("claude-1")
	if delivery := a2aapi.Delivery(context.Background(), h.e, target, result, h.srv); delivery != a2aapi.Queued {
		t.Errorf("delivery without a stream = %s", delivery)
	}
	h.sendFrom("p1", "claude-1", "m-read", "wake", "now")
	c.ok("inbox", args("messageId", "m-read"))
	s := c.open()
	s.waitFor(`"message_id":"m-queued"`)
	// Frames come in order, so the marker comes after any replay of m-read.
	h.sendFrom("p1", "claude-1", "m-marker", "wake", "now")
	s.waitFor(`"message_id":"m-marker"`)
	if s.has("m-read") {
		t.Error("a read message was pushed")
	}
}

func TestATaskResultBecomesAChannelNotification(t *testing.T) {
	h := setup(t)
	h.register("p1")
	c := claudeClient(h, "claude-1")
	c.initialize()
	s := c.open()
	sent := c.ok("send", args("target", "p1", "text", "build it", "kind", "task"))
	pending, _ := h.e.Owed("p1")
	if len(pending.Tasks) != 1 {
		t.Fatalf("owed = %+v, send = %q", pending, sent)
	}
	taskID := pending.Tasks[0].TaskID
	if _, err := h.e.LinkUpdate("p1", taskID, jsonx.NewObject("state", "working", "text", "on it")); err != nil {
		t.Fatal(err)
	}
	if _, err := h.e.LinkUpdate("p1", taskID, jsonx.NewObject("state", "completed", "text", "built")); err != nil {
		t.Fatal(err)
	}
	frame := s.waitFor(`"kind":"result"`)
	mustContain(t, "result frame", frame, `"content":"p1: built"`, `"sender_kind":"session"`)
	if strings.Contains(frame, "task_id") || strings.Contains(frame, taskID) {
		t.Errorf("the frame names the task, which no tool takes: %q", frame)
	}
	if s.has("on it") {
		t.Error("a progress note was pushed")
	}
}

func TestAReconnectOnTheSameSessionKeepsThePushStream(t *testing.T) {
	h := setup(t, envOptions{grace: 300 * time.Millisecond, heartbeat: time.Minute})
	h.register("p1")
	c := claudeClient(h, "claude-1")
	c.initialize()
	first := c.open()
	eventually(t, "online", func() bool { row, _ := h.e.Agent("claude-1"); return row.Online == 1 })
	second := c.open()
	if second.status != http.StatusOK {
		t.Fatalf("second GET = %d", second.status)
	}
	first.waitClosed()
	// Tool calls within the grace restart the session timer. The session must stay alive and keep its stream.
	for range 5 {
		c.ok("peers", args())
		time.Sleep(100 * time.Millisecond)
	}
	if h.srv.SessionCount() != 1 {
		t.Fatalf("session freed while its stream is open")
	}
	second.close()
	third := c.open()
	if third.status != http.StatusOK {
		t.Fatalf("reconnect GET = %d", third.status)
	}
	eventually(t, "online", func() bool { row, _ := h.e.Agent("claude-1"); return row.Online == 1 })
	result := h.sendFrom("p1", "claude-1", "m-x", "wake", "now")
	target, _ := h.e.Agent("claude-1")
	if delivery := a2aapi.Delivery(context.Background(), h.e, target, result, h.srv); delivery != a2aapi.Unconfirmed {
		t.Errorf("delivery after the reconnects = %s", delivery)
	}
	third.waitFor(`"message_id":"m-x"`)
}

func TestASessionWithAnOpenStreamOutlivesTheGraceOfItsReplacedStream(t *testing.T) {
	h := setup(t, envOptions{grace: 200 * time.Millisecond, heartbeat: time.Minute})
	c := claudeClient(h, "claude-1")
	c.initialize()
	first := c.open()
	eventually(t, "online", func() bool { row, _ := h.e.Agent("claude-1"); return row.Online == 1 })
	second := c.open()
	first.waitClosed()
	time.Sleep(400 * time.Millisecond)
	if h.srv.SessionCount() != 1 {
		t.Errorf("session freed while its stream is open")
	}
	second.close()
	if third := c.open(); third.status != http.StatusOK {
		t.Errorf("reconnect GET = %d", third.status)
	}
}
