package mcp

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/external"
	"github.com/orswn/a2a-hub/internal/jsonx"
)

// vector is a request that the Hermes webhook platform accepted, recorded from the Hermes source.
type vector struct {
	Secret    string            `json:"secret"`
	Timestamp string            `json:"timestamp"`
	Body      string            `json:"body"`
	Headers   map[string]string `json:"headers"`
}

func readVector(t *testing.T) vector {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "hermes-webhook-vector.json"))
	if err != nil {
		t.Fatal(err)
	}
	var v vector
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestTheWebhookSignatureAndBodyMatchTheRecordedHermesVector(t *testing.T) {
	v := readVector(t)
	if got := Sign(v.Secret, v.Timestamp, []byte(v.Body)); got != v.Headers["X-Webhook-Signature-V2"] {
		t.Errorf("signature = %s", got)
	}
	taskID, replyTo := "task-1", "01HERMESSEND00000000000000"
	item := engine.InboxItem{
		MessageID: "01HUBMSG0000000000000VECTOR", TaskID: &taskID, ContextID: "01HUBTHREAD000000000000000",
		From: engine.InboxFrom{Name: "mars", Kind: engine.KindSession, Agent: "01a1b7750715"}, Kind: "result",
		Text: "Build passed on mars ✔", TaskState: "completed",
	}
	origin := jsonx.NewObject("platform", "telegram", "chat_id", "-100123", "thread_id", "42")
	if body := string(WebhookBody(item, origin, "pi", replyTo)); body != v.Body {
		t.Errorf("body\n got %s\nwant %s", body, v.Body)
	}
}

func TestTheWebhookBodyOriginIsAlwaysThreeStrings(t *testing.T) {
	item := engine.InboxItem{MessageID: "m", ContextID: "c", Kind: "answer", Text: "t"}
	cases := []struct {
		origin *jsonx.Object
		want   string
	}{
		{nil, `{"platform":"","chat_id":"","thread_id":""}`},
		{jsonx.NewObject("platform", "telegram", "chat_id", "-1", "thread_id", jsonx.Null), `{"platform":"telegram","chat_id":"-1","thread_id":""}`},
	}
	for _, c := range cases {
		var body struct {
			Origin json.RawMessage `json:"origin"`
			TaskID string          `json:"task_id"`
			State  string          `json:"state"`
		}
		if err := json.Unmarshal(WebhookBody(item, c.origin, "", ""), &body); err != nil {
			t.Fatal(err)
		}
		if string(body.Origin) != c.want {
			t.Errorf("origin = %s, want %s", body.Origin, c.want)
		}
	}
}

type receiver struct {
	mu       sync.Mutex
	statuses []int
	requests []*http.Request
	bodies   []string
	times    []time.Time
	// hold, when set, keeps each answer back until it is closed.
	hold chan struct{}
	got  chan struct{}
	url  string
}

// newReceiver answers each post with the next status, then 200.
func newReceiver(t *testing.T, statuses ...int) *receiver {
	r := &receiver{statuses: statuses, got: make(chan struct{}, 64)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		r.requests = append(r.requests, req)
		r.bodies = append(r.bodies, string(body))
		r.times = append(r.times, time.Now())
		status := http.StatusOK
		if len(r.statuses) > 0 {
			status, r.statuses = r.statuses[0], r.statuses[1:]
		}
		hold := r.hold
		r.mu.Unlock()
		r.got <- struct{}{}
		if hold != nil {
			<-hold
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(server.Close)
	r.url = server.URL + "/webhooks/a2a-hub"
	return r
}

func (r *receiver) wait(t *testing.T, n int) {
	t.Helper()
	for {
		r.mu.Lock()
		count := len(r.requests)
		r.mu.Unlock()
		if count >= n {
			return
		}
		select {
		case <-r.got:
		case <-time.After(5 * time.Second):
			t.Fatalf("got %d posts, want %d", count, n)
		}
	}
}

func (r *receiver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.requests)
}

const secret = "s3cret"

var fixedNow = time.Unix(1_800_000_000, 0)

func hermesSetup(t *testing.T, hook *receiver, retryDelays ...time.Duration) (*env, *client) {
	t.Helper()
	h := setup(t, envOptions{grace: time.Minute, heartbeat: time.Minute, retryDelays: retryDelays, now: func() time.Time { return fixedNow }})
	if err := h.e.EnsureExternal("hermes", "hermes"); err != nil {
		t.Fatal(err)
	}
	if hook != nil {
		h.hooks.set("hermes", &external.Webhook{URL: hook.url, Secret: secret})
	}
	h.register("p1", "labels", jsonx.NewObject(engine.HarnessLabel, "pi"))
	c := h.client("hermes-token", HarnessHermes)
	c.initialize()
	return h, c
}

func telegram() *jsonx.Object {
	return jsonx.NewObject("platform", "telegram", "chat_id", "-100123", "thread_id", "42")
}

func sentIDs(t *testing.T, text string) (messageID, contextID string) {
	t.Helper()
	match := sendHead.FindStringSubmatch(text)
	if match == nil {
		t.Fatalf("send = %q", text)
	}
	return match[3], match[2]
}

func TestAReplyToAHermesSendIsPostedToItsWebhookWithTheSendOrigin(t *testing.T) {
	hook := newReceiver(t)
	h, c := hermesSetup(t, hook)
	sent := c.ok("send", args("target", "p1", "text", "run the build", "kind", "task", "origin", telegram()))
	messageID, contextID := sentIDs(t, sent)
	owed, _ := h.e.Owed("p1")
	if len(owed.Tasks) != 1 {
		t.Fatalf("owed = %+v", owed)
	}
	taskID := owed.Tasks[0].TaskID
	if _, err := h.e.LinkUpdate("p1", taskID, jsonx.NewObject("state", "working", "text", "building")); err != nil {
		t.Fatal(err)
	}
	if _, err := h.e.LinkUpdate("p1", taskID, jsonx.NewObject("state", "completed", "text", "Build passed")); err != nil {
		t.Fatal(err)
	}
	hook.wait(t, 1)
	request, body := hook.requests[0], hook.bodies[0]
	var got map[string]any
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	resultID, _ := got["message_id"].(string)
	want := `{"event":"a2a-hub.message","origin":{"platform":"telegram","chat_id":"-100123","thread_id":"42"},` +
		`"from":{"id":"p1","name":"p1","harness":"pi"},"kind":"result","message_id":"` + resultID + `","in_reply_to":"` + messageID +
		`","hub_thread_id":"` + contextID + `","task_id":"` + taskID + `","state":"completed","text":"Build passed"}`
	if body != want {
		t.Errorf("body\n got %s\nwant %s", body, want)
	}
	timestamp := strconv.FormatInt(fixedNow.Unix(), 10)
	headers := map[string]string{
		"Content-Type": "application/json", "X-Request-ID": resultID, "X-Webhook-Timestamp": timestamp,
		"X-Webhook-Signature-V2": Sign(secret, timestamp, []byte(body)),
	}
	for name, value := range headers {
		if request.Header.Get(name) != value {
			t.Errorf("%s = %q, want %q", name, request.Header.Get(name), value)
		}
	}
	if request.URL.Path != "/webhooks/a2a-hub" {
		t.Errorf("path = %s", request.URL.Path)
	}
	inbox := c.ok("inbox", args())
	mustContain(t, "hermes inbox", inbox, "Build passed", "building")
}

func TestAReplyMessageInAHermesThreadStaysInItsInboxAndIsPosted(t *testing.T) {
	hook := newReceiver(t)
	h, c := hermesSetup(t, hook)
	origin := jsonx.NewObject("platform", "telegram", "chat_id", "-5", "thread_id", jsonx.Null)
	sent := c.ok("send", args("target", "p1", "text", "hello", "origin", origin))
	messageID, _ := sentIDs(t, sent)
	h.sendFrom("p1", "hermes", "m-answer", "kind", "answer", "replyTo", messageID)
	hook.wait(t, 1)
	mustContain(t, "body", hook.bodies[0], `"origin":{"platform":"telegram","chat_id":"-5","thread_id":""}`,
		`"in_reply_to":"`+messageID+`"`, `"hub_thread_id":"`, `"task_id":"","state":""`)
	jobs, _ := h.e.ExternalJobs()
	if len(jobs) != 0 {
		t.Errorf("dispatched = %+v", jobs)
	}
	inbox := c.ok("inbox", args())
	mustContain(t, "inbox", inbox, "messageId=m-answer", "replyTo="+messageID)
}

func TestASendWithoutOriginPostsEmptyOriginStrings(t *testing.T) {
	hook := newReceiver(t)
	h, c := hermesSetup(t, hook)
	messageID, _ := sentIDs(t, c.ok("send", args("target", "p1", "text", "hello")))
	h.sendFrom("p1", "hermes", "m-answer", "kind", "answer", "replyTo", messageID)
	hook.wait(t, 1)
	mustContain(t, "body", hook.bodies[0], `"origin":{"platform":"","chat_id":"","thread_id":""}`)
}

func TestTheWebhookPostIsRetriedOnAServerErrorWithTheSameRequestID(t *testing.T) {
	hook := newReceiver(t, http.StatusBadGateway, http.StatusInternalServerError)
	h, c := hermesSetup(t, hook, time.Millisecond, time.Millisecond)
	messageID, _ := sentIDs(t, c.ok("send", args("target", "p1", "text", "hello")))
	h.sendFrom("p1", "hermes", "m-answer", "kind", "answer", "replyTo", messageID)
	hook.wait(t, 3)
	for _, request := range hook.requests {
		if request.Header.Get("X-Request-ID") != "m-answer" {
			t.Errorf("request id = %q", request.Header.Get("X-Request-ID"))
		}
	}
	if strings.Contains(h.logged(), "webhook post") {
		t.Errorf("logged = %q", h.logged())
	}
}

func TestTheWebhookPostStopsAfterThreeTriesOrARefusal(t *testing.T) {
	hook := newReceiver(t, 503, 503, 503, 401)
	h, c := hermesSetup(t, hook, time.Millisecond, time.Millisecond)
	messageID, _ := sentIDs(t, c.ok("send", args("target", "p1", "text", "hello")))
	h.sendFrom("p1", "hermes", "m-1", "kind", "answer", "replyTo", messageID)
	hook.wait(t, 3)
	eventually(t, "logged", func() bool {
		return strings.Contains(h.logged(), "m-1 for hermes failed: status 503; it stays in the inbox")
	})
	h.sendFrom("p1", "hermes", "m-2", "kind", "answer", "replyTo", messageID)
	eventually(t, "refusal logged", func() bool { return strings.Contains(h.logged(), "m-2 for hermes failed: status 401") })
	if hook.count() != 4 {
		t.Errorf("posts = %d", hook.count())
	}
	mustContain(t, "inbox", c.ok("inbox", args()), "messageId=m-1", "messageId=m-2")
}

func TestAWaitingSendTakesTheReplyAndNothingIsPosted(t *testing.T) {
	hook := newReceiver(t)
	h, c := hermesSetup(t, hook)
	go func() {
		eventually(t, "task", func() bool { owed, _ := h.e.Owed("p1"); return len(owed.Tasks) == 1 })
		owed, _ := h.e.Owed("p1")
		_, _ = h.e.LinkUpdate("p1", owed.Tasks[0].TaskID, jsonx.NewObject("state", "completed", "text", "quick answer"))
	}()
	text := c.ok("send", args("target", "p1", "text", "quick?", "kind", "question", "wait", 10, "origin", telegram()))
	mustContain(t, "wait", text, "First reply:", "quick answer")
	messageID, _ := sentIDs(t, text)
	h.sendFrom("p1", "hermes", "m-late", "kind", "answer", "replyTo", messageID)
	hook.wait(t, 1)
	if hook.count() != 1 || !strings.Contains(hook.bodies[0], "m-late") {
		t.Errorf("posts = %q", hook.bodies)
	}
}

func TestOriginIsIgnoredOutsideHermesAndChecksItsShape(t *testing.T) {
	hook := newReceiver(t)
	h, c := hermesSetup(t, hook)
	if text, isError := c.call("send", args("target", "p1", "text", "x", "origin", "telegram")); !isError || text != "origin must be an object." {
		t.Errorf("string origin = %q", text)
	}
	if text, isError := c.call("send", args("target", "p1", "text", "x", "origin", jsonx.NewObject("thread_id", float64(4)))); !isError ||
		text != "origin.thread_id must be a string or null." {
		t.Errorf("number thread = %q", text)
	}
	codex := codexClient(h)
	codex.ok("send", args("target", "p1", "text", "x", "origin", telegram()), "threadId", thread)
	inbox, _ := h.e.Inbox("p1", false, 1)
	if found, _ := h.e.MCPThread(thread, inbox[0].ContextID, ""); found == nil || found.Origin != nil {
		t.Errorf("codex send stored an origin: %+v", found)
	}
}

func TestAReplyToHermesInItsThreadIsQueuedInItsInbox(t *testing.T) {
	h, c := hermesSetup(t, nil)
	codex := codexClient(h)
	codex.ok("peers", args(), "threadId", thread)
	messageID, _ := sentIDs(t, c.ok("send", args("target", "id:"+thread, "text", "hello")))
	text := codex.ok("reply", args("messageId", messageID, "text", "hi back"), "threadId", thread)
	mustContain(t, "reply", text, "queued: hermes (id:hermes, stored in the hub inbox of hermes)")
	mustContain(t, "hermes inbox", c.ok("inbox", args()), "hi back")
	if text := codex.ok("inbox", args(), "threadId", thread); !strings.Contains(text, "No unread messages.") {
		t.Errorf("the answered message is still unread: %q", text)
	}
}

func TestAReplyThatComesDuringTheDeliveryWaitIsTakenByTheWaitingSend(t *testing.T) {
	hook := newReceiver(t)
	h, c := hermesSetup(t, hook)
	h.online("p1")
	go func() {
		eventually(t, "task", func() bool { owed, _ := h.e.Owed("p1"); return len(owed.Tasks) == 1 })
		owed, _ := h.e.Owed("p1")
		_, _ = h.e.LinkUpdate("p1", owed.Tasks[0].TaskID, jsonx.NewObject("state", "completed", "text", "fast answer"))
	}()
	text := c.ok("send", args("target", "p1", "text", "quick?", "kind", "question", "wait", 10))
	mustContain(t, "wait", text, "unconfirmed: p1", "First reply:", "fast answer")
	messageID, _ := sentIDs(t, text)
	h.sendFrom("p1", "hermes", "m-late", "kind", "answer", "replyTo", messageID)
	hook.wait(t, 1)
	if hook.count() != 1 || !strings.Contains(hook.bodies[0], "m-late") {
		t.Errorf("posts = %q", hook.bodies)
	}
}

func TestTheWebhookPostDoesNotFollowARedirect(t *testing.T) {
	target := newReceiver(t)
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.url, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirect.Close)
	h, c := hermesSetup(t, nil)
	h.hooks.set("hermes", &external.Webhook{URL: redirect.URL, Secret: secret})
	messageID, _ := sentIDs(t, c.ok("send", args("target", "p1", "text", "hello")))
	h.sendFrom("p1", "hermes", "m-1", "kind", "answer", "replyTo", messageID)
	eventually(t, "logged", func() bool {
		return strings.Contains(h.logged(), "m-1 for hermes") && strings.Contains(h.logged(), "status 307")
	})
	if target.count() != 0 {
		t.Errorf("the redirect target got %d posts", target.count())
	}
}
