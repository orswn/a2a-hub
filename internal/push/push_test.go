package push

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orswn/a2a-hub/internal/auth"
	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/jsonx"
	"github.com/orswn/a2a-hub/internal/pushguard"
	"github.com/orswn/a2a-hub/internal/store"
)

var hermes = engine.Caller{Principal: auth.Principal{Name: "hermes", Kind: auth.KindPeer}}

type call struct {
	url    string
	header http.Header
	body   string
}

type testPush struct {
	t      *testing.T
	dir    string
	engine *engine.Engine
	sender *Sender
	clock  atomic.Int64
	status atomic.Int64
	mu     sync.Mutex
	calls  []call
	taskID string
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func setup(t *testing.T, allow []string) *testPush {
	t.Helper()
	p := &testPush{t: t, dir: t.TempDir()}
	p.clock.Store(1_000_000)
	p.status.Store(500)
	guard, err := pushguard.New(allow, func(_ context.Context, host string) ([]string, error) {
		if host == "inside.example" {
			return []string{"10.0.0.7"}, nil
		}
		return []string{"93.184.216.34"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(context.Background(), p.dir)
	if err != nil {
		t.Fatal(err)
	}
	e, err := engine.New(context.Background(), db, engine.Options{Now: p.now, PushGuard: guard})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = e.Close()
		_ = db.Close()
	})
	p.engine = e
	p.sender = New(e, Options{Now: p.now, Guard: guard, Transport: roundTrip(func(req *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(req.Body)
		p.mu.Lock()
		p.calls = append(p.calls, call{url: req.URL.String(), header: req.Header.Clone(), body: string(body)})
		p.mu.Unlock()
		header := http.Header{}
		if p.status.Load()/100 == 3 {
			header.Set("Location", "https://elsewhere.example/b")
		}
		return &http.Response{StatusCode: int(p.status.Load()), Header: header, Body: http.NoBody, Request: req}, nil
	})})
	if _, err := e.Register(auth.Principal{Name: "mars", Kind: auth.KindHost},
		jsonx.NewObject("agent", "a1", "protocol", float64(engine.HubProtocol), "labels", jsonx.NewObject(engine.HarnessLabel, "test"))); err != nil {
		t.Fatal(err)
	}
	sent, err := e.Send(engine.SendInput{AgentID: "a1", Caller: hermes, Message: jsonx.NewObject(
		"messageId", "m1", "role", "ROLE_USER", "parts", []any{jsonx.NewObject("text", "x")})})
	if err != nil {
		t.Fatal(err)
	}
	p.taskID = sent.Task.ID
	return p
}

func (p *testPush) now() int64 { return p.clock.Load() }

func (p *testPush) config(url, id, token string) {
	p.t.Helper()
	if _, err := p.engine.CreatePushConfig("a1", hermes, engine.PushConfigInput{TaskID: p.taskID, URL: url, ID: id, Token: token}); err != nil {
		p.t.Fatal(err)
	}
}

func (p *testPush) update(state string) {
	p.t.Helper()
	if _, err := p.engine.LinkUpdate("a1", p.taskID, jsonx.NewObject("state", state)); err != nil {
		p.t.Fatal(err)
	}
}

func (p *testPush) due(at int64) []engine.OutboxItem {
	p.t.Helper()
	items, err := p.engine.DuePushes(at, 10)
	if err != nil {
		p.t.Fatal(err)
	}
	return items
}

func (p *testPush) next() (int64, bool) {
	p.t.Helper()
	at, ok, err := p.engine.NextPushAt()
	if err != nil {
		p.t.Fatal(err)
	}
	return at, ok
}

func (p *testPush) deliver(item engine.OutboxItem) bool {
	p.t.Helper()
	ok, err := p.sender.Deliver(item)
	if err != nil {
		p.t.Fatal(err)
	}
	return ok
}

// payload reads one path of a push body, such as "statusUpdate.status.state".
func payload(t *testing.T, item engine.OutboxItem, path string) any {
	t.Helper()
	var value any
	if err := json.Unmarshal([]byte(item.Payload), &value); err != nil {
		t.Fatal(err)
	}
	for _, key := range strings.Split(path, ".") {
		switch v := value.(type) {
		case map[string]any:
			value = v[key]
		case []any:
			if len(v) == 0 {
				return nil
			}
			value = v[0]
			if m, ok := value.(map[string]any); ok {
				value = m[key]
			}
		default:
			return nil
		}
	}
	return value
}

func str(s string) *string { return &s }

func TestHeadersUseAuthorizationWhenAuthenticationIsSetElseTheToken(t *testing.T) {
	cases := []struct {
		item engine.OutboxItem
		want http.Header
	}{
		{engine.OutboxItem{Token: str("t"), AuthScheme: str("Bearer"), AuthCredentials: str("abc")},
			http.Header{"Content-Type": {"application/a2a+json"}, "Authorization": {"Bearer abc"}}},
		{engine.OutboxItem{Token: str("t")},
			http.Header{"Content-Type": {"application/a2a+json"}, "X-A2a-Notification-Token": {"t"}}},
		{engine.OutboxItem{}, http.Header{"Content-Type": {"application/a2a+json"}}},
	}
	for i, c := range cases {
		if got := Headers(c.item); !reflect.DeepEqual(got, c.want) {
			t.Errorf("case %d: %v", i, got)
		}
	}
}

func TestHeadersSendTheTokenWhenAuthenticationHasNoCredentials(t *testing.T) {
	if got := Headers(engine.OutboxItem{Token: str("t"), AuthScheme: str("Bearer")}); !reflect.DeepEqual(got,
		http.Header{"Content-Type": {"application/a2a+json"}, "X-A2a-Notification-Token": {"t"}}) {
		t.Errorf("with token: %v", got)
	}
	if got := Headers(engine.OutboxItem{AuthScheme: str("Bearer")}); !reflect.DeepEqual(got,
		http.Header{"Content-Type": {"application/a2a+json"}}) {
		t.Errorf("without token: %v", got)
	}
}

func TestOutboxGetsOneRowPerConfigOnEachTaskEventInOrder(t *testing.T) {
	p := setup(t, []string{"127.0.0.0/8"})
	p.config("http://127.0.0.1:1/a", "", "tok")
	p.config("http://127.0.0.1:1/b", "b", "")
	p.update("working")
	p.update("completed")
	due := p.due(p.now())
	if len(due) != 2 {
		t.Fatalf("due = %d", len(due))
	}
	if payload(t, due[0], "statusUpdate.taskId") != p.taskID || payload(t, due[0], "statusUpdate.status.state") != "TASK_STATE_WORKING" {
		t.Errorf("first = %s", due[0].Payload)
	}
	if err := p.engine.PushDone(due[0].ID); err != nil {
		t.Fatal(err)
	}
	for _, item := range p.due(p.now()) {
		if strings.HasSuffix(item.URL, "/a") && payload(t, item, "statusUpdate.status.state") != "TASK_STATE_COMPLETED" {
			t.Errorf("next of /a = %s", item.Payload)
		}
	}
}

func TestRetriesAfter1s5sAnd30sThenDropsTheRow(t *testing.T) {
	p := setup(t, []string{"127.0.0.0/8"})
	p.config("http://127.0.0.1:1/hook", "", "tok")
	p.update("working")
	var waits []int64
	for attempt := range 4 {
		due := p.due(p.now())
		if len(due) != 1 || due[0].Attempts != int64(attempt) {
			t.Fatalf("attempt %d: due = %+v", attempt, due)
		}
		if p.deliver(due[0]) {
			t.Fatal("delivered")
		}
		if at, ok := p.next(); ok {
			waits = append(waits, at-p.now())
			p.clock.Store(at)
		}
	}
	if !reflect.DeepEqual(waits, []int64{1000, 5000, 30000}) {
		t.Errorf("waits = %v", waits)
	}
	if due := p.due(p.now() + 1e9); len(due) != 0 {
		t.Errorf("due = %v", due)
	}
	if len(p.calls) != 4 || p.calls[0].header.Get("X-A2A-Notification-Token") != "tok" {
		t.Errorf("calls = %+v", p.calls)
	}
}

func TestDeletesTheRowAfterA2xxAnswer(t *testing.T) {
	p := setup(t, []string{"127.0.0.0/8"})
	p.status.Store(204)
	p.config("http://127.0.0.1:1/hook", "", "")
	p.update("working")
	if !p.deliver(p.due(p.now())[0]) {
		t.Error("not delivered")
	}
	if _, ok := p.next(); ok {
		t.Error("a row is left")
	}
}

func TestRefusesAConfigURLThatIsNotHTTPOrHTTPS(t *testing.T) {
	p := setup(t, []string{"127.0.0.0/8"})
	for _, url := range []string{"file:///etc/passwd", "ftp://x/y", "not a url", ""} {
		_, err := p.engine.CreatePushConfig("a1", hermes, engine.PushConfigInput{TaskID: p.taskID, URL: url})
		var hubErr *engine.Error
		if !errors.As(err, &hubErr) {
			t.Errorf("%q: %v", url, err)
		}
	}
}

func TestRemovesQueuedRowsWhenTheConfigIsDeleted(t *testing.T) {
	p := setup(t, []string{"127.0.0.0/8"})
	p.config("http://127.0.0.1:1/hook", "c1", "")
	p.update("working")
	if err := p.engine.DeletePushConfig("a1", hermes, p.taskID, "c1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.next(); ok {
		t.Error("a row is left")
	}
}

func TestDoesNotFollowRedirectsAndCountsA3xxAsAFailedAttempt(t *testing.T) {
	p := setup(t, []string{"127.0.0.0/8"})
	p.status.Store(302)
	p.config("https://hooks.example/a", "", "")
	p.update("working")
	if p.deliver(p.due(p.now())[0]) {
		t.Error("delivered")
	}
	if len(p.calls) != 1 {
		t.Errorf("calls = %+v", p.calls)
	}
	if due := p.due(p.now() + 60_000); len(due) != 1 || due[0].Attempts != 1 {
		t.Errorf("due = %+v", due)
	}
}

func TestRefusesAtSendTimeAHostNameThatResolvesToADeniedAddress(t *testing.T) {
	p := setup(t, nil)
	p.status.Store(200)
	p.config("https://inside.example/a", "", "")
	p.update("working")
	if p.deliver(p.due(p.now())[0]) {
		t.Error("delivered")
	}
	if len(p.calls) != 0 {
		t.Errorf("calls = %+v", p.calls)
	}
	if due := p.due(p.now() + 60_000); len(due) != 1 || due[0].Attempts != 1 {
		t.Errorf("due = %+v", due)
	}
}

func TestRefusesAtConfigTimeAnIPLiteralInADeniedRange(t *testing.T) {
	p := setup(t, nil)
	for _, url := range []string{"http://169.254.169.254/latest/meta-data", "http://127.0.0.1:9/x", "http://[fd00::1]/x"} {
		_, err := p.engine.CreatePushConfig("a1", hermes, engine.PushConfigInput{TaskID: p.taskID, URL: url})
		if err == nil || !strings.Contains(err.Error(), "not allowed") {
			t.Errorf("%s: %v", url, err)
		}
	}
}

func TestKeepsAtMostTheNewestRowsOfOneConfigWhileAWebhookIsDown(t *testing.T) {
	p := setup(t, []string{"127.0.0.0/8"})
	p.config("https://hooks.example/a", "c1", "")
	p.config("https://hooks.example/b", "c2", "")
	p.update("working")
	for i := range engine.PushBacklogMax + 10 {
		if _, err := p.engine.SetStatus("a1", "step "+strconv.Itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	reader, err := sql.Open("sqlite", "file:"+filepath.Join(p.dir, store.FileName)+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	rows, err := reader.Query("SELECT config_id, count(*) FROM push_outbox GROUP BY config_id ORDER BY config_id")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var id string
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			t.Fatal(err)
		}
		got = append(got, id+"="+strconv.Itoa(n))
	}
	_ = rows.Close()
	if want := []string{"c1=" + strconv.Itoa(engine.PushBacklogMax), "c2=" + strconv.Itoa(engine.PushBacklogMax)}; !reflect.DeepEqual(got, want) {
		t.Errorf("rows = %v, want %v", got, want)
	}
	if text := payload(t, p.due(p.now())[0], "statusUpdate.status.message.parts.text"); text != "step 10" {
		t.Errorf("oldest = %v", text)
	}
}

// The resolver of the send time check says public, but the socket goes to loopback: the dial refuses it.
func TestRefusesASocketToADeniedAddressAfterTheSendTimeCheck(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { hits.Add(1) }))
	defer server.Close()
	_, port, _ := net.SplitHostPort(server.Listener.Addr().String())
	p := setup(t, nil)
	p.sender = New(p.engine, Options{Now: p.now, Guard: mustGuard(t, nil)})
	item := engine.OutboxItem{ID: 1, URL: "http://localhost:" + port + "/hook", Payload: "{}"}
	ok, reason := p.sender.attempt(item)
	if ok || !regexp.MustCompile(`push address (127\.0\.0\.1|::1) is not allowed`).MatchString(reason) || hits.Load() != 0 {
		t.Errorf("attempt = %v %q, hits %d", ok, reason, hits.Load())
	}
	p.sender = New(p.engine, Options{Now: p.now, Guard: mustGuard(t, []string{"127.0.0.0/8"})})
	if ok, reason := p.sender.attempt(item); !ok || hits.Load() != 1 {
		t.Errorf("allowed attempt = %v %q, hits %d", ok, reason, hits.Load())
	}
}

func mustGuard(t *testing.T, allow []string) *pushguard.Guard {
	t.Helper()
	g, err := pushguard.New(allow, func(context.Context, string) ([]string, error) { return []string{"93.184.216.34"}, nil })
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestStartSendsQueuedRowsAndStopWaitsForTheOpenAttempts(t *testing.T) {
	received := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
	}))
	defer server.Close()
	p := setup(t, []string{"127.0.0.0/8"})
	p.sender = New(p.engine, Options{Guard: mustGuard(t, []string{"127.0.0.0/8"})})
	p.config(server.URL+"/hook", "", "")
	p.update("working")
	p.sender.Start()
	defer p.sender.Stop()
	select {
	case body := <-received:
		if !strings.Contains(body, "TASK_STATE_WORKING") {
			t.Errorf("body = %s", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no push")
	}
	p.update("completed")
	select {
	case body := <-received:
		if !strings.Contains(body, "TASK_STATE_COMPLETED") {
			t.Errorf("body = %s", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no push after the commit")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := p.next(); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("rows are left")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStopWaitsForARunThatATimerStarted(t *testing.T) {
	p := setup(t, nil)
	var armed atomic.Bool
	var once sync.Once
	entered := make(chan struct{})
	release := make(chan struct{})
	p.sender = New(p.engine, Options{Guard: mustGuard(t, nil), Now: func() int64 {
		if armed.Load() {
			once.Do(func() {
				close(entered)
				<-release
			})
		}
		return p.now()
	}})
	p.sender.schedule(p.now() + 50)
	armed.Store(true)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the timer did not start a run")
	}
	stopped := make(chan struct{})
	go func() {
		p.sender.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		close(release)
		t.Fatal("Stop returned while a run was open")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after the run ended")
	}
}
