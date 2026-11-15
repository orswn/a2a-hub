package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orswn/a2a-hub/internal/auth"
	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/external"
	"github.com/orswn/a2a-hub/internal/jsonx"
	"github.com/orswn/a2a-hub/internal/store"
)

var hermes = engine.Caller{Principal: auth.Principal{Name: "hermes", Kind: auth.KindPeer}}

type harness struct {
	t      *testing.T
	engine *engine.Engine
	mu     sync.Mutex
	logs   []string
}

func setup(t *testing.T) *harness {
	t.Helper()
	db, err := store.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	e, err := engine.New(context.Background(), db, engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = e.Close()
		_ = db.Close()
	})
	return &harness{t: t, engine: e}
}

func (h *harness) log(line string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.logs = append(h.logs, line)
}

func (h *harness) gateway(agentURL string, change func(*external.Agent)) *Gateway {
	agent := external.Agent{
		ID: "ext", Name: "ext", URL: agentURL, Token: "t", Description: "d",
		Poll: 50 * time.Millisecond, CardTTL: time.Minute, ReplyTimeout: 10 * time.Second,
		Idle: 5 * time.Second, Retry: 50 * time.Millisecond, Offline: time.Minute,
	}
	if change != nil {
		change(&agent)
	}
	g := New(h.engine, Options{Log: h.log, ConnectTimeout: 2 * time.Second})
	g.Configure([]external.Agent{agent})
	g.Start()
	h.t.Cleanup(g.Stop)
	return g
}

func (h *harness) send(text string) string {
	h.t.Helper()
	message := jsonx.NewObject("role", "ROLE_USER", "messageId", h.engine.NewID(),
		"parts", []any{jsonx.NewObject("text", text)})
	result, err := h.engine.Send(engine.SendInput{AgentID: "ext", Caller: hermes, Message: message})
	if err != nil {
		h.t.Fatal(err)
	}
	return result.Task.ID
}

func (h *harness) waitTask(id string, want engine.State) *engine.Task {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		task, err := h.engine.ReadTask(id)
		if err != nil {
			h.t.Fatal(err)
		}
		if task != nil && task.Status.State == want {
			return task
		}
		if time.Now().After(deadline) {
			state := engine.State("gone")
			if task != nil {
				state = task.Status.State
			}
			h.t.Fatalf("task %s is %s, want %s; log %q", id, state, want, h.logs)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func statusText(task *engine.Task) string {
	if task.Status.Message == nil {
		return ""
	}
	return engine.MessageText(task.Status.Message)
}

// agentServer serves a card and answers SendStreamingMessage with the given SSE frames.
func agentServer(t *testing.T, stream func(w http.ResponseWriter, r *http.Request)) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"name":"ext","skills":[{"id":"s","name":"S","tags":["a",1]}]}`)
			return
		}
		stream(w, r)
	}))
	t.Cleanup(server.Close)
	return server
}

func frames(w http.ResponseWriter, events ...string) {
	w.Header().Set("Content-Type", "text/event-stream")
	for _, event := range events {
		_, _ = io.WriteString(w, "data: "+event+"\n\n")
		w.(http.Flusher).Flush()
	}
}

func TestBackoffDoublesThePollIntervalUpToOneMinute(t *testing.T) {
	cases := []struct {
		poll     time.Duration
		attempts int64
		want     time.Duration
	}{
		{10 * time.Second, 0, 10 * time.Second},
		{10 * time.Second, 1, 20 * time.Second},
		{10 * time.Second, 2, 40 * time.Second},
		{10 * time.Second, 3, time.Minute},
		{10 * time.Second, 200, time.Minute},
		{2 * time.Minute, 0, time.Minute},
	}
	for _, c := range cases {
		if got := Backoff(c.poll, c.attempts); got != c.want {
			t.Errorf("Backoff(%v, %d) = %v, want %v", c.poll, c.attempts, got, c.want)
		}
	}
}

func TestStatusUpdatesMapToTheHubTask(t *testing.T) {
	cases := []struct {
		name   string
		events []string
		state  engine.State
		text   string
	}{
		{"completed with text", []string{
			`{"jsonrpc":"2.0","id":1,"result":{"task":{"id":"r1","contextId":"c","status":{"state":"TASK_STATE_WORKING"}}}}`,
			`{"jsonrpc":"2.0","id":1,"result":{"statusUpdate":{"taskId":"r1","status":{"state":"TASK_STATE_COMPLETED","message":{"parts":[{"text":"done"}]}}}}}`,
		}, engine.StateCompleted, "done"},
		{"input required", []string{
			`{"jsonrpc":"2.0","id":1,"result":{"statusUpdate":{"taskId":"r1","status":{"state":"TASK_STATE_INPUT_REQUIRED","message":{"parts":[{"text":"which?"}]}}}}}`,
		}, engine.StateInputRequired, "which?"},
		{"rejected task", []string{
			`{"jsonrpc":"2.0","id":1,"result":{"task":{"id":"r1","status":{"state":"TASK_STATE_REJECTED","message":{"parts":[{"text":"no"}]}}}}}`,
		}, engine.StateRejected, "no"},
		{"direct message", []string{
			`{"jsonrpc":"2.0","id":1,"result":{"message":{"role":"ROLE_AGENT","parts":[{"text":"hi"}]}}}`,
		}, engine.StateCompleted, "hi"},
		{"error frame", []string{
			`{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"boom"}}`,
		}, engine.StateFailed, "ext answered -32603: boom"},
		{"unauthorized frame", []string{
			`{"jsonrpc":"2.0","id":1,"error":{"code":-32050,"message":"x"}}`,
		}, engine.StateFailed, "hub token rejected by ext"},
		{"untrusted frame", []string{
			`{"jsonrpc":"2.0","id":1,"error":{"code":-32052,"message":"x"}}`,
		}, engine.StateFailed, "hub not in A2A_TRUSTED_PEERS of ext"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := setup(t)
			server := agentServer(t, func(w http.ResponseWriter, _ *http.Request) { frames(w, c.events...) })
			h.gateway(server.URL, nil)
			task := h.waitTask(h.send("hello"), c.state)
			if got := statusText(task); got != c.text {
				t.Errorf("status text %q, want %q", got, c.text)
			}
		})
	}
}

func TestArtifactsWaitForTheFinalStatusAndAppendJoinsTheParts(t *testing.T) {
	h := setup(t)
	server := agentServer(t, func(w http.ResponseWriter, _ *http.Request) {
		frames(w,
			`{"result":{"artifactUpdate":{"taskId":"r1","artifact":{"artifactId":"a","parts":[{"text":"one "}]}}}}`,
			`{"result":{"artifactUpdate":{"taskId":"r1","append":true,"artifact":{"artifactId":"a","parts":[{"text":"two"}]}}}}`,
			`{"result":{"statusUpdate":{"taskId":"r1","status":{"state":"TASK_STATE_COMPLETED"}}}}`,
		)
	})
	h.gateway(server.URL, nil)
	task := h.waitTask(h.send("hello"), engine.StateCompleted)
	if len(task.Artifacts) != 1 {
		t.Fatalf("artifacts %s", jsonx.Stringify(task.Artifacts))
	}
	parts, _ := task.Artifacts[0].Value("parts").([]any)
	if len(parts) != 2 {
		t.Errorf("parts %s", jsonx.Stringify(parts))
	}
}

func TestHTTPStatusesMapToTheHubAction(t *testing.T) {
	cases := []struct {
		status int
		body   string
		text   string
	}{
		{http.StatusUnauthorized, "", "hub token rejected by ext"},
		{http.StatusForbidden, "", "hub not in A2A_TRUSTED_PEERS of ext"},
		{http.StatusTooManyRequests, "", "ext rate limit: the hub gave up after 5 retries"},
		{http.StatusInternalServerError, "oops", "ext answered HTTP 500"},
		{http.StatusOK, `{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"gone"}}`, "ext answered -32001: gone"},
	}
	for _, c := range cases {
		t.Run(fmt.Sprint(c.status), func(t *testing.T) {
			h := setup(t)
			server := agentServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(c.status)
				_, _ = io.WriteString(w, c.body)
			})
			h.gateway(server.URL, func(a *external.Agent) { a.Retry = time.Millisecond })
			task := h.waitTask(h.send("hello"), engine.StateFailed)
			if got := statusText(task); got != c.text {
				t.Errorf("status text %q, want %q", got, c.text)
			}
		})
	}
}

func TestTheOutboundRequestHasTheAgentHeadersAndTheSenderLine(t *testing.T) {
	h := setup(t)
	type seen struct {
		header http.Header
		body   string
	}
	got := make(chan seen, 1)
	server := agentServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got <- seen{r.Header.Clone(), string(body)}
		frames(w, `{"result":{"message":{"parts":[{"text":"ok"}]}}}`)
	})
	h.gateway(server.URL, nil)
	h.send("hello")
	request := <-got
	for key, want := range map[string]string{
		"Authorization": "Bearer t", "A2A-Version": "1.0", "Content-Type": "application/json", "Accept": "text/event-stream",
	} {
		if value := request.header.Get(key); value != want {
			t.Errorf("%s: %q, want %q", key, value, want)
		}
	}
	if !strings.Contains(request.body, `"method":"SendStreamingMessage"`) || !strings.Contains(request.body, `"text":"[from hermes]\nhello"`) {
		t.Errorf("body %s", request.body)
	}
}

func TestADroppedStreamFallsBackToGetTask(t *testing.T) {
	h := setup(t)
	server := agentServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch {
		case strings.Contains(string(body), "SendStreamingMessage"):
			frames(w, `{"result":{"task":{"id":"r1","contextId":"c","status":{"state":"TASK_STATE_WORKING"}}}}`)
		case strings.Contains(string(body), "GetTask"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":1,"result":{"id":"r1","status":{"state":"TASK_STATE_COMPLETED"},`+
				`"artifacts":[{"artifactId":"a","parts":[{"text":"result"}]}]}}`)
		}
	})
	h.gateway(server.URL, nil)
	task := h.waitTask(h.send("hello"), engine.StateCompleted)
	if len(task.Artifacts) != 1 {
		t.Errorf("artifacts %s", jsonx.Stringify(task.Artifacts))
	}
}

func TestAStreamWithoutBytesForTheIdleTimeIsDropped(t *testing.T) {
	h := setup(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	server := agentServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "ListTasks") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":1,"result":{"tasks":[]}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	h.gateway(server.URL, func(a *external.Agent) { a.Idle = 100 * time.Millisecond })
	task := h.waitTask(h.send("hello"), engine.StateFailed)
	if want := "ext did not confirm the task before the stream dropped. Ask again in the same context."; statusText(task) != want {
		t.Errorf("status text %q", statusText(task))
	}
}

func TestAnUnreachableAgentFailsTheTaskAfterTheOfflineTime(t *testing.T) {
	h := setup(t)
	server := httptest.NewServer(http.NotFoundHandler())
	url := server.URL
	server.Close()
	h.gateway(url, func(a *external.Agent) {
		a.Poll = 10 * time.Millisecond
		a.Offline = 100 * time.Millisecond
	})
	task := h.waitTask(h.send("hello"), engine.StateFailed)
	if text := statusText(task); !strings.HasPrefix(text, "ext is unreachable: ") || !strings.Contains(text, "ECONNREFUSED") {
		t.Errorf("status text %q", text)
	}
}

func TestStopEndsOpenStreamsAndLeavesNoGoroutine(t *testing.T) {
	h := setup(t)
	opened := make(chan struct{}, 1)
	release := make(chan struct{})
	server := agentServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"result":{"task":{"id":"r1","status":{"state":"TASK_STATE_WORKING"}}}}`+"\n\n")
		w.(http.Flusher).Flush()
		opened <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	defer close(release)
	before := runtime.NumGoroutine()
	g := New(h.engine, Options{Log: h.log})
	g.Configure([]external.Agent{{
		ID: "ext", Name: "ext", URL: server.URL, Token: "t", Description: "d",
		Poll: time.Hour, CardTTL: time.Hour, ReplyTimeout: time.Hour, Idle: time.Hour, Retry: time.Hour, Offline: time.Hour,
	}})
	g.Start()
	id := h.send("hello")
	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("no stream")
	}
	h.waitTask(id, engine.StateWorking)
	stopped := make(chan struct{})
	go func() {
		g.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return")
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		buf := make([]byte, 1<<20)
		t.Errorf("%d goroutines, %d before:\n%s", n, before, buf[:runtime.Stack(buf, true)])
	}
	remote, err := h.engine.LatestRemote(id)
	if err != nil || remote == nil || remote.State == "done" {
		t.Errorf("the dispatch row must stay for the next start: %+v %v", remote, err)
	}
}

func TestTheCardTakesTheSkillsOfTheAgentCard(t *testing.T) {
	h := setup(t)
	server := agentServer(t, func(http.ResponseWriter, *http.Request) {})
	g := h.gateway(server.URL, nil)
	deadline := time.Now().Add(5 * time.Second)
	for {
		card := g.Card("ext")
		if card != nil && len(card.Skills) == 1 {
			if got := string(jsonx.Stringify(card.Skills[0])); got != `{"id":"s","name":"S","description":"","tags":["a"]}` {
				t.Errorf("skill %s", got)
			}
			if card.Description != "d" {
				t.Errorf("description %q", card.Description)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no skills")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCancelTaskOutlivesTheEndOfTheDispatch(t *testing.T) {
	h := setup(t)
	cancelArrived := make(chan struct{})
	var clientGone, answered atomic.Bool
	server := agentServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		switch {
		case strings.Contains(string(body), "SendStreamingMessage"):
			frames(w, `{"result":{"task":{"id":"r1","contextId":"c","status":{"state":"TASK_STATE_WORKING"}}}}`)
			select {
			case <-cancelArrived:
			case <-r.Context().Done():
				return
			}
			frames(w, `{"result":{"statusUpdate":{"taskId":"r1","status":{"state":"TASK_STATE_WORKING","message":{"parts":[{"text":"progress"}]}}}}}`)
			<-r.Context().Done()
		case strings.Contains(string(body), "CancelTask"):
			close(cancelArrived)
			select {
			case <-time.After(300 * time.Millisecond):
			case <-r.Context().Done():
				clientGone.Store(true)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":1,"result":{"id":"r1","status":{"state":"TASK_STATE_CANCELED"}}}`)
			answered.Store(true)
		}
	})
	h.gateway(server.URL, nil)
	id := h.send("hello")
	h.waitTask(id, engine.StateWorking)
	if _, err := h.engine.Cancel("ext", hermes, id); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for !answered.Load() && !clientGone.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if clientGone.Load() || !answered.Load() {
		h.mu.Lock()
		defer h.mu.Unlock()
		t.Errorf("the hub aborted its own CancelTask request: answered=%v logs=%q", answered.Load(), h.logs)
	}
}

func (h *harness) online(id string) bool {
	h.t.Helper()
	agents, err := h.engine.ListAgents(true)
	if err != nil {
		h.t.Fatal(err)
	}
	for _, agent := range agents {
		if agent.ID == id {
			return agent.Online
		}
	}
	return false
}

func TestADispatchChecksItsJobAgainAfterItWroteTheDispatchRow(t *testing.T) {
	agent := func(url string) external.Agent {
		return external.Agent{ID: "ext", Name: "ext", URL: url, Token: "t", Description: "d",
			Poll: time.Hour, CardTTL: time.Hour, ReplyTimeout: time.Hour, Idle: time.Hour, Retry: time.Hour, Offline: time.Hour}
	}
	cases := []struct {
		name    string
		between func(h *harness, g *Gateway, taskID string)
		queued  bool
		remote  string
		state   engine.State
	}{
		{"stop", func(_ *harness, g *Gateway, _ string) { g.Stop() }, true, "", engine.StateSubmitted},
		{"reload with a new URL", func(_ *harness, g *Gateway, _ string) {
			g.Configure([]external.Agent{agent("http://127.0.0.1:2/")})
		}, true, "", engine.StateSubmitted},
		{"agent removed", func(_ *harness, g *Gateway, _ string) { g.Configure(nil) }, false, "done", engine.StateFailed},
		{"task canceled", func(h *harness, _ *Gateway, taskID string) {
			if _, err := h.engine.Cancel("ext", hermes, taskID); err != nil {
				h.t.Fatal(err)
			}
		}, false, "done", engine.StateCanceled},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := setup(t)
			g := New(h.engine, Options{Log: h.log})
			t.Cleanup(g.Stop)
			current := agent("http://127.0.0.1:1/")
			g.Configure([]external.Agent{current})
			taskID := h.send("hello")
			jobs, err := h.engine.ExternalJobs()
			if err != nil || len(jobs) != 1 {
				t.Fatalf("jobs %+v %v", jobs, err)
			}
			g.mu.Lock()
			configured := g.agents["ext"]
			g.mu.Unlock()
			r := g.track(taskID, "ext")
			remote, err := h.engine.StartRemote(jobs[0])
			if err != nil {
				t.Fatal(err)
			}
			c.between(h, g, taskID)
			if g.stillDue(r, remote, jobs[0], configured) {
				t.Fatal("the dispatch went on")
			}
			g.untrack(r)
			jobs, _ = h.engine.ExternalJobs()
			sends := 0
			for _, job := range jobs {
				if job.Op == "send" {
					sends++
				}
			}
			if queued := sends == 1; queued != c.queued {
				t.Errorf("send job queued %v, want %v", queued, c.queued)
			}
			latest, _ := h.engine.LatestRemote(taskID)
			switch {
			case c.remote == "" && latest != nil:
				t.Errorf("dispatch row left: %+v", latest)
			case c.remote != "" && (latest == nil || latest.State != c.remote):
				t.Errorf("dispatch row %+v, want %s", latest, c.remote)
			}
			if state, _ := h.engine.TaskState(taskID); state != c.state {
				t.Errorf("task %s, want %s", state, c.state)
			}
		})
	}
}

func TestAReloadThatKeepsTheAgentKeepsItsPresenceWrites(t *testing.T) {
	h := setup(t)
	g := New(h.engine, Options{Log: h.log})
	t.Cleanup(g.Stop)
	agent := external.Agent{ID: "ext", Name: "ext", URL: "http://127.0.0.1:1/", Token: "t", Description: "d"}
	g.Configure([]external.Agent{agent})
	g.mu.Lock()
	before := g.agents["ext"]
	g.mu.Unlock()
	g.Configure([]external.Agent{agent})
	g.setOnline(before, true)
	if !h.online("ext") {
		t.Error("a dispatch from before the reload could not set the agent online")
	}
	agent.URL = "http://127.0.0.1:2/"
	g.Configure([]external.Agent{agent})
	g.setOnline(before, false)
	if !h.online("ext") {
		t.Error("a dispatch to the old URL set the agent offline")
	}
}

func TestStopClosesTheIdleConnectionOfTheCardFetch(t *testing.T) {
	h := setup(t)
	server := agentServer(t, func(http.ResponseWriter, *http.Request) {})
	before := runtime.NumGoroutine()
	g := New(h.engine, Options{Log: h.log})
	g.Configure([]external.Agent{{
		ID: "ext", Name: "ext", URL: server.URL, Token: "t", Description: "d",
		Poll: time.Hour, CardTTL: time.Hour, ReplyTimeout: time.Hour, Idle: time.Hour, Retry: time.Hour, Offline: time.Hour,
	}})
	g.Start()
	deadline := time.Now().Add(5 * time.Second)
	for !h.online("ext") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	g.Stop()
	deadline = time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Errorf("%d goroutines, %d before", n, before)
	}
}
