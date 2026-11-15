package a2aapi

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/orswn/a2a-hub/internal/auth"
	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/jsonx"
	"github.com/orswn/a2a-hub/internal/store"
)

var hermes = engine.Caller{Principal: auth.Principal{Name: "hermes", Kind: auth.KindPeer}}

type testHub struct {
	t      *testing.T
	engine *engine.Engine
	holds  *HoldLimiter
	mu     sync.Mutex
	lines  []string
}

func setup(t *testing.T, dir string) *testHub {
	t.Helper()
	path := store.Memory
	if dir != "" {
		path = dir
	}
	db, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	h := &testHub{t: t, holds: NewHoldLimiter(0)}
	e, err := engine.New(context.Background(), db, engine.Options{Log: h.log})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = e.Close()
		_ = db.Close()
	})
	if _, err := e.Register(auth.Principal{Name: "mars", Kind: auth.KindHost},
		jsonx.NewObject("agent", "a1", "protocol", float64(engine.HubProtocol), "labels", jsonx.NewObject(engine.HarnessLabel, "test"))); err != nil {
		t.Fatal(err)
	}
	h.engine = e
	return h
}

func (h *testHub) log(line string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lines = append(h.lines, line)
}

func (h *testHub) logged() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return strings.Join(h.lines, "\n")
}

func (h *testHub) handler(caller engine.Caller) *Handler {
	agent, err := h.engine.RequireAgent("a1")
	if err != nil {
		h.t.Fatal(err)
	}
	return &Handler{Engine: h.engine, Agent: agent, Caller: caller, Holds: h.holds, Log: h.log}
}

func (h *testHub) taskIDs() []string {
	result, err := h.engine.ListTasks("a1", engine.Caller{Principal: auth.Principal{Name: "mars", Kind: auth.KindHost}},
		engine.ListTasksInput{PageSize: MaxPageSize})
	if err != nil {
		h.t.Fatal(err)
	}
	ids := make([]string, 0, len(result.Tasks))
	for _, task := range result.Tasks {
		ids = append(ids, task.ID)
	}
	return ids
}

func callContext() context.Context {
	ctx, _ := a2asrv.NewCallContext(context.Background(), a2asrv.NewServiceParams(nil))
	return ctx
}

func request(text string, returnImmediately bool) *a2a.SendMessageRequest {
	message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(text))
	req := &a2a.SendMessageRequest{Message: message}
	if returnImmediately {
		req.Config = &a2a.SendMessageConfig{ReturnImmediately: true}
	}
	return req
}

// eventually polls cond, for state that another goroutine sets.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out: %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func isMalformed(err error) bool { return errors.Is(err, a2a.ErrInvalidParams) }

func TestAbortedBlockingSendFreesTheListenerAndTheHold(t *testing.T) {
	h := setup(t, "")
	ctx, cancel := context.WithCancel(callContext())
	done := make(chan error, 1)
	go func() {
		_, err := h.handler(hermes).SendMessage(ctx, request("hold", false))
		done <- err
	}()
	eventually(t, "the blocking send waits", func() bool {
		ids := h.taskIDs()
		return len(ids) == 1 && h.engine.ListenerCount(ids[0]) == 1
	})
	id := h.taskIDs()[0]
	if n := h.holds.Count("hermes"); n != 1 {
		t.Fatalf("holds = %d", n)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("err = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the send did not return after the abort")
	}
	if n := h.engine.ListenerCount(id); n != 0 {
		t.Errorf("listeners = %d", n)
	}
	if n := h.holds.Count("hermes"); n != 0 {
		t.Errorf("holds = %d", n)
	}
}

func TestAbortedStreamFreesTheListenerAndTheHold(t *testing.T) {
	h := setup(t, "")
	ctx, cancel := context.WithCancel(callContext())
	next, stop := iter.Pull2(h.handler(hermes).SendStreamingMessage(ctx, request("stream", false)))
	defer stop()
	first, err, ok := next()
	if _, isTask := first.(*a2a.Task); !ok || err != nil || !isTask {
		t.Fatalf("first = %T %v %v", first, err, ok)
	}
	id := h.taskIDs()[0]
	second := make(chan bool, 1)
	go func() {
		_, _, ok := next()
		second <- ok
	}()
	if n := h.engine.ListenerCount(id); n != 1 {
		t.Fatalf("listeners = %d", n)
	}
	if n := h.holds.Count("hermes"); n != 1 {
		t.Fatalf("holds = %d", n)
	}
	cancel()
	if <-second {
		t.Fatal("the stream yielded after the abort")
	}
	if n := h.engine.ListenerCount(id); n != 0 {
		t.Errorf("listeners = %d", n)
	}
	if n := h.holds.Count("hermes"); n != 0 {
		t.Errorf("holds = %d", n)
	}
}

func TestStreamEndsAtAuthRequired(t *testing.T) {
	h := setup(t, "")
	next, stop := iter.Pull2(h.handler(hermes).SendStreamingMessage(callContext(), request("auth", false)))
	defer stop()
	if _, err, ok := next(); err != nil || !ok {
		t.Fatalf("first: %v %v", err, ok)
	}
	id := h.taskIDs()[0]
	if _, err := h.engine.ApplyChange(id, engine.TaskChange{State: engine.StateAuthRequired}); err != nil {
		t.Fatal(err)
	}
	var seen []string
	for {
		event, err, ok := next()
		if !ok {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		seen = append(seen, fmt.Sprintf("%T", event))
	}
	if strings.Join(seen, ",") != "*a2a.TaskStatusUpdateEvent" {
		t.Errorf("seen = %v", seen)
	}
	if n := h.engine.ListenerCount(id); n != 0 {
		t.Errorf("listeners = %d", n)
	}
}

func TestHoldAboveTheLimitPerPrincipalIsAnInternalError(t *testing.T) {
	h := setup(t, "")
	ctx, cancel := context.WithCancel(callContext())
	var wg sync.WaitGroup
	for range HoldLimit {
		wg.Go(func() { _, _ = h.handler(hermes).SendMessage(ctx, request("hold", false)) })
	}
	eventually(t, "every send holds", func() bool { return h.holds.Count("hermes") == HoldLimit })
	_, err := h.handler(hermes).SendMessage(callContext(), request("one more", false))
	if !errors.Is(err, a2a.ErrInternalError) || !strings.Contains(err.Error(), "limit") {
		t.Errorf("err = %v", err)
	}
	if n := len(h.taskIDs()); n != HoldLimit {
		t.Errorf("tasks = %d", n)
	}
	host := engine.Caller{Principal: auth.Principal{Name: "mars", Kind: auth.KindHost}}
	if result, err := h.handler(host).SendMessage(callContext(), request("x", true)); err != nil || result == nil {
		t.Errorf("other principal: %v %v", result, err)
	}
	cancel()
	wg.Wait()
	if n := h.holds.Count("hermes"); n != 0 {
		t.Errorf("holds = %d", n)
	}
}

func TestNegativeHistoryLengthIsMalformed(t *testing.T) {
	h := setup(t, "")
	handler := h.handler(hermes)
	if _, err := handler.SendMessage(callContext(), request("x", true)); err != nil {
		t.Fatal(err)
	}
	id := a2a.TaskID(h.taskIDs()[0])
	minus := func(n int) *int { return &n }
	if _, err := handler.GetTask(callContext(), &a2a.GetTaskRequest{ID: id, HistoryLength: minus(-5)}); !isMalformed(err) {
		t.Errorf("GetTask: %v", err)
	}
	if _, err := handler.ListTasks(callContext(), &a2a.ListTasksRequest{PageSize: 10, HistoryLength: minus(-1)}); !isMalformed(err) {
		t.Errorf("ListTasks: %v", err)
	}
	req := request("y", true)
	req.Config.HistoryLength = minus(-2)
	if _, err := handler.SendMessage(callContext(), req); !isMalformed(err) {
		t.Errorf("SendMessage: %v", err)
	}
	if n := len(h.taskIDs()); n != 1 {
		t.Errorf("tasks = %d", n)
	}
	task, err := handler.GetTask(callContext(), &a2a.GetTaskRequest{ID: id, HistoryLength: minus(0)})
	if err != nil || task.Status.State != a2a.TaskStateSubmitted {
		t.Errorf("GetTask(0) = %v %v", task, err)
	}
}

func TestUnexpectedErrorIsLoggedAndSentAsAGenericMessage(t *testing.T) {
	dir := t.TempDir()
	h := setup(t, dir)
	other, err := sql.Open("sqlite", "file:"+filepath.Join(dir, store.FileName))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Close() }()
	if _, err := other.Exec("DROP TABLE task_messages"); err != nil {
		t.Fatal(err)
	}
	_, err = h.handler(hermes).SendMessage(callContext(), request("x", true))
	if !errors.Is(err, a2a.ErrInternalError) || err.Error() != "internal error" {
		t.Errorf("err = %v", err)
	}
	if !strings.Contains(h.logged(), "task_messages") {
		t.Errorf("logged = %q", h.logged())
	}
}

func TestASendStampsTheDeliveryUnderTheBusURIThatTheRequestUsed(t *testing.T) {
	current, legacy := engine.BusExtensionURI, engine.LegacyBusExtensionURI
	wait := map[string]any{"deliveryWait": true}
	cases := []struct {
		name     string
		message  map[string]any
		request  map[string]any
		wantURI  string
		header   string
		activate []string
	}{
		{"protocol 2 client", map[string]any{legacy: wait}, nil, legacy, legacy, []string{legacy}},
		{"protocol 3 client", map[string]any{current: wait}, nil, current, current, []string{current}},
		{"request metadata", nil, map[string]any{legacy: wait}, legacy, "", []string{}},
		{"both keys", map[string]any{legacy: wait, current: wait}, nil, current, current + ", " + legacy, []string{current, legacy}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := setup(t, "")
			params := map[string][]string{}
			if c.header != "" {
				params[a2a.SvcParamExtensions] = []string{c.header}
			}
			ctx, callCtx := a2asrv.NewCallContext(context.Background(), a2asrv.NewServiceParams(params))
			req := request("hi", true)
			req.Message.Metadata = c.message
			req.Metadata = c.request
			result, err := h.handler(hermes).SendMessage(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			task, ok := result.(*a2a.Task)
			if !ok {
				t.Fatalf("result %T", result)
			}
			stamps := map[string]any{}
			for _, uri := range engine.BusExtensionURIs {
				if v, ok := task.Metadata[uri]; ok {
					stamps[uri] = v
				}
			}
			if got, want := fmt.Sprint(stamps), fmt.Sprint(map[string]any{c.wantURI: map[string]any{"delivery": Queued}}); got != want {
				t.Errorf("stamps %s, want %s", got, want)
			}
			if got := callCtx.Extensions().ActivatedURIs(); fmt.Sprint(got) != fmt.Sprint(c.activate) {
				t.Errorf("activated %q, want %q", got, c.activate)
			}
		})
	}
}
