package codexadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orswn/a2a-hub/internal/codexrpc"
	"github.com/orswn/a2a-hub/internal/codexrpc/codexrpctest"
	"github.com/orswn/a2a-hub/internal/linkclient"
)

const (
	threadA = "01a1df27-4fcc-7253-81a3-1af98be8f127"
	threadB = "01a1df28-0000-7000-8000-000000000002"
	threadC = "01a1df29-0000-7000-8000-000000000003"
	threadD = "01a1df2a-0000-7000-8000-000000000004"
)

// appServer scripts the fake app-server: its loaded threads and the answers of turn/start and thread/resume.
type appServer struct {
	*codexrpctest.Server
	mu        sync.Mutex
	loaded    []string
	threads   map[string]map[string]any
	turnStart []*codexrpc.Error
	resume    []*codexrpc.Error
	// onTurnStart and onResume run before the answer of each turn/start and thread/resume.
	onTurnStart func()
	onResume    func()
}

func newAppServer(t *testing.T, path string, threads ...map[string]any) *appServer {
	f := &appServer{threads: map[string]map[string]any{}}
	for _, thread := range threads {
		id := thread["id"].(string)
		f.threads[id] = thread
		f.loaded = append(f.loaded, id)
	}
	f.Server = codexrpctest.Start(t, path, f.handle)
	return f
}

func threadObject(id, name, cwd string) map[string]any {
	thread := map[string]any{"id": id, "cwd": cwd, "ephemeral": false, "status": map[string]any{"type": "idle"}}
	if name == "" {
		thread["name"] = nil
	} else {
		thread["name"] = name
	}
	return thread
}

func (f *appServer) handle(call codexrpctest.Call) (any, *codexrpc.Error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var params struct {
		ThreadID string `json:"threadId"`
	}
	_ = json.Unmarshal(call.Params, &params)
	switch call.Method {
	case "thread/loaded/list":
		return map[string]any{"data": append([]string{}, f.loaded...), "nextCursor": nil}, nil
	case "thread/read":
		thread := f.threads[params.ThreadID]
		if thread == nil {
			return nil, &codexrpc.Error{Code: -32600, Message: "thread not loaded: " + params.ThreadID}
		}
		return map[string]any{"thread": thread}, nil
	case "thread/resume":
		if f.onResume != nil {
			f.onResume()
		}
		if len(f.resume) > 0 {
			next := f.resume[0]
			f.resume = f.resume[1:]
			if next != nil {
				return nil, next
			}
		}
		return map[string]any{"thread": f.threads[params.ThreadID]}, nil
	case "turn/start":
		if f.onTurnStart != nil {
			f.onTurnStart()
		}
		if len(f.turnStart) > 0 {
			next := f.turnStart[0]
			f.turnStart = f.turnStart[1:]
			if next != nil {
				return nil, next
			}
		}
		return map[string]any{"turn": map[string]any{"id": "turn-1", "status": "inProgress"}}, nil
	}
	return map[string]any{}, nil
}

func (f *appServer) script(turnStart []*codexrpc.Error, resume []*codexrpc.Error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.turnStart = turnStart
	f.resume = resume
}

// turnText returns the text of the n-th turn/start.
func (f *appServer) turnText(t *testing.T, n int) string {
	t.Helper()
	call, err := f.WaitCall(10*time.Second, n, codexrpctest.Method("turn/start"))
	if err != nil {
		t.Fatalf("no turn/start #%d", n)
	}
	var params struct {
		ThreadID string `json:"threadId"`
		Input    []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"input"`
	}
	if err := json.Unmarshal(call.Params, &params); err != nil || len(params.Input) != 1 || params.Input[0].Type != "text" {
		t.Fatalf("turn/start params = %s", call.Params)
	}
	return params.Input[0].Text
}

func (f *appServer) count(method string) int {
	n := 0
	for _, call := range f.Calls() {
		if !call.Answer && call.Method == method {
			n++
		}
	}
	return n
}

type world struct {
	hub      *testHub
	app      *appServer
	socket   string
	state    string
	clock    *fakeClock
	adapter  *Adapter
	cancel   context.CancelFunc
	done     chan struct{}
	logMu    sync.Mutex
	logLines []string
	token    string
}

func newWorld(t *testing.T, threads ...map[string]any) *world {
	t.Helper()
	dir, err := os.MkdirTemp("", "cx")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	w := &world{token: hostToken, hub: startHub(t), socket: filepath.Join(dir, "codex.sock"), state: filepath.Join(dir, "state", "codex-threads.json"), clock: newFakeClock()}
	w.app = newAppServer(t, w.socket, threads...)
	return w
}

func (w *world) start(t *testing.T) {
	t.Helper()
	w.adapter = New(Options{
		Socket:     w.socket,
		Link:       &linkclient.Client{BaseURL: w.hub.url, Token: w.token},
		StateFile:  w.state,
		Version:    "test",
		Clock:      w.clock,
		RetryDelay: func(int) time.Duration { return time.Millisecond },
		Log: func(line string) {
			w.logMu.Lock()
			w.logLines = append(w.logLines, line)
			w.logMu.Unlock()
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.done = make(chan struct{})
	go func() {
		defer close(w.done)
		_ = w.adapter.Run(ctx)
	}()
	t.Cleanup(func() {
		w.stop()
		w.logMu.Lock()
		defer w.logMu.Unlock()
		for _, line := range w.logLines {
			if strings.Contains(line, w.token) {
				t.Errorf("a log line holds the token: %q", line)
			}
		}
	})
}

func (w *world) stop() {
	if w.cancel == nil {
		return
	}
	w.cancel()
	<-w.done
	w.cancel = nil
}

func (w *world) logged(part string) bool {
	w.logMu.Lock()
	defer w.logMu.Unlock()
	for _, line := range w.logLines {
		if strings.Contains(line, part) {
			return true
		}
	}
	return false
}

func (w *world) savedThreads(t *testing.T) map[string]savedThread {
	t.Helper()
	threads, err := loadState(w.state)
	if err != nil {
		t.Fatal(err)
	}
	return threads
}

func TestLoadedAndStartedThreadsRegisterAndEphemeralOnesDoNot(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	w.start(t)
	info := w.hub.waitAgent(t, threadA, online)
	labels := info["labels"].(map[string]any)
	if info["name"] != "alpha" || info["kind"] != "session" || labels["harness"] != "codex" || info["cwd"] != "/work/a" {
		t.Fatalf("agent = %v", info)
	}
	ephemeral := threadObject(threadC, "", "/tmp")
	ephemeral["ephemeral"] = true
	w.app.Notify("thread/started", map[string]any{"thread": ephemeral})
	w.app.Notify("thread/started", map[string]any{"thread": threadObject(threadB, "", "/work/b")})
	info = w.hub.waitAgent(t, threadB, online)
	if info["name"] != "codex-000002" {
		t.Fatalf("default name = %v", info["name"])
	}
	if status, _ := w.hub.agent(threadC); status != 404 {
		t.Fatalf("ephemeral thread registered: %d", status)
	}
	w.app.Notify("thread/status/changed", map[string]any{"threadId": threadC, "status": map[string]any{"type": "active"}})
	w.app.Notify("thread/name/updated", map[string]any{"threadId": threadA, "threadName": "renamed"})
	w.hub.waitAgent(t, threadA, func(info map[string]any) bool { return info["name"] == "renamed" })
	if status, _ := w.hub.agent(threadC); status != 404 {
		t.Fatalf("ephemeral thread registered after a status change: %d", status)
	}
	if w.app.count("thread/read") != 1 {
		t.Fatalf("thread/read calls = %d, want 1 for the loaded thread", w.app.count("thread/read"))
	}
}

func TestStatusChangesSetTheStateAndFindResumedThreads(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	w.app.Notify("thread/status/changed", map[string]any{"threadId": threadA, "status": map[string]any{"type": "active", "activeFlags": []any{}}})
	w.hub.waitAgent(t, threadA, func(info map[string]any) bool { return info["state"] == "busy" })
	w.app.Notify("thread/status/changed", map[string]any{"threadId": threadA, "status": map[string]any{"type": "idle"}})
	w.hub.waitAgent(t, threadA, func(info map[string]any) bool { return info["state"] == "idle" })

	// A TUI resume sends no thread/started, only the status of the loaded thread.
	w.app.mu.Lock()
	w.app.threads[threadB] = threadObject(threadB, "beta", "/work/b")
	w.app.mu.Unlock()
	w.app.Notify("thread/status/changed", map[string]any{"threadId": threadB, "status": map[string]any{"type": "idle"}})
	info := w.hub.waitAgent(t, threadB, online)
	if info["name"] != "beta" {
		t.Fatalf("resumed thread = %v", info)
	}
}

func TestThePollFindsALoadedThreadWithoutAnEvent(t *testing.T) {
	w := newWorld(t)
	w.start(t)
	waitFor(t, "poll ticker", func() bool { return w.clock.liveTickers(DefaultPollEvery) == 1 })
	w.app.mu.Lock()
	w.app.threads[threadB] = threadObject(threadB, "beta", "/work/b")
	w.app.loaded = []string{threadB}
	w.app.mu.Unlock()
	w.clock.Advance(DefaultPollEvery)
	w.hub.waitAgent(t, threadB, online)
}

func TestAWakeStartsATurnWithTheRenderedTextAndIsDelivered(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	w.hub.session("pi-1")

	w.hub.send("pi-1", threadA, "stored only", map[string]any{"kind": "note", "wake": "never"})
	result := w.hub.send("pi-1", threadA, "please check the build", map[string]any{"kind": "task", "wake": "now", "deliveryWait": true})
	if got := delivery(result); got != "delivered" {
		t.Fatalf("delivery = %q", got)
	}
	taskID, messageID := taskOf(result)
	want := "Hub message from pi-1 (pi) kind=task messageId=" + messageID +
		": please check the build\n\nAnswer with the a2a reply tool and this messageId."
	if got := w.app.turnText(t, 0); got != want {
		t.Fatalf("turn text:\n%q\nwant\n%q", got, want)
	}
	if w.app.count("turn/start") != 1 {
		t.Fatal("the wake=never message started a turn")
	}
	if _, err := w.app.WaitCall(time.Second, 0, codexrpctest.Method("thread/resume")); err != nil {
		t.Fatal("no thread/resume before the first wake")
	}
	waitFor(t, "task working", func() bool { return w.hub.taskState(threadA, taskID) == "TASK_STATE_WORKING" })

	// A second wake while the adapter holds the subscription needs no resume. A running turn takes it as a
	// steer through turn/start.
	w.app.Notify("thread/status/changed", map[string]any{"threadId": threadA, "status": map[string]any{"type": "active"}})
	w.hub.waitAgent(t, threadA, func(info map[string]any) bool { return info["state"] == "busy" })
	w.hub.send("pi-1", threadA, "also this", map[string]any{"kind": "note", "wake": "now"})
	if got := w.app.turnText(t, 1); !strings.HasSuffix(got, ": also this") || strings.Contains(got, "Answer with") {
		t.Fatalf("steer text = %q", got)
	}
	if w.app.count("turn/steer") != 0 || w.app.count("thread/resume") != 1 {
		t.Fatalf("turn/steer %d, thread/resume %d", w.app.count("turn/steer"), w.app.count("thread/resume"))
	}

	// The idle after the wake drops the subscription, so the thread can unload.
	w.app.Notify("thread/status/changed", map[string]any{"threadId": threadA, "status": map[string]any{"type": "idle"}})
	if _, err := w.app.WaitCall(10*time.Second, 0, codexrpctest.Method("thread/unsubscribe")); err != nil {
		t.Fatal("no thread/unsubscribe after idle")
	}
}

func TestAnOverloadedAppServerIsRetriedBeforeTheAck(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	overloaded := &codexrpc.Error{Code: codexrpc.CodeOverloaded, Message: "Server overloaded; retry later."}
	w.app.script([]*codexrpc.Error{overloaded, overloaded}, []*codexrpc.Error{overloaded})
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	w.hub.session("pi-1")
	result := w.hub.send("pi-1", threadA, "hello", map[string]any{"kind": "note", "wake": "now", "deliveryWait": true})
	if got := delivery(result); got != "delivered" {
		t.Fatalf("delivery = %q", got)
	}
	if w.app.count("thread/resume") != 2 || w.app.count("turn/start") != 3 {
		t.Fatalf("thread/resume %d, turn/start %d", w.app.count("thread/resume"), w.app.count("turn/start"))
	}
}

func TestAReviewTurnHoldsTheWakeAndTheLaterOnesUntilItCompletes(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	review := &codexrpc.Error{Code: -32600, Message: "cannot steer a review turn", Data: json.RawMessage(`{"codexErrorInfo":"activeTurnNotSteerable"}`)}
	w.app.script([]*codexrpc.Error{review}, nil)
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	w.hub.session("pi-1")
	w.hub.send("pi-1", threadA, "first", map[string]any{"kind": "note", "wake": "now"})
	if got := w.app.turnText(t, 0); !strings.HasSuffix(got, ": first") {
		t.Fatalf("first try = %q", got)
	}
	second := w.hub.send("pi-1", threadA, "second", map[string]any{"kind": "note", "wake": "now", "deliveryWait": true})
	if got := delivery(second); got != "unconfirmed" {
		t.Fatalf("a wake behind a held one must not be delivered: %q", got)
	}
	if n := w.app.count("turn/start"); n != 1 {
		t.Fatalf("turn/start calls while held = %d", n)
	}
	w.app.Notify("turn/completed", map[string]any{"threadId": threadA, "turn": map[string]any{"id": "review", "status": "completed"}})
	if got := w.app.turnText(t, 1); !strings.HasSuffix(got, ": first") {
		t.Fatalf("retry = %q", got)
	}
	if got := w.app.turnText(t, 2); !strings.HasSuffix(got, ": second") {
		t.Fatalf("next wake = %q", got)
	}
}

func TestAnUnloadedThreadStaysOnlineAndIsResumedForAWake(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	w.hub.session("pi-1")
	w.hub.send("pi-1", threadA, "one", map[string]any{"kind": "note", "wake": "now"})
	w.app.turnText(t, 0)
	w.app.Notify("thread/status/changed", map[string]any{"threadId": threadA, "status": map[string]any{"type": "active"}})
	w.hub.waitAgent(t, threadA, func(info map[string]any) bool { return info["state"] == "busy" })
	w.app.Notify("thread/status/changed", map[string]any{"threadId": threadA, "status": map[string]any{"type": "notLoaded"}})
	w.app.Notify("thread/closed", map[string]any{"threadId": threadA})
	w.hub.waitAgent(t, threadA, func(info map[string]any) bool { return info["state"] == "idle" && online(info) })
	w.hub.send("pi-1", threadA, "two", map[string]any{"kind": "note", "wake": "now"})
	w.app.turnText(t, 1)
	if n := w.app.count("thread/resume"); n != 2 {
		t.Fatalf("thread/resume calls = %d, want a new resume after the unload", n)
	}
}

func TestAThreadWithoutActivityForSevenDaysGoesOfflineUntilItsNextEvent(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	waitFor(t, "sweep and poll tickers", func() bool {
		return w.clock.liveTickers(DefaultSweepEvery) == 1 && w.clock.liveTickers(DefaultPollEvery) == 1
	})
	waitFor(t, "state file", func() bool { _, ok := w.savedThreads(t)[threadA]; return ok })
	w.clock.Advance(DefaultOfflineAfter + DefaultSweepEvery)
	w.hub.waitAgent(t, threadA, func(info map[string]any) bool { return !online(info) })
	waitFor(t, "state file without the thread", func() bool { _, ok := w.savedThreads(t)[threadA]; return !ok })

	// The poll still lists the loaded thread. It does not count as activity.
	scans := w.app.count("thread/loaded/list")
	w.clock.Advance(DefaultPollEvery)
	waitFor(t, "poll", func() bool { return w.app.count("thread/loaded/list") > scans })
	if _, info := w.hub.agent(threadA); online(info) {
		t.Fatal("the poll registered a retired thread")
	}

	w.app.Notify("thread/status/changed", map[string]any{"threadId": threadA, "status": map[string]any{"type": "active"}})
	w.hub.waitAgent(t, threadA, func(info map[string]any) bool { return online(info) && info["state"] == "busy" })
}

func TestApprovalRequestsGetNoAnswer(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	w.app.Request(99, "item/commandExecution/requestApproval", map[string]any{"threadId": threadA, "command": "touch x"})
	w.app.Notify("thread/started", map[string]any{"thread": threadObject(threadB, "", "/work/b")})
	w.hub.waitAgent(t, threadB, online)
	for _, call := range w.app.Calls() {
		if call.Answer {
			t.Fatalf("the adapter answered a server request: %+v", call)
		}
	}
}

func TestATaskResultWakesTheThreadThatSentTheTask(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	w.hub.session("pi-1")
	result := w.hub.send(threadA, "pi-1", "run the tests", map[string]any{"kind": "task", "wake": "now"})
	taskID, _ := taskOf(result)
	status, out := w.hub.do("POST", "/link/pi-1/tasks/"+taskID, nil, map[string]any{"state": "working"})
	if status != 200 {
		t.Fatalf("working: %d %v", status, out)
	}
	status, out = w.hub.do("POST", "/link/pi-1/tasks/"+taskID, nil, map[string]any{"state": "completed", "text": "all green"})
	if status != 200 {
		t.Fatalf("completed: %d %v", status, out)
	}
	got := w.app.turnText(t, 0)
	prefix := "Hub message from pi-1 (pi) kind=result messageId="
	if !strings.HasPrefix(got, prefix) || !strings.HasSuffix(got, ": all green") || strings.Contains(got, taskID) {
		t.Fatalf("result text = %q", got)
	}
	if w.app.count("turn/start") != 1 {
		t.Fatal("the working update without text started a turn")
	}
}

func TestAReplacedLinkStopsAndRegistersAgainOnAnEventAfterAMinute(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	ctx, cancel := context.WithCancel(context.Background())
	res, err := w.hub.openStream(ctx, threadA)
	if err != nil || res.StatusCode != 200 {
		t.Fatalf("second stream: %v", err)
	}
	waitFor(t, "replaced log", func() bool { return w.logged("replaced by another client") })
	cancel()
	_ = res.Body.Close()
	w.hub.waitAgent(t, threadA, func(info map[string]any) bool { return !online(info) })

	w.app.Notify("thread/status/changed", map[string]any{"threadId": threadA, "status": map[string]any{"type": "idle"}})
	w.app.Notify("thread/started", map[string]any{"thread": threadObject(threadB, "", "/work/b")})
	w.hub.waitAgent(t, threadB, online)
	if _, info := w.hub.agent(threadA); online(info) {
		t.Fatal("the link came back within a minute")
	}
	w.clock.Advance(replacedRetry)
	w.app.Notify("thread/status/changed", map[string]any{"threadId": threadA, "status": map[string]any{"type": "idle"}})
	w.hub.waitAgent(t, threadA, online)
}

func TestAWakeWaitsForAnAppServerRestart(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	w.hub.session("pi-1")
	w.app.Close()
	if err := os.Remove(w.socket); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	w.hub.send("pi-1", threadA, "after restart", map[string]any{"kind": "note", "wake": "now"})
	restarted := newAppServer(t, w.socket, threadObject(threadA, "alpha", "/work/a"), threadObject(threadD, "delta", "/work/d"))
	if got := restarted.turnText(t, 0); !strings.HasSuffix(got, ": after restart") {
		t.Fatalf("text = %q", got)
	}
	if restarted.count("thread/resume") != 1 {
		t.Fatal("no new subscription after the restart")
	}
	w.hub.waitAgent(t, threadD, online)
}

func TestTheStateFileBringsBackUnloadedThreadsAfterARestart(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	old := w.clock.Now().Add(-DefaultOfflineAfter - time.Hour)
	if err := saveState(w.state, map[string]savedThread{threadC: {Name: "old", LastActivity: old}}, old.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	waitFor(t, "state file", func() bool { _, ok := w.savedThreads(t)[threadA]; return ok })
	if _, ok := w.savedThreads(t)[threadC]; ok {
		t.Fatal("an entry older than 7 days stayed in the state file")
	}
	if status, _ := w.hub.agent(threadC); status != 404 {
		t.Fatal("an entry older than 7 days registered")
	}
	info, err := os.Stat(w.state)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("state file mode: %v %v", info, err)
	}
	w.stop()

	w.app.mu.Lock()
	w.app.loaded = nil
	w.app.mu.Unlock()
	w.start(t)
	w.hub.waitAgent(t, threadA, func(info map[string]any) bool { return online(info) && info["name"] == "alpha" })
}

func TestACorruptStateFileIsLoggedAndWrittenNew(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	if err := os.MkdirAll(filepath.Dir(w.state), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(w.state, []byte("{nope"), 0o600); err != nil {
		t.Fatal(err)
	}
	w.start(t)
	waitFor(t, "corrupt log", func() bool { return w.logged("corrupt state file") })
	waitFor(t, "rewritten file", func() bool {
		_, err := loadState(w.state)
		_, ok := w.savedThreads(t)[threadA]
		return err == nil && ok
	})
}

func TestAGoneThreadGoesOfflineAndLeavesTheStateFile(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	w.app.script(nil, []*codexrpc.Error{{Code: -32600, Message: "no rollout found for thread id " + threadA}})
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	waitFor(t, "state file", func() bool { _, ok := w.savedThreads(t)[threadA]; return ok })
	w.app.mu.Lock()
	w.app.loaded = nil
	w.app.mu.Unlock()
	w.hub.session("pi-1")
	w.hub.send("pi-1", threadA, "hello", map[string]any{"kind": "note", "wake": "now"})
	w.hub.waitAgent(t, threadA, func(info map[string]any) bool { return !online(info) })
	waitFor(t, "state file without the thread", func() bool { _, ok := w.savedThreads(t)[threadA]; return !ok })
	if w.app.count("turn/start") != 0 {
		t.Fatal("turn/start on a gone thread")
	}
}

func TestANewLoadedThreadWithoutRolloutIsWokenWithoutResume(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	// Codex writes the rollout of a new thread at its first turn. Until then thread/resume fails.
	noRollout := &codexrpc.Error{Code: -32600, Message: "no rollout found for thread id " + threadA}
	w.app.script(nil, []*codexrpc.Error{noRollout, noRollout})
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	w.hub.session("pi-1")
	result := w.hub.send("pi-1", threadA, "first", map[string]any{"kind": "note", "wake": "now", "deliveryWait": true})
	if got := delivery(result); got != "delivered" {
		t.Fatalf("delivery = %q", got)
	}
	if got := w.app.turnText(t, 0); !strings.HasSuffix(got, ": first") {
		t.Fatalf("turn text = %q", got)
	}
	// The turn runs on an unsubscribed thread: its end sets the hub state and sends no thread/unsubscribe.
	w.app.Notify("thread/status/changed", map[string]any{"threadId": threadA, "status": map[string]any{"type": "active"}})
	w.app.Notify("thread/status/changed", map[string]any{"threadId": threadA, "status": map[string]any{"type": "idle"}})
	w.hub.waitAgent(t, threadA, func(info map[string]any) bool { return info["state"] == "idle" })
	if n := w.app.count("thread/unsubscribe"); n != 0 {
		t.Fatalf("thread/unsubscribe calls = %d", n)
	}
	// The next wake tries the resume again.
	w.hub.send("pi-1", threadA, "second", map[string]any{"kind": "note", "wake": "now"})
	w.app.turnText(t, 1)
	if n := w.app.count("thread/resume"); n != 2 {
		t.Fatalf("thread/resume calls = %d", n)
	}
}

func (w *world) unsubscribeDue(id string) bool {
	w.adapter.mu.Lock()
	th := w.adapter.threads[id]
	w.adapter.mu.Unlock()
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.unsubscribeDue
}

func TestAnIdleDuringAWakeUnsubscribesAfterTheWake(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	w.app.onTurnStart = func() {
		w.app.Notify("thread/status/changed", map[string]any{"threadId": threadA, "status": map[string]any{"type": "idle"}})
		// The wake holds its lock until this turn/start is answered, so the idle cannot unsubscribe by itself.
		waitFor(t, "the unsubscribe to be due", func() bool { return w.unsubscribeDue(threadA) })
	}
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	w.hub.session("pi-1")
	w.hub.send("pi-1", threadA, "quick", map[string]any{"kind": "note", "wake": "now"})
	w.app.turnText(t, 0)
	if _, err := w.app.WaitCall(10*time.Second, 0, codexrpctest.Method("thread/unsubscribe")); err != nil {
		t.Fatal("no thread/unsubscribe after an idle during the wake")
	}
}

func TestTheIdleOfTheResumeDoesNotUnsubscribeTheNewTurn(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	resumes := 0
	w.app.onResume = func() {
		resumes++
		if resumes == 1 {
			w.app.Notify("thread/status/changed", map[string]any{"threadId": threadA, "status": map[string]any{"type": "idle"}})
		}
	}
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	w.hub.session("pi-1")
	w.hub.send("pi-1", threadA, "one", map[string]any{"kind": "note", "wake": "now"})
	w.app.turnText(t, 0)
	// A second wake waits for any unsubscribe that the end of the first wake started.
	w.hub.send("pi-1", threadA, "two", map[string]any{"kind": "note", "wake": "now"})
	w.app.turnText(t, 1)
	if n := w.app.count("thread/unsubscribe"); n != 0 {
		t.Fatalf("thread/unsubscribe after the idle of the resume: %d", n)
	}
	if n := w.app.count("thread/resume"); n != 1 {
		t.Fatalf("thread/resume calls = %d", n)
	}
}

func TestNoUnsubscribeWhileTheThreadIsBusy(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	w.hub.session("pi-1")
	w.hub.send("pi-1", threadA, "go", map[string]any{"kind": "note", "wake": "now"})
	w.app.turnText(t, 0)
	w.app.Notify("thread/status/changed", map[string]any{"threadId": threadA, "status": map[string]any{"type": "active"}})
	w.hub.waitAgent(t, threadA, func(info map[string]any) bool { return info["state"] == "busy" })
	w.adapter.mu.Lock()
	th := w.adapter.threads[threadA]
	w.adapter.mu.Unlock()
	th.mu.Lock()
	th.unsubscribeDue = true
	th.mu.Unlock()
	th.unsubscribeIfIdle()
	// An unsubscribe holds wakeMu until its answer.
	th.wakeMu.Lock()
	th.wakeMu.Unlock() //nolint:staticcheck // SA2001: the empty section waits for a running unsubscribe.
	if n := w.app.count("thread/unsubscribe"); n != 0 {
		t.Fatalf("thread/unsubscribe while busy: %d", n)
	}
}

func TestTurnStartCarriesOnlyTheThreadAndTheInput(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	w.hub.session("pi-1")
	w.hub.send("pi-1", threadA, "hi", map[string]any{"kind": "note", "wake": "now"})
	call, err := w.app.WaitCall(10*time.Second, 0, codexrpctest.Method("turn/start"))
	if err != nil {
		t.Fatal(err)
	}
	var params map[string]json.RawMessage
	if err := json.Unmarshal(call.Params, &params); err != nil {
		t.Fatal(err)
	}
	if len(params) != 2 || params["threadId"] == nil || params["input"] == nil {
		t.Fatalf("turn/start params = %s", call.Params)
	}
}

func TestFiveUnknownErrorsLeaveTheEventUnackedAndTheThreadOffline(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	boom := &codexrpc.Error{Code: -32603, Message: "internal error"}
	w.app.script([]*codexrpc.Error{boom, boom, boom, boom, boom}, nil)
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	w.hub.session("pi-1")
	w.hub.send("pi-1", threadA, "stuck", map[string]any{"kind": "note", "wake": "now"})
	w.hub.waitAgent(t, threadA, func(info map[string]any) bool { return !online(info) })
	if n := w.app.count("turn/start"); n != 5 {
		t.Fatalf("turn/start calls = %d", n)
	}
	// The next event of the thread links it again, and the hub replays the unacked wake.
	w.app.Notify("thread/status/changed", map[string]any{"threadId": threadA, "status": map[string]any{"type": "idle"}})
	if got := w.app.turnText(t, 5); !strings.HasSuffix(got, ": stuck") {
		t.Fatalf("replayed text = %q", got)
	}
}

func TestAShuttingDownAppServerIsRetriedWithoutLimit(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	down := &codexrpc.Error{Code: -32600, Message: "server is shutting down", Data: json.RawMessage(`{"reason":"serverShuttingDown"}`)}
	w.app.script([]*codexrpc.Error{down, down, down, down, down, down}, nil)
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	w.hub.session("pi-1")
	result := w.hub.send("pi-1", threadA, "later", map[string]any{"kind": "note", "wake": "now", "deliveryWait": true})
	if got := delivery(result); got != "delivered" {
		t.Fatalf("delivery = %q", got)
	}
	if n := w.app.count("turn/start"); n != 7 {
		t.Fatalf("turn/start calls = %d", n)
	}
}

func TestSubAgentThreadsAreNotPeers(t *testing.T) {
	spawned := threadObject(threadB, "", "/work/a")
	spawned["source"] = map[string]any{"subAgent": map[string]any{"thread_spawn": map[string]any{"parent_thread_id": threadA, "depth": 1}}}
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"), spawned)
	w.start(t)
	w.hub.waitAgent(t, threadA, online)
	child := threadObject(threadC, "", "/work/a")
	child["parentThreadId"] = threadA
	w.app.Notify("thread/started", map[string]any{"thread": child})
	review := threadObject(threadD, "", "/work/a")
	review["source"] = map[string]any{"subAgent": "review"}
	w.app.mu.Lock()
	w.app.threads[threadD] = review
	w.app.mu.Unlock()
	w.app.Notify("thread/status/changed", map[string]any{"threadId": threadD, "status": map[string]any{"type": "active"}})
	waitFor(t, "thread/read of the status event", func() bool { return w.app.count("thread/read") >= 3 })
	w.app.Notify("thread/started", map[string]any{"thread": threadObject("01a1df2b-0000-7000-8000-000000000005", "", "/work/e")})
	w.hub.waitAgent(t, "01a1df2b-0000-7000-8000-000000000005", online)
	for _, id := range []string{threadB, threadC, threadD} {
		if status, _ := w.hub.agent(id); status != 404 {
			t.Fatalf("sub-agent thread %s registered: %d", id, status)
		}
	}
}

func TestTheTokenStaysOutOfTheLog(t *testing.T) {
	w := newWorld(t, threadObject(threadA, "alpha", "/work/a"))
	w.token = "refused-token-0123456789"
	w.start(t)
	waitFor(t, "a link error in the log", func() bool { return w.logged("401") })
}

func TestOnlyUserThreadsArePeers(t *testing.T) {
	cases := []struct {
		source       any
		threadSource any
		peer         bool
	}{
		{"cli", nil, true},
		{"vscode", "user", true},
		{"appServer", nil, true},
		{"exec", "user", true},
		{map[string]any{"custom": "atlas"}, nil, true},
		{"unknown", nil, false},
		{"cli", "subagent", false},
		{"cli", "guardian_review", false},
		{"cli", "memory_consolidation", false},
		{"vscode", "some_feature", false},
	}
	var threads []map[string]any
	for i, c := range cases {
		thread := threadObject(fmt.Sprintf("01a1df2c-0000-7000-8000-%012d", i), "", "/work/s")
		thread["source"] = c.source
		thread["threadSource"] = c.threadSource
		threads = append(threads, thread)
	}
	w := newWorld(t, threads...)
	w.start(t)
	seen := func(id string) (known, notPeer bool) {
		w.adapter.mu.Lock()
		defer w.adapter.mu.Unlock()
		return w.adapter.threads[id] != nil, w.adapter.notPeers[id]
	}
	for i, c := range cases {
		id := threads[i]["id"].(string)
		waitFor(t, "a decision on thread "+id, func() bool {
			known, notPeer := seen(id)
			return known || notPeer
		})
		if known, _ := seen(id); known != c.peer {
			t.Fatalf("source %v, threadSource %v: peer = %v", c.source, c.threadSource, known)
		}
	}
}

func TestDefaultNamesOfThreadsFromOneMillisecondDiffer(t *testing.T) {
	// UUIDv7: the first 12 hex digits are the time, the last ones are random.
	a, b := "01a1dfdb-8025-7a11-8c3e-5f1b2a9d4e70", "01a1dfdb-8025-7b22-9d4f-6a2c3b8e5f81"
	if got := defaultName(a); got != "codex-9d4e70" {
		t.Fatalf("defaultName(a) = %q", got)
	}
	if got := defaultName(b); got != "codex-8e5f81" {
		t.Fatalf("defaultName(b) = %q", got)
	}
}
