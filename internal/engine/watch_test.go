package engine

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/orswn/a2a-hub/internal/jsonx"
)

type eventLog struct {
	mu     sync.Mutex
	events []TaskEvent
	final  chan struct{}
}

func newEventLog() *eventLog { return &eventLog{final: make(chan struct{}, 1)} }

func (l *eventLog) add(event TaskEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
	if event.State == StateCompleted {
		select {
		case l.final <- struct{}{}:
		default:
		}
	}
}

func (l *eventLog) states() []State {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []State
	for _, event := range l.events {
		out = append(out, event.State)
	}
	return out
}

// completeEachTask completes each task of agentID as soon as its message event is committed. inline does it
// in the link listener, so the completion commits before Send returns to its caller. Otherwise a goroutine
// does it after a short random pause, so the completion races the caller. Wait stops it and waits for it.
func completeEachTask(t *testing.T, h *testEnv, agentID string, inline bool) (wait func()) {
	complete := func(id string) {
		if _, err := h.engine.ApplyChange(id, TaskChange{State: StateCompleted}); err != nil {
			t.Error(err)
		}
	}
	ids := make(chan string, 1024)
	off := h.engine.OnLink(func(agent string, event LinkEvent) {
		if agent != agentID || event.Type != "message" {
			return
		}
		v, err := jsonx.Parse(event.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		if id, _ := v.(*jsonx.Object).String("taskId"); id == "" {
			return
		} else if inline {
			complete(id)
		} else {
			ids <- id
		}
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for id := range ids {
			for range rand.IntN(64) {
				runtime.Gosched()
			}
			complete(id)
		}
	}()
	return func() {
		off()
		close(ids)
		<-done
	}
}

// In the TypeScript hub the listeners of a call run inside the call, so a listener added after Send returns
// gets none of the send's events. Here another goroutine may still drain them, so the listeners must be the
// ones of the commit.
func TestTaskListenerAddedAfterSendGetsNoneOfItsEvents(t *testing.T) {
	h := setup(t)
	h.register("a1")
	h.register("a2")
	entered, hold := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer h.engine.OnLink(func(agent string, _ LinkEvent) {
		if agent == "a2" {
			once.Do(func() {
				close(entered)
				<-hold
			})
		}
	})()
	draining := make(chan struct{})
	go func() {
		defer close(draining)
		if _, err := h.engine.Send(SendInput{AgentID: "a2", Caller: fromHermes, Message: message("hold")}); err != nil {
			t.Error(err)
		}
	}()
	<-entered

	sent := h.send("a1", fromHermes, message("x"))
	log := newEventLog()
	defer h.engine.OnTask(sent.Task.ID, log.add)()
	close(hold)
	<-draining
	got(h.engine.ApplyChange(sent.Task.ID, TaskChange{State: StateCompleted})).ok(t)
	if states := log.states(); len(states) != 1 || states[0] != StateCompleted {
		t.Errorf("listener got %q, want only the completed status", states)
	}
}

func TestSendWatchSeesTheFinalStatusOnceAndNeverTheSendItself(t *testing.T) {
	for _, inline := range []bool{true, false} {
		t.Run(fmt.Sprintf("inline=%v", inline), func(t *testing.T) { testSendWatch(t, inline) })
	}
}

func testSendWatch(t *testing.T, inline bool) {
	h := setup(t)
	h.register("a1")
	wait := completeEachTask(t, h, "a1", inline)
	logs := make([]*eventLog, 200)
	for i := range logs {
		logs[i] = newEventLog()
		sent, err := h.engine.Send(SendInput{
			AgentID: "a1", Caller: fromHermes, Message: message(fmt.Sprintf("w%d", i)), Watch: logs[i].add,
		})
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-logs[i].final:
		case <-time.After(5 * time.Second):
			t.Fatalf("send %d: the watch missed the final status, got %q", i, logs[i].states())
		}
		sent.Unwatch()
	}
	wait()
	for i, log := range logs {
		if states := log.states(); len(states) != 1 || states[0] != StateCompleted {
			t.Errorf("send %d: watch got %q, want only the completed status", i, states)
		}
	}
}

func TestSendWatchIsANoOpForAMessageWithoutATask(t *testing.T) {
	h := setup(t)
	h.register("a1")
	log := newEventLog()
	sent := got(h.engine.Send(SendInput{
		AgentID: "a1", Caller: fromHermes, Message: withMeta(message("n"), "kind", "note"), Watch: log.add,
	})).ok(t)
	if sent.Task != nil || sent.Unwatch == nil {
		t.Fatalf("note = %+v", sent)
	}
	sent.Unwatch()
}

func TestWatchTaskReadsAndSubscribesInOneCall(t *testing.T) {
	h := setup(t)
	h.register("a1")
	wait := completeEachTask(t, h, "a1", false)
	type watched struct {
		snapshot State
		log      *eventLog
		off      func()
	}
	runs := make([]watched, 200)
	for i := range runs {
		id := h.taskID("a1", fromHermes, message(fmt.Sprintf("r%d", i)))
		log := newEventLog()
		task, off, err := h.engine.WatchTask("a1", fromHermes, id, nil, log.add)
		if err != nil {
			t.Fatal(err)
		}
		runs[i] = watched{task.Status.State, log, off}
		if task.Status.State != StateCompleted {
			select {
			case <-log.final:
			case <-time.After(5 * time.Second):
				t.Fatalf("task %d: the watch missed the final status after snapshot %s", i, task.Status.State)
			}
		}
	}
	wait()
	completedFirst := 0
	for i, run := range runs {
		run.off()
		states := run.log.states()
		if run.snapshot == StateCompleted {
			completedFirst++
			if len(states) != 0 {
				t.Errorf("task %d: snapshot is completed, watch also got %q", i, states)
			}
		} else if len(states) != 1 || states[0] != StateCompleted {
			t.Errorf("task %d: snapshot %s, watch got %q, want only the completed status", i, run.snapshot, states)
		}
	}
	t.Logf("%d of %d snapshots were already completed", completedFirst, len(runs))
	if _, _, err := h.engine.WatchTask("a1", Caller{Principal: venus}, "id-0001", nil, func(TaskEvent) {}); err == nil ||
		err.Error() != "Task not found: id-0001" {
		t.Errorf("foreign task = %v", err)
	}
	if n := h.engine.ListenerCount("id-0001"); n != 0 {
		t.Errorf("listeners after a refused watch = %d", n)
	}
}
