package engine

import (
	"sync"
	"testing"
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
