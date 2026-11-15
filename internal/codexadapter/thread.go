package codexadapter

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/orswn/a2a-hub/internal/codexrpc"
	"github.com/orswn/a2a-hub/internal/linkclient"
)

// thread serves one Codex thread as a hub session agent. Three goroutines run per thread: the link reader,
// the worker that handles events in seq order and acks them, and the state poster.
type thread struct {
	a  *Adapter
	id string

	ctx    context.Context
	cancel context.CancelFunc

	mu          sync.Mutex
	name        string
	cwd         string
	activity    time.Time
	subscribed  bool
	desired     string
	posted      string
	registered  bool
	linkStopped bool
	stoppedAt   time.Time
	doneCh      chan struct{}
	queue       []linkclient.Event
	queueNudge  chan struct{}
	stateNudge  chan struct{}
	lastSeq     *int64

	// wakeMu covers one wake: resume and turn/start. An unsubscribe waits for the end of the wake.
	wakeMu sync.Mutex
	// unsubscribeDue is set by an idle in the read loop, so it follows the wire order, and is cleared before
	// each turn/start. A wake reads it after its Unlock, so an idle during a wake is never lost.
	unsubscribeDue bool
}

// defaultName takes the last 6 hex digits of the thread id. Codex thread ids are UUIDv7: their first digits
// are a time that threads of the same hours share.
func defaultName(id string) string {
	short := strings.ReplaceAll(id, "-", "")
	if len(short) > 6 {
		short = short[len(short)-6:]
	}
	return "codex-" + short
}

func newThread(a *Adapter, info threadInfo, activity time.Time) *thread {
	ctx, cancel := context.WithCancel(a.ctx)
	name := info.Name
	if name == "" {
		name = defaultName(info.ID)
	}
	return &thread{
		a: a, id: info.ID, ctx: ctx, cancel: cancel, name: name, cwd: info.Cwd, activity: activity,
		desired: "idle", doneCh: make(chan struct{}),
		queueNudge: make(chan struct{}, 1), stateNudge: make(chan struct{}, 1),
	}
}

func (t *thread) start() {
	go t.readLoop()
	go t.workLoop()
	go t.stateLoop()
}

func (t *thread) stop() { t.cancel() }

// savedLocked is the state file entry. The caller holds a.mu.
func (t *thread) savedLocked() savedThread {
	t.mu.Lock()
	defer t.mu.Unlock()
	return savedThread{Name: t.name, Cwd: t.cwd, LastActivity: t.activity}
}

func (t *thread) touch(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if now.After(t.activity) {
		t.activity = now
	}
}

func (t *thread) lastActivity() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.activity
}

func (t *thread) setSubscribed(on bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.subscribed = on
}

func (t *thread) isSubscribed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.subscribed
}

// restartIfStopped starts the link reader again after a replaced or refused link, at most once per
// replacedRetry.
func (t *thread) restartIfStopped(now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.linkStopped || now.Sub(t.stoppedAt) < replacedRetry || t.ctx.Err() != nil {
		return
	}
	t.linkStopped = false
	go t.readLoop()
}

func (t *thread) linkStop(why string) {
	t.mu.Lock()
	t.linkStopped = true
	t.registered = false
	t.stoppedAt = t.a.opts.Clock.Now()
	t.mu.Unlock()
	t.a.log("thread " + t.id + " link stopped: " + why)
}

// setStatus maps a Codex thread status to the hub state: active is busy, all else idle. An unloaded thread
// stays online, so a wake can still reach it.
func (t *thread) setStatus(status string) {
	state := "idle"
	if status == "active" {
		state = "busy"
	}
	t.mu.Lock()
	t.desired = state
	if status == "notLoaded" {
		t.subscribed = false
	}
	if status == "idle" {
		t.unsubscribeDue = true
	}
	t.mu.Unlock()
	select {
	case t.stateNudge <- struct{}{}:
	default:
	}
	if state == "idle" {
		t.turnDone()
		if status == "idle" {
			t.unsubscribeIfIdle()
		}
	}
}

// turnDone wakes a wake that waits for the end of a review or compact turn.
func (t *thread) turnDone() {
	t.mu.Lock()
	defer t.mu.Unlock()
	close(t.doneCh)
	t.doneCh = make(chan struct{})
}

func (t *thread) done() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.doneCh
}

// unsubscribeIfIdle drops the subscription that a wake took, so the app-server can unload the idle thread
// as usual. It runs a due unsubscribe only when no wake runs; a wake runs it at its end. It does not block:
// the call runs in its own goroutine, which holds wakeMu until the answer, so a later wake waits for it.
func (t *thread) unsubscribeIfIdle() {
	if !t.wakeMu.TryLock() {
		return
	}
	t.mu.Lock()
	due, idle, subscribed := t.unsubscribeDue, t.desired == "idle", t.subscribed
	t.unsubscribeDue = false
	t.mu.Unlock()
	if !due || !idle || !subscribed {
		t.wakeMu.Unlock()
		return
	}
	// The caller may hold a.mu, so only the goroutine reads the connection.
	go func() {
		defer t.wakeMu.Unlock()
		t.a.mu.Lock()
		conn := t.a.conn
		t.a.mu.Unlock()
		if conn == nil {
			return
		}
		if err := conn.Call(t.ctx, "thread/unsubscribe", map[string]any{"threadId": t.id}, nil); err != nil {
			if t.ctx.Err() == nil {
				t.a.log("thread/unsubscribe of " + t.id + " failed: " + err.Error())
			}
			return
		}
		t.setSubscribed(false)
	}()
}

func (t *thread) rename(name string) {
	if name == "" {
		name = defaultName(t.id)
	}
	t.mu.Lock()
	changed := t.name != name
	t.name = name
	registered := t.registered
	t.mu.Unlock()
	t.a.persist(t)
	if changed && registered {
		go func() {
			if _, err := t.register(t.ctx); err != nil && t.ctx.Err() == nil {
				t.a.log("register of " + t.id + " failed: " + err.Error())
			}
		}()
	}
}

func (t *thread) register(ctx context.Context) (linkclient.RegisterResult, error) {
	t.mu.Lock()
	body := linkclient.RegisterBody{
		Agent: t.id, Name: t.name, Cwd: t.cwd, Labels: map[string]string{"harness": Harness}, Protocol: linkclient.Protocol,
	}
	t.mu.Unlock()
	return t.a.opts.Link.Register(ctx, body)
}

// readLoop registers the thread and reads its link events into the worker queue. It reconnects on failure
// and stops for good when the hub refuses the link or another client replaced it.
func (t *thread) readLoop() {
	ctx := t.ctx
	failures := 0
	for ctx.Err() == nil {
		_, err := t.register(ctx)
		var stream *linkclient.Stream
		if err == nil {
			t.mu.Lock()
			after := t.lastSeq
			t.mu.Unlock()
			stream, err = t.a.opts.Link.Events(ctx, t.id, after)
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if linkclient.Fatal(err) {
				t.linkStop(err.Error())
				return
			}
			failures++
			if failures == 1 || failures%10 == 0 {
				t.a.log("link of " + t.id + " down: " + err.Error())
			}
			if !t.a.sleep(ctx, failures) {
				return
			}
			continue
		}
		failures = 0
		t.mu.Lock()
		t.registered = true
		t.posted = ""
		t.mu.Unlock()
		select {
		case t.stateNudge <- struct{}{}:
		default:
		}
		reason := t.consume(stream)
		t.mu.Lock()
		t.registered = false
		t.mu.Unlock()
		if reason == "replaced" {
			t.linkStop("replaced by another client")
			return
		}
		if ctx.Err() != nil {
			return
		}
		failures++
		if !t.a.sleep(ctx, failures) {
			return
		}
	}
}

func (t *thread) consume(stream *linkclient.Stream) string {
	defer func() { _ = stream.Close() }()
	idle := time.AfterFunc(t.a.opts.StreamIdle, func() { _ = stream.Close() })
	defer idle.Stop()
	stop := context.AfterFunc(t.ctx, func() { _ = stream.Close() })
	defer stop()
	first := true
	for {
		event, err := stream.Next(func() { idle.Reset(t.a.opts.StreamIdle) })
		if err != nil {
			return "stream ended"
		}
		if event.Type == "replaced" {
			return "replaced"
		}
		if event.Seq < 0 {
			continue
		}
		t.mu.Lock()
		// The hub replays only events after Last-Event-ID. An older first id means that its data was reset.
		if first && t.lastSeq != nil && event.Seq < *t.lastSeq {
			t.lastSeq = nil
		}
		first = false
		if t.lastSeq != nil && event.Seq <= *t.lastSeq {
			t.mu.Unlock()
			continue
		}
		seq := event.Seq
		t.lastSeq = &seq
		t.queue = append(t.queue, event)
		t.mu.Unlock()
		select {
		case t.queueNudge <- struct{}{}:
		default:
		}
	}
}

func (t *thread) pop() (linkclient.Event, bool) {
	for {
		t.mu.Lock()
		if len(t.queue) > 0 {
			event := t.queue[0]
			t.queue = t.queue[1:]
			t.mu.Unlock()
			return event, true
		}
		t.mu.Unlock()
		select {
		case <-t.queueNudge:
		case <-t.ctx.Done():
			return linkclient.Event{}, false
		}
	}
}

// workLoop handles the events of the thread in seq order. Acks are cumulative, so an event that waits holds
// back the acks of the later ones.
func (t *thread) workLoop() {
	for {
		event, ok := t.pop()
		if !ok {
			return
		}
		if err := t.handle(event); err != nil {
			if t.ctx.Err() != nil {
				return
			}
			go t.a.drop(t, err.Error())
			return
		}
		t.ack(event.Seq)
	}
}

func (t *thread) ack(seq int64) {
	for n := 1; t.ctx.Err() == nil; n++ {
		err := t.a.opts.Link.Ack(t.ctx, t.id, seq)
		if err == nil || linkclient.Fatal(err) || n == maxCallFailures {
			if err != nil {
				t.a.log("ack of " + t.id + " failed: " + err.Error())
			}
			return
		}
		if !t.a.sleep(t.ctx, n) {
			return
		}
	}
}

func (t *thread) stateLoop() {
	for {
		select {
		case <-t.stateNudge:
		case <-t.ctx.Done():
			return
		}
		t.mu.Lock()
		desired, posted, registered := t.desired, t.posted, t.registered
		t.mu.Unlock()
		if !registered || desired == posted {
			continue
		}
		if err := t.a.opts.Link.State(t.ctx, t.id, desired); err != nil {
			if t.ctx.Err() == nil {
				t.a.log("state of " + t.id + " failed: " + err.Error())
			}
			continue
		}
		t.mu.Lock()
		t.posted = desired
		again := t.desired != desired
		t.mu.Unlock()
		if again {
			select {
			case t.stateNudge <- struct{}{}:
			default:
			}
		}
	}
}

// handle runs one link event. An error means that the thread cannot take the wake; the event stays unacked.
func (t *thread) handle(event linkclient.Event) error {
	switch event.Type {
	case "message":
		msg, ok := decode[messageEvent](event.Payload)
		if !ok || msg.Wake != "now" {
			return nil
		}
		from := t.a.senderOf(t.ctx, msg.From.Agent, msg.From.Name, msg.From.Kind)
		text := Render(from, msg.Kind, msg.MessageID, msg.Text, msg.TaskID != "")
		if err := t.wake(text); err != nil {
			return err
		}
		if msg.TaskID != "" && msg.TaskState == "submitted" {
			err := t.a.opts.Link.TaskState(t.ctx, t.id, msg.TaskID, "working")
			var httpErr *linkclient.HTTPError
			if err != nil && (!errors.As(err, &httpErr) || httpErr.Status != http.StatusConflict) {
				t.a.log("working state of task " + msg.TaskID + " failed: " + err.Error())
			}
		}
	case "update":
		update, ok := decode[updateEvent](event.Payload)
		if !ok || update.MessageID == "" || update.Text == "" || !wakingUpdateStates[update.State] {
			return nil
		}
		if update.Kind == "" {
			update.Kind = "result"
		}
		from := t.a.senderOf(t.ctx, update.From.Agent, update.From.Name, "")
		text := Render(from, update.Kind, update.MessageID, update.Text, false)
		return t.wake(text)
	}
	return nil
}

// wake gives the text to the thread as user input: thread/resume when the adapter is not subscribed, then
// turn/start, which steers a running turn. It retries while the app-server is down or overloaded, and after
// the end of a review or compact turn.
func (t *thread) wake(text string) error {
	ctx := t.ctx
	t.wakeMu.Lock()
	defer func() {
		t.wakeMu.Unlock()
		t.unsubscribeIfIdle()
	}()
	failures, tries := 0, 0
	retry := func(err error) error {
		failures++
		if !codexrpc.IsCode(err, codexrpc.CodeOverloaded) && !errors.Is(err, codexrpc.ErrClosed) && !shuttingDown(err) {
			tries++
			t.a.log("wake of " + t.id + " failed: " + err.Error())
			if tries >= maxCallFailures {
				return err
			}
		}
		if !t.a.sleep(ctx, failures) {
			return ctx.Err()
		}
		return nil
	}
	for {
		conn, err := t.a.waitConn(ctx)
		if err != nil {
			return err
		}
		if !t.isSubscribed() {
			err := conn.Call(ctx, "thread/resume", map[string]any{"threadId": t.id, "excludeTurns": true}, nil)
			switch {
			case gone(err):
				// A new thread has no rollout until its first turn. It is loaded, and turn/start works on it
				// without a subscription.
				loaded, loadErr := loadedThreads(ctx, conn)
				if loadErr != nil {
					if err := retry(loadErr); err != nil {
						return err
					}
					continue
				}
				if !slices.Contains(loaded, t.id) {
					return errors.New("thread is gone: " + err.Error())
				}
			case err != nil:
				if err := retry(err); err != nil {
					return err
				}
				continue
			default:
				t.setSubscribed(true)
			}
		}
		done := t.done()
		t.mu.Lock()
		t.unsubscribeDue = false
		t.mu.Unlock()
		input := []map[string]any{{"type": "text", "text": text, "text_elements": []any{}}}
		err = conn.Call(ctx, "turn/start", map[string]any{"threadId": t.id, "input": input}, nil)
		switch {
		case err == nil:
			t.touch(t.a.opts.Clock.Now())
			t.a.persist(t)
			return nil
		case busyTurn(err):
			select {
			case <-done:
			case <-ctx.Done():
				return ctx.Err()
			}
		case gone(err):
			t.setSubscribed(false)
			if err := retry(err); err != nil {
				return err
			}
		default:
			if err := retry(err); err != nil {
				return err
			}
		}
	}
}
