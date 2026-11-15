package link

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orswn/a2a-hub/internal/auth"
	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/jsonx"
	"github.com/orswn/a2a-hub/internal/store"
)

func setup(t *testing.T) *engine.Engine {
	t.Helper()
	db, err := store.Open(context.Background(), store.Memory)
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
	if _, err := e.Register(auth.Principal{Name: "mars", Kind: auth.KindHost},
		jsonx.NewObject("agent", "a1", "protocol", float64(engine.HubProtocol))); err != nil {
		t.Fatal(err)
	}
	return e
}

// recorder is a ResponseWriter that notes when the head went out and can hold a write that contains slow.
type recorder struct {
	header   http.Header
	slow     string
	mu       sync.Mutex
	body     bytes.Buffer
	headerAt time.Time
	wrote    chan struct{}
}

func newRecorder(slow string) *recorder {
	return &recorder{header: http.Header{}, slow: slow, wrote: make(chan struct{}, 64)}
}

func (r *recorder) Header() http.Header { return r.header }

func (r *recorder) WriteHeader(int) {
	r.mu.Lock()
	r.headerAt = time.Now()
	r.mu.Unlock()
}

func (r *recorder) Write(p []byte) (int, error) {
	if r.slow != "" && bytes.Contains(p, []byte(r.slow)) {
		time.Sleep(200 * time.Millisecond)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.body.Write(p)
	select {
	case r.wrote <- struct{}{}:
	default:
	}
	return len(p), nil
}

func (r *recorder) text() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.body.String()
}

func serve(hub *Hub, w http.ResponseWriter) (cancel func(), done chan time.Time) {
	ctx, cancel := context.WithCancel(context.Background())
	done = make(chan time.Time, 1)
	go func() {
		hub.Serve(w, httptest.NewRequestWithContext(ctx, http.MethodGet, "/link/a1/events", nil), "a1", nil)
		done <- time.Now()
	}()
	return cancel, done
}

func online(t *testing.T, e *engine.Engine) bool {
	t.Helper()
	row, err := e.Agent("a1")
	if err != nil {
		t.Fatal(err)
	}
	return row.Online == 1
}

func TestASecondStreamStartsAfterTheReplacedStreamEnded(t *testing.T) {
	e := setup(t)
	hub := New(e, Options{PresenceGrace: time.Minute, Heartbeat: time.Minute})
	defer hub.Stop()
	first := newRecorder("event: replaced")
	cancelFirst, firstDone := serve(hub, first)
	defer cancelFirst()
	<-first.wrote
	second := newRecorder("")
	cancelSecond, secondDone := serve(hub, second)
	ended := <-firstDone
	<-second.wrote
	second.mu.Lock()
	headerAt := second.headerAt
	second.mu.Unlock()
	if headerAt.Before(ended) {
		t.Errorf("the second stream wrote its head %v before the first stream ended", ended.Sub(headerAt))
	}
	if !strings.HasSuffix(first.text(), "event: replaced\ndata: {}\n\n") {
		t.Errorf("first stream = %q", first.text())
	}
	if !online(t, e) {
		t.Error("agent is offline while the second stream is open")
	}
	cancelSecond()
	<-secondDone
}

func TestOfflineNowDuringTheReplaceWaitLeavesTheAgentOffline(t *testing.T) {
	e := setup(t)
	hub := New(e, Options{PresenceGrace: 100 * time.Millisecond, Heartbeat: time.Minute})
	defer hub.Stop()
	first := newRecorder("event: replaced")
	cancelFirst, firstDone := serve(hub, first)
	defer cancelFirst()
	<-first.wrote
	second := newRecorder("")
	cancelSecond, secondDone := serve(hub, second)
	defer cancelSecond()
	// The first stream holds its replaced frame for 200 ms, so the second stream is in its replace wait.
	time.Sleep(50 * time.Millisecond)
	if err := hub.OfflineNow("a1"); err != nil {
		t.Fatal(err)
	}
	<-firstDone
	select {
	case <-secondDone:
	case <-time.After(3 * time.Second):
		t.Fatal("second stream did not end after OfflineNow")
	}
	time.Sleep(300 * time.Millisecond)
	if online(t, e) {
		t.Error("agent is online after OfflineNow with no stream")
	}
}

func TestAClosedStreamMarksTheAgentOfflineAfterTheGrace(t *testing.T) {
	e := setup(t)
	// The grace is long enough for a loaded machine to read the row before it ends.
	hub := New(e, Options{PresenceGrace: time.Second, Heartbeat: time.Minute})
	defer hub.Stop()
	w := newRecorder("")
	cancel, done := serve(hub, w)
	<-w.wrote
	if !online(t, e) {
		t.Fatal("agent is offline while its stream is open")
	}
	cancel()
	<-done
	if !online(t, e) {
		t.Error("agent went offline before the grace")
	}
	deadline := time.Now().Add(5 * time.Second)
	for online(t, e) {
		if time.Now().After(deadline) {
			t.Fatal("agent is still online after the grace")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestOfflineNowEndsTheStreamAndMarksTheAgentOffline(t *testing.T) {
	e := setup(t)
	hub := New(e, Options{PresenceGrace: time.Minute, Heartbeat: time.Minute})
	defer hub.Stop()
	w := newRecorder("")
	cancel, done := serve(hub, w)
	defer cancel()
	<-w.wrote
	if err := hub.OfflineNow("a1"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stream still open")
	}
	if online(t, e) {
		t.Error("agent is online")
	}
}

func TestHeartbeatWritesAComment(t *testing.T) {
	e := setup(t)
	hub := New(e, Options{PresenceGrace: time.Minute, Heartbeat: 20 * time.Millisecond})
	defer hub.Stop()
	w := newRecorder("")
	cancel, done := serve(hub, w)
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(w.text(), ": hb\n\n") {
		if time.Now().After(deadline) {
			t.Fatalf("no heartbeat: %q", w.text())
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
}
