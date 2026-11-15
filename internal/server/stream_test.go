package server

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

// deadlineWriter notes each write deadline and each write in order.
type deadlineWriter struct {
	header http.Header
	steps  []string
	body   strings.Builder
}

func (w *deadlineWriter) Header() http.Header { return w.header }
func (w *deadlineWriter) WriteHeader(int)     {}
func (w *deadlineWriter) Flush()              {}

func (w *deadlineWriter) Write(p []byte) (int, error) {
	w.steps = append(w.steps, "write")
	return w.body.Write(p)
}

func (w *deadlineWriter) SetWriteDeadline(at time.Time) error {
	if left := time.Until(at); left < 50*time.Second || left > 70*time.Second {
		w.steps = append(w.steps, "bad deadline")
		return nil
	}
	w.steps = append(w.steps, "deadline")
	return nil
}

func TestEachStreamWriteHasAWriteDeadline(t *testing.T) {
	s := &Server{opts: Options{Heartbeat: time.Minute}, log: func(string) {}}
	w := &deadlineWriter{header: http.Header{}}
	task := &a2a.Task{ID: "t1", ContextID: "c1", Status: a2a.TaskStatus{State: a2a.TaskStateCompleted}}
	events := func(yield func(a2a.Event, error) bool) { yield(task, nil) }
	ctx, cancel := context.WithCancel(context.Background())
	s.writeStream(ctx, cancel, w, float64(1), events)
	if !strings.Contains(w.body.String(), `"id":"t1"`) {
		t.Fatalf("body %q", w.body.String())
	}
	if len(w.steps) < 4 {
		t.Fatalf("steps %v", w.steps)
	}
	for i := 0; i < len(w.steps); i += 2 {
		if w.steps[i] != "deadline" || i+1 >= len(w.steps) || w.steps[i+1] != "write" {
			t.Fatalf("steps %v", w.steps)
		}
	}
}

// frameWriter keeps the stream body and tells waiters about each heartbeat frame.
type frameWriter struct {
	header     http.Header
	mu         sync.Mutex
	body       strings.Builder
	heartbeats chan struct{}
}

func (w *frameWriter) Header() http.Header { return w.header }
func (w *frameWriter) WriteHeader(int)     {}
func (w *frameWriter) Flush()              {}

func (w *frameWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if string(p) == ": hb\n\n" {
		select {
		case w.heartbeats <- struct{}{}:
		default:
		}
	}
	return w.body.Write(p)
}

func (w *frameWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.String()
}

func TestAnA2AStreamSendsAHeartbeatCommentWhileItWaitsForEvents(t *testing.T) {
	s := &Server{opts: Options{Heartbeat: time.Millisecond}, log: func(string) {}}
	w := &frameWriter{header: http.Header{}, heartbeats: make(chan struct{}, 1)}
	working := &a2a.Task{ID: "t1", ContextID: "c1", Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}
	done := &a2a.TaskStatusUpdateEvent{TaskID: "t1", ContextID: "c1", Status: a2a.TaskStatus{State: a2a.TaskStateCompleted}}
	// The timeout only ends a stream that never sends a heartbeat, so the test fails instead of hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	events := func(yield func(a2a.Event, error) bool) {
		if !yield(working, nil) {
			return
		}
		for range 2 {
			select {
			case <-w.heartbeats:
			case <-ctx.Done():
				return
			}
		}
		yield(done, nil)
	}
	s.writeStream(ctx, cancel, w, float64(1), events)

	if got := w.header.Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("content type %q", got)
	}
	frames := strings.SplitAfter(w.String(), "\n\n")
	if last := frames[len(frames)-1]; last != "" {
		t.Fatalf("unterminated frame %q", last)
	}
	frames = frames[:len(frames)-1]
	var data []string
	between := 0
	for _, frame := range frames {
		switch {
		case strings.HasPrefix(frame, "data: "):
			data = append(data, frame)
		case frame == ": hb\n\n":
			if len(data) == 1 {
				between++
			}
		default:
			t.Errorf("frame %q, want data or the heartbeat comment", frame)
		}
	}
	if len(data) != 2 || !strings.Contains(data[0], `"TASK_STATE_WORKING"`) || !strings.Contains(data[1], `"TASK_STATE_COMPLETED"`) {
		t.Fatalf("data frames %q", data)
	}
	if !strings.HasPrefix(frames[0], "data: ") || between < 2 {
		t.Errorf("frames %q, want the first event, then at least 2 heartbeats before the last event", frames)
	}
}
