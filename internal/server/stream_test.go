package server

import (
	"context"
	"net/http"
	"strings"
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
