// Package link serves the link event streams of pi agents and keeps their presence.
package link

import (
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/orswn/a2a-hub/internal/engine"
)

// writeTimeout bounds one write to a stream, so a client that stopped reading does not hold its events forever.
const writeTimeout = time.Minute

// replaceWait bounds the wait of a new stream for the stream that it replaces.
const replaceWait = 2 * time.Second

// Options configures a Hub.
type Options struct {
	PresenceGrace time.Duration
	Heartbeat     time.Duration
	Log           func(line string)
}

type stream struct {
	// stop gets "replaced" when a newer stream takes over, or "" when the stream must end.
	stop chan string
	// done closes when Serve returned, so the response of a replaced stream ends before its successor writes.
	done chan struct{}
}

func (s *stream) end(reason string) {
	select {
	case s.stop <- reason:
	default:
	}
}

// Hub owns the open link streams and the presence grace timers.
type Hub struct {
	engine *engine.Engine
	opts   Options
	mu     sync.Mutex
	open   map[string]*stream
	grace  map[string]*time.Timer
}

// New makes a Hub.
func New(e *engine.Engine, opts Options) *Hub {
	if opts.Log == nil {
		opts.Log = func(string) {}
	}
	return &Hub{engine: e, opts: opts, open: map[string]*stream{}, grace: map[string]*time.Timer{}}
}

// Start gives the agents that an earlier run left online the grace period to reconnect, so a restart sends
// no presence churn.
func (h *Hub) Start(onlineAgents []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, id := range onlineAgents {
		h.scheduleOffline(id)
	}
}

// Frame is the SSE frame of a link event.
func Frame(event engine.LinkEvent) []byte {
	out := []byte("id: " + strconv.FormatInt(event.Seq, 10) + "\nevent: " + event.Type + "\ndata: ")
	out = append(out, event.Payload...)
	return append(out, "\n\n"...)
}

// Serve streams the events of an agent until the client goes, a newer stream replaces it, or the agent goes
// offline. afterSeq is the last seq the client holds; the ack stays the durable read mark.
func (h *Hub) Serve(w http.ResponseWriter, r *http.Request, agentID string, afterSeq *int64) {
	current := &stream{stop: make(chan string, 1), done: make(chan struct{})}
	defer close(current.done)
	h.mu.Lock()
	previous := h.open[agentID]
	if previous != nil {
		previous.end("replaced")
	}
	h.open[agentID] = current
	h.mu.Unlock()
	if previous != nil {
		select {
		case <-previous.done:
		case <-time.After(replaceWait):
		}
	}
	h.mu.Lock()
	h.cancelOffline(agentID)
	online := h.engine.SetOnline(agentID, true)
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.open[agentID] == current {
			delete(h.open, agentID)
			h.scheduleOffline(agentID)
		}
	}()

	header := w.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	header.Set("Connection", "keep-alive")
	header.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	control := http.NewResponseController(w)
	write := func(frame []byte) bool {
		_ = control.SetWriteDeadline(time.Now().Add(writeTimeout))
		if _, err := w.Write(frame); err != nil {
			return false
		}
		if err := control.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return false
		}
		return true
	}
	if !write(nil) {
		return
	}

	if online != nil {
		h.opts.Log("link open failed: " + online.Error())
		return
	}
	stream, err := h.engine.OpenLink(agentID, afterSeq)
	if err != nil {
		h.opts.Log("link open failed: " + err.Error())
		return
	}
	defer stream.Close()
	for _, event := range stream.Replay {
		if !write(Frame(event)) {
			return
		}
	}
	heartbeat := time.NewTicker(h.opts.Heartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case event, ok := <-stream.Events:
			if !ok || !write(Frame(event)) {
				return
			}
		case <-heartbeat.C:
			if !write([]byte(": hb\n\n")) {
				return
			}
		case reason := <-current.stop:
			if reason == "replaced" {
				write([]byte("event: replaced\ndata: {}\n\n"))
			}
			return
		case <-r.Context().Done():
			return
		}
	}
}

// OfflineNow ends the stream of an agent whose process quit and marks it offline at once. The mailbox stays.
func (h *Hub) OfflineNow(agentID string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cancelOffline(agentID)
	if current := h.open[agentID]; current != nil {
		delete(h.open, agentID)
		current.end("")
	}
	return h.engine.SetOnline(agentID, false)
}

// Stop ends every stream and timer.
func (h *Hub) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, timer := range h.grace {
		timer.Stop()
		delete(h.grace, id)
	}
	for id, current := range h.open {
		current.end("")
		delete(h.open, id)
	}
}

// scheduleOffline needs h.mu.
func (h *Hub) scheduleOffline(agentID string) {
	h.cancelOffline(agentID)
	var timer *time.Timer
	timer = time.AfterFunc(h.opts.PresenceGrace, func() {
		// The lock covers the write, so a stream that opens now marks the agent online after it.
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.grace[agentID] != timer {
			return
		}
		delete(h.grace, agentID)
		if _, streaming := h.open[agentID]; !streaming {
			if err := h.engine.SetOnline(agentID, false); err != nil {
				h.opts.Log("presence update failed: " + err.Error())
			}
		}
	})
	h.grace[agentID] = timer
}

// cancelOffline needs h.mu.
func (h *Hub) cancelOffline(agentID string) {
	if timer := h.grace[agentID]; timer != nil {
		timer.Stop()
		delete(h.grace, agentID)
	}
}
