package engine

import (
	"context"
	"encoding/json"
	"math"
	"sync"
	"time"
)

// emit stores a link event for a pi agent and tells the link listeners after the commit. nil means the agent
// is not a pi agent, so it has no mailbox.
func (e *Engine) emit(agentID, eventType string, payload any) *int64 {
	row := e.agent(agentID)
	if row == nil || row.Kind != "pi" {
		return nil
	}
	text := stringify(payload)
	_, seq := e.run("INSERT INTO link_events (agent_id, type, payload, created_at) VALUES (?, ?, ?, ?)",
		agentID, eventType, text, e.iso())
	event := LinkEvent{Seq: seq, Type: eventType, Payload: json.RawMessage(text)}
	e.afterCommit(func() func() { return e.linkNotice(agentID, event) })
	return &seq
}

func (e *Engine) unacked(agentID string) []LinkEvent {
	events := []LinkEvent{}
	e.each("SELECT seq, type, payload FROM link_events WHERE agent_id = ? AND acked_at IS NULL ORDER BY seq",
		args(agentID), func(scan func(...any)) {
			var event LinkEvent
			var payload string
			scan(&event.Seq, &event.Type, &payload)
			event.Payload = json.RawMessage(payload)
			events = append(events, event)
		})
	return events
}

// Unacked lists the events that the agent has not acked, oldest first.
func (e *Engine) Unacked(agentID string) ([]LinkEvent, error) {
	return call(e, func() []LinkEvent { return e.unacked(agentID) })
}

// replay gives the unacked events for a new stream, only those after afterSeq when given. A stale close must
// not end a process started later, so it is acked and skipped.
func (e *Engine) replay(agentID string, afterSeq *int64) []LinkEvent {
	cutoff := ISO(e.now() - StaleControlMS)
	e.run("UPDATE link_events SET acked_at = ? WHERE agent_id = ? AND type = 'control' AND acked_at IS NULL AND created_at < ?",
		e.iso(), agentID, cutoff)
	events := e.unacked(agentID)
	if afterSeq == nil {
		return events
	}
	out := []LinkEvent{}
	for _, event := range events {
		if event.Seq > *afterSeq {
			out = append(out, event)
		}
	}
	return out
}

// Replay gives the unacked events for a new stream. Use OpenLink for a stream that also takes new events.
func (e *Engine) Replay(agentID string, afterSeq *int64) ([]LinkEvent, error) {
	return call(e, func() []LinkEvent { return e.replay(agentID, afterSeq) })
}

// Link is an open link subscription.
type Link struct {
	// Replay holds the unacked events at the time of the open.
	Replay []LinkEvent
	// Events gives each later event of the agent once, in seq order. It closes after Close.
	Events <-chan LinkEvent
	close  func()
}

// Close ends the subscription. It is safe to call more than once.
func (l *Link) Close() { l.close() }

// OpenLink reads the replay and subscribes to new events of the agent under one lock, so no event is lost
// between the two and none comes twice.
func (e *Engine) OpenLink(agentID string, afterSeq *int64) (*Link, error) {
	sub := &linkSub{wake: make(chan struct{}, 1), done: make(chan struct{}), out: make(chan LinkEvent)}
	link, err := call(e, func() *Link {
		replay := e.replay(agentID, afterSeq)
		// Every event up to the newest seq is committed and in the replay or acked. Its listener call may still
		// be queued, so the subscriber drops it.
		watermark := e.count("SELECT max(seq) FROM link_events WHERE agent_id = ?", agentID)
		off := e.OnLink(func(target string, event LinkEvent) {
			if target == agentID && event.Seq > watermark {
				sub.push(event)
			}
		})
		var once sync.Once
		return &Link{Replay: replay, Events: sub.out, close: func() {
			once.Do(func() {
				off()
				close(sub.done)
			})
		}}
	})
	if err != nil {
		return nil, err
	}
	go sub.pump()
	return link, nil
}

type linkSub struct {
	mu   sync.Mutex
	buf  []LinkEvent
	wake chan struct{}
	done chan struct{}
	out  chan LinkEvent
}

// push never blocks, so a slow stream cannot hold up the listeners of other agents.
func (s *linkSub) push(event LinkEvent) {
	s.mu.Lock()
	s.buf = append(s.buf, event)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *linkSub) pump() {
	defer close(s.out)
	for {
		s.mu.Lock()
		if len(s.buf) == 0 {
			s.mu.Unlock()
			select {
			case <-s.wake:
				continue
			case <-s.done:
				return
			}
		}
		event := s.buf[0]
		s.buf = s.buf[1:]
		s.mu.Unlock()
		select {
		case s.out <- event:
		case <-s.done:
			return
		}
	}
}

// Ack marks the events up to upTo as acked and returns how many were open. upTo is the value as jsonx parses
// it from the request body: a float64 for a JSON number. Any other type, a Go int too, gets the refusal.
func (e *Engine) Ack(agentID string, upTo any) (int64, error) {
	return call(e, func() int64 {
		n, ok := upTo.(float64)
		if !ok || n != math.Trunc(n) || math.IsInf(n, 0) || n < 0 {
			throw(ReasonInvalid, "upTo must be a non-negative integer")
		}
		changes, _ := e.run("UPDATE link_events SET acked_at = ? WHERE agent_id = ? AND seq <= ? AND acked_at IS NULL",
			e.iso(), agentID, n)
		e.touch(agentID)
		seq := int64(math.MaxInt64)
		if n < math.MaxInt64 {
			seq = int64(n)
		}
		e.afterCommit(func() func() { return e.ackNotice(agentID, seq) })
		return changes
	})
}

func (e *Engine) isAcked(seq int64) bool {
	var at *string
	return e.one("SELECT acked_at FROM link_events WHERE seq = ?", args(seq), &at) && at != nil
}

// IsAcked reports whether the event is acked.
func (e *Engine) IsAcked(seq int64) (bool, error) {
	return call(e, func() bool { return e.isAcked(seq) })
}

// WaitForAck waits until the agent acks seq. After the timeout it reports whether seq is acked by then. A done
// ctx gives false.
func (e *Engine) WaitForAck(ctx context.Context, agentID string, seq int64, timeout time.Duration) bool {
	acked := make(chan struct{}, 1)
	off := e.OnAck(func(agent string, upTo int64) {
		if agent == agentID && upTo >= seq {
			select {
			case acked <- struct{}{}:
			default:
			}
		}
	})
	defer off()
	if done, err := e.IsAcked(seq); err == nil && done {
		return true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-acked:
		return true
	case <-timer.C:
		done, err := e.IsAcked(seq)
		return err == nil && done
	case <-ctx.Done():
		return false
	}
}
