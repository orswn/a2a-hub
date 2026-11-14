package engine

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Senders on several goroutines emit events for two agents while subscribers open, read a little, close,
// and open again after the last seq they saw, as a link stream does on a reconnect. Each subscriber must see
// every event of its agent once and in order.
func TestOpenLinkLosesAndDoublesNoEventWhileLinksOpenAndClose(t *testing.T) {
	h := setup(t)
	h.register("a1")
	h.register("a2")
	const senders, perSender, subscribers = 4, 60, 6
	var finalMax atomic.Int64
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	seen := make([][]int64, subscribers)
	var subs sync.WaitGroup
	for s := range subscribers {
		subs.Add(1)
		go func() {
			defer subs.Done()
			var after *int64
			for round := 0; ; round++ {
				link, err := h.engine.OpenLink("a1", after)
				if err != nil {
					t.Error(err)
					return
				}
				for _, event := range link.Replay {
					seen[s] = append(seen[s], event.Seq)
				}
				// Read a few events; the count varies per subscriber and round so closes land everywhere.
				for range (s+round)%5 + 1 {
					select {
					case event := <-link.Events:
						seen[s] = append(seen[s], event.Seq)
					case <-time.After(5 * time.Millisecond):
					case <-ctx.Done():
					}
				}
				link.Close()
				if n := len(seen[s]); n > 0 {
					last := seen[s][n-1]
					after = &last
				}
				if final := finalMax.Load(); final != 0 && after != nil && *after == final {
					return
				}
				if ctx.Err() != nil {
					t.Errorf("subscriber %d timed out", s)
					return
				}
			}
		}()
	}

	var send sync.WaitGroup
	for i := range senders {
		send.Add(1)
		go func() {
			defer send.Done()
			for j := range perSender {
				agent := "a1"
				if j%3 == 2 {
					agent = "a2"
				}
				msg := withMeta(message(fmt.Sprintf("s%d-%d", i, j)), "kind", "note")
				if _, err := h.engine.Send(SendInput{AgentID: agent, Caller: fromHermes, Message: msg}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	send.Wait()

	var want []int64
	for _, event := range got(h.engine.Unacked("a1")).ok(t) {
		want = append(want, event.Seq)
	}
	finalMax.Store(want[len(want)-1])
	subs.Wait()
	for s, got := range seen {
		if !slices.Equal(got, want) {
			t.Errorf("subscriber %d saw %d events, want %d in order\n got %v\nwant %v", s, len(got), len(want), got, want)
		}
	}
}

// A subscriber that stays open gets each event once, after its replay, while other links churn.
func TestOpenLinkDeliversEveryLaterEventToAnOpenSubscriber(t *testing.T) {
	h := setup(t)
	h.register("a1")
	h.send("a1", fromHermes, withMeta(message("before"), "kind", "note"))
	link := got(h.engine.OpenLink("a1", nil)).ok(t)
	defer link.Close()
	stop := make(chan struct{})
	var churn sync.WaitGroup
	churn.Add(1)
	go func() {
		defer churn.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			other, err := h.engine.OpenLink("a1", nil)
			if err != nil {
				t.Error(err)
				return
			}
			other.Close()
		}
	}()
	const n = 100
	var send sync.WaitGroup
	for i := range 4 {
		send.Add(1)
		go func() {
			defer send.Done()
			for j := range n / 4 {
				msg := withMeta(message(fmt.Sprintf("m%d-%d", i, j)), "kind", "note")
				if _, err := h.engine.Send(SendInput{AgentID: "a1", Caller: fromHermes, Message: msg}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	send.Wait()
	close(stop)
	churn.Wait()
	all := got(h.engine.Unacked("a1")).ok(t)
	var got []int64
	for _, event := range link.Replay {
		got = append(got, event.Seq)
	}
	timeout := time.After(10 * time.Second)
	for len(got) < len(all) {
		select {
		case event := <-link.Events:
			got = append(got, event.Seq)
		case <-timeout:
			t.Fatalf("got %d of %d events", len(got), len(all))
		}
	}
	var want []int64
	for _, event := range all {
		want = append(want, event.Seq)
	}
	if !slices.Equal(got, want) {
		t.Errorf("got %v\nwant %v", got, want)
	}
	select {
	case event := <-link.Events:
		t.Errorf("extra event %d", event.Seq)
	case <-time.After(20 * time.Millisecond):
	}
	link.Close()
	if _, open := <-link.Events; open {
		t.Error("Events is open after Close")
	}
}
