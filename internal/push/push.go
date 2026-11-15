// Package push sends the push outbox of the engine to the task webhooks.
package push

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/pushguard"
)

// Timeout bounds one delivery attempt.
const Timeout = 10 * time.Second

// Backoff is the delay before retry 1, 2, and 3. After the third retry fails, the row is dropped.
var Backoff = []time.Duration{time.Second, 5 * time.Second, 30 * time.Second}

const batch = 16

// Options configures a Sender.
type Options struct {
	// Transport sends the requests. nil dials through the guard.
	Transport http.RoundTripper
	// Now gives the time in milliseconds since the epoch. nil uses the engine clock.
	Now     func() int64
	Timeout time.Duration
	Log     func(line string)
	Guard   *pushguard.Guard
}

// Headers gives the request headers of a push: Authorization when the config has a scheme and credentials,
// else the notification token header when it has a token.
func Headers(item engine.OutboxItem) http.Header {
	header := http.Header{"Content-Type": {"application/a2a+json"}}
	switch {
	case filled(item.AuthScheme) && filled(item.AuthCredentials):
		header.Set("Authorization", *item.AuthScheme+" "+*item.AuthCredentials)
	case filled(item.Token):
		header.Set("X-A2A-Notification-Token", *item.Token)
	}
	return header
}

func filled(s *string) bool { return s != nil && *s != "" }

// Sender sends the push outbox. Rows survive a restart because they live in the database.
type Sender struct {
	engine  *engine.Engine
	client  *http.Client
	now     func() int64
	timeout time.Duration
	log     func(line string)
	guard   *pushguard.Guard

	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	mu       sync.Mutex
	inFlight map[int64]bool
	timer    *time.Timer
	timerAt  int64
	off      func()
	stopped  bool

	// runMu covers the read of the due rows and the claim loop, so a delivery that ends in between cannot
	// leave the in-flight set while a run still holds its stale row.
	runMu sync.Mutex
}

// New makes a Sender. Start begins the sending.
func New(e *engine.Engine, opts Options) *Sender {
	s := &Sender{
		engine: e, now: opts.Now, timeout: opts.Timeout, log: opts.Log, guard: opts.Guard,
		inFlight: map[int64]bool{}, timerAt: math.MaxInt64,
	}
	if s.now == nil {
		s.now = e.Now
	}
	if s.timeout <= 0 {
		s.timeout = Timeout
	}
	if s.log == nil {
		s.log = func(string) {}
	}
	transport := opts.Transport
	if transport == nil {
		dialer := &net.Dialer{Timeout: Timeout}
		transport = &http.Transport{
			DialContext: s.guard.DialContext(dialer), ForceAttemptHTTP2: true, IdleConnTimeout: 90 * time.Second,
		}
	}
	s.client = &http.Client{
		Transport: transport,
		// A redirect could lead to a host that the check did not see.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	return s
}

// Start sends the due rows and every row that a later commit queues.
func (s *Sender) Start() {
	s.mu.Lock()
	s.off = s.engine.OnPush(func() { s.schedule(s.now()) })
	s.mu.Unlock()
	s.schedule(s.now())
}

// Stop ends the sending and waits for a running run and for the open attempts, which it aborts. A row of an
// aborted attempt stays as it was and is sent again after a restart.
func (s *Sender) Stop() {
	s.mu.Lock()
	s.stopped = true
	if s.off != nil {
		s.off()
	}
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	s.mu.Unlock()
	s.cancel()
	s.wg.Wait()
}

func (s *Sender) schedule(at int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped || at >= s.timerAt {
		return
	}
	if s.timer != nil {
		s.timer.Stop()
	}
	s.timerAt = at
	var timer *time.Timer
	timer = time.AfterFunc(time.Duration(max(0, at-s.now()))*time.Millisecond, func() {
		s.mu.Lock()
		if s.timer != timer {
			s.mu.Unlock()
			return
		}
		s.timer = nil
		s.timerAt = math.MaxInt64
		if s.stopped {
			s.mu.Unlock()
			return
		}
		s.wg.Add(1)
		s.mu.Unlock()
		defer s.wg.Done()
		s.Run()
	})
	s.timer = timer
}

// Run starts every due delivery, then arms the timer for the next due row.
func (s *Sender) Run() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	open := len(s.inFlight)
	s.mu.Unlock()
	if !s.claim(open) {
		return
	}
	next, ok, err := s.engine.NextPushAt()
	if err != nil {
		s.log("push run failed: " + err.Error())
		return
	}
	s.mu.Lock()
	idle := len(s.inFlight) == 0
	s.mu.Unlock()
	if ok && idle {
		s.schedule(next)
	}
}

// claim starts a delivery for each due row that is not in flight. It reports false when the read failed.
func (s *Sender) claim(open int) bool {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	due, err := s.engine.DuePushes(s.now(), batch+open)
	if err != nil {
		s.log("push run failed: " + err.Error())
		return false
	}
	for _, item := range due {
		s.mu.Lock()
		if s.stopped || s.inFlight[item.ID] {
			s.mu.Unlock()
			continue
		}
		s.inFlight[item.ID] = true
		s.wg.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.wg.Done()
			if _, err := s.Deliver(item); err != nil {
				s.log(fmt.Sprintf("push %d failed: %v", item.ID, err))
			}
			s.runMu.Lock()
			s.mu.Lock()
			delete(s.inFlight, item.ID)
			s.mu.Unlock()
			s.runMu.Unlock()
			s.schedule(s.now())
		}()
	}
	return true
}

// Deliver makes one attempt and reports whether the webhook answered 2xx. The address check runs before each
// attempt and again on the socket, so a DNS change after config time is caught. The error is a failed update
// of the outbox row.
func (s *Sender) Deliver(item engine.OutboxItem) (bool, error) {
	ok, reason := s.attempt(item)
	s.mu.Lock()
	stopped := s.stopped
	s.mu.Unlock()
	if stopped {
		return ok, nil
	}
	if ok {
		return true, s.engine.PushDone(item.ID)
	}
	attempts := item.Attempts + 1
	if attempts > int64(len(Backoff)) {
		s.log(fmt.Sprintf("push to %s dropped after %d attempts: %s", item.URL, attempts, reason))
		return false, s.engine.PushDone(item.ID)
	}
	return false, s.engine.PushRetry(item.ID, attempts, s.now()+Backoff[attempts-1].Milliseconds())
}

func (s *Sender) attempt(item engine.OutboxItem) (bool, string) {
	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	if refused := s.guard.CheckSend(ctx, item.URL); refused != "" {
		return false, refused
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, item.URL, strings.NewReader(item.Payload))
	if err != nil {
		return false, err.Error()
	}
	req.Header = Headers(item)
	res, err := s.client.Do(req)
	if err != nil {
		return false, err.Error()
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 64*1024))
	_ = res.Body.Close()
	return res.StatusCode >= 200 && res.StatusCode < 300, fmt.Sprintf("HTTP %d", res.StatusCode)
}
