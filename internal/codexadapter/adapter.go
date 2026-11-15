// Package codexadapter links each Codex thread of a host to the hub as a session agent and wakes a thread
// with a turn when a hub message asks for it. It is a plain client of the Codex app-server.
package codexadapter

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/orswn/a2a-hub/internal/codexrpc"
	"github.com/orswn/a2a-hub/internal/linkclient"
)

// Harness is the harness label of a Codex thread agent.
const Harness = "codex"

// Defaults of Options.
const (
	DefaultPollEvery    = 30 * time.Second
	DefaultOfflineAfter = 7 * 24 * time.Hour
	DefaultSweepEvery   = time.Hour
	DefaultStreamIdle   = 45 * time.Second
	// replacedRetry bounds how often a thread whose link another client replaced registers again.
	replacedRetry = time.Minute
	// maxCallFailures bounds the tries of a wake that fails with an unknown app-server error.
	maxCallFailures = 5
)

// optOut are the notifications that the adapter does not read.
var optOut = []string{
	"item/agentMessage/delta",
	"item/reasoning/summaryTextDelta",
	"item/reasoning/summaryPartAdded",
	"item/reasoning/textDelta",
	"item/commandExecution/outputDelta",
	"item/fileChange/outputDelta",
	"item/plan/delta",
}

// Options configures an Adapter.
type Options struct {
	// Socket is the app-server unix socket.
	Socket string
	Link   *linkclient.Client
	// StateFile keeps the threads of the last 7 days across adapter restarts.
	StateFile string
	Version   string
	Clock     Clock
	// RetryDelay gives the wait before retry n, from 1.
	RetryDelay   func(n int) time.Duration
	Log          func(string)
	PollEvery    time.Duration
	OfflineAfter time.Duration
	SweepEvery   time.Duration
	// StreamIdle is the silence after which a link stream counts as dead. The hub sends a heartbeat every 15 s.
	StreamIdle time.Duration
}

// DefaultRetryDelay is 1 s doubled per try, capped at 30 s.
func DefaultRetryDelay(n int) time.Duration {
	delay := time.Second
	for i := 1; i < n && delay < 30*time.Second; i++ {
		delay *= 2
	}
	return min(delay, 30*time.Second)
}

// Adapter serves the Codex threads of one app-server.
type Adapter struct {
	opts Options
	ctx  context.Context

	mu          sync.Mutex
	threads     map[string]*thread
	retired     map[string]bool
	notPeers    map[string]bool
	discovering map[string]bool
	conn        *codexrpc.Conn
	ready       chan struct{}
	senders     map[string]sender
	saved       map[string]savedThread
	wg          sync.WaitGroup

	saveNudge chan struct{}
}

// New makes an Adapter.
func New(opts Options) *Adapter {
	if opts.Clock == nil {
		opts.Clock = realClock{}
	}
	if opts.RetryDelay == nil {
		opts.RetryDelay = DefaultRetryDelay
	}
	if opts.Log == nil {
		opts.Log = func(string) {}
	}
	if opts.PollEvery == 0 {
		opts.PollEvery = DefaultPollEvery
	}
	if opts.OfflineAfter == 0 {
		opts.OfflineAfter = DefaultOfflineAfter
	}
	if opts.SweepEvery == 0 {
		opts.SweepEvery = DefaultSweepEvery
	}
	if opts.StreamIdle == 0 {
		opts.StreamIdle = DefaultStreamIdle
	}
	return &Adapter{
		opts:        opts,
		threads:     map[string]*thread{},
		retired:     map[string]bool{},
		notPeers:    map[string]bool{},
		discovering: map[string]bool{},
		ready:       make(chan struct{}),
		senders:     map[string]sender{},
		saved:       map[string]savedThread{},
		saveNudge:   make(chan struct{}, 1),
	}
}

// Run serves until ctx ends. It closes no link on the way out: the hub keeps the threads online for its
// presence grace, so a restarted adapter takes over without presence churn.
func (a *Adapter) Run(ctx context.Context) error {
	a.ctx = ctx
	saved, err := loadState(a.opts.StateFile)
	if err != nil {
		a.opts.Log(err.Error())
	}
	cutoff := a.opts.Clock.Now().Add(-a.opts.OfflineAfter)
	for id, s := range saved {
		if s.LastActivity.After(cutoff) {
			a.ensure(threadInfo{ID: id, Name: s.Name, Cwd: s.Cwd}, false, s.LastActivity)
		}
	}
	a.nudgeSave()
	a.wg.Add(2)
	go a.saveLoop()
	go a.sweepLoop()
	a.connectLoop(ctx)
	a.wg.Wait()
	return nil
}

func (a *Adapter) log(format string) { a.opts.Log(format) }

func (a *Adapter) sleep(ctx context.Context, n int) bool {
	timer := time.NewTimer(a.opts.RetryDelay(n))
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// connectLoop keeps one app-server connection.
func (a *Adapter) connectLoop(ctx context.Context) {
	failures := 0
	for ctx.Err() == nil {
		conn, err := codexrpc.Dial(ctx, a.opts.Socket, a.onEvent)
		if err == nil {
			err = conn.Initialize(ctx, codexrpc.ClientInfo{Name: "a2a_hub_adapter", Title: "a2a-hub adapter", Version: a.opts.Version}, optOut)
			if err != nil {
				_ = conn.Close()
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failures++
			if failures == 1 || failures%10 == 0 {
				a.log("app-server connect failed: " + err.Error())
			}
			if !a.sleep(ctx, failures) {
				return
			}
			continue
		}
		failures = 0
		a.log("app-server connected")
		a.setConn(conn)
		a.scan(ctx, conn)
		ticker := a.opts.Clock.NewTicker(a.opts.PollEvery)
	connected:
		for {
			select {
			case <-conn.Done():
				break connected
			case <-ticker.C():
				a.scan(ctx, conn)
			case <-ctx.Done():
				break connected
			}
		}
		ticker.Stop()
		_ = conn.Close()
		a.clearConn(conn)
		if ctx.Err() != nil {
			return
		}
		a.log("app-server connection lost")
		failures++
		if !a.sleep(ctx, failures) {
			return
		}
	}
}

func (a *Adapter) setConn(conn *codexrpc.Conn) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.conn = conn
	for _, t := range a.threads {
		t.setSubscribed(false)
	}
	close(a.ready)
}

func (a *Adapter) clearConn(conn *codexrpc.Conn) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.conn != conn {
		return
	}
	a.conn = nil
	a.ready = make(chan struct{})
	for _, t := range a.threads {
		t.setSubscribed(false)
	}
}

// waitConn returns the current connection, after it comes up if it is down.
func (a *Adapter) waitConn(ctx context.Context) (*codexrpc.Conn, error) {
	for {
		a.mu.Lock()
		conn, ready := a.conn, a.ready
		a.mu.Unlock()
		if conn != nil {
			return conn, nil
		}
		select {
		case <-ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// scan registers the loaded threads that the adapter does not serve yet. Retired threads wait for an event.
func (a *Adapter) scan(ctx context.Context, conn *codexrpc.Conn) {
	ids, err := loadedThreads(ctx, conn)
	if err != nil {
		if ctx.Err() == nil && !errors.Is(err, codexrpc.ErrClosed) {
			a.log("thread/loaded/list failed: " + err.Error())
		}
		return
	}
	for _, id := range ids {
		a.mu.Lock()
		skip := a.threads[id] != nil || a.retired[id] || a.notPeers[id]
		a.mu.Unlock()
		if !skip {
			a.discover(ctx, conn, id, false, "")
		}
	}
}

// loadedThreads reads all pages of thread/loaded/list.
func loadedThreads(ctx context.Context, conn *codexrpc.Conn) ([]string, error) {
	var ids []string
	cursor := ""
	for {
		params := map[string]any{}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var page struct {
			Data       []string `json:"data"`
			NextCursor *string  `json:"nextCursor"`
		}
		if err := conn.Call(ctx, "thread/loaded/list", params, &page); err != nil {
			return nil, err
		}
		ids = append(ids, page.Data...)
		if page.NextCursor == nil || *page.NextCursor == "" {
			return ids, nil
		}
		cursor = *page.NextCursor
	}
}

// threadInfo is the part of a Codex thread that the adapter reads.
type threadInfo struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	Cwd            string          `json:"cwd"`
	Ephemeral      bool            `json:"ephemeral"`
	ParentThreadID string          `json:"parentThreadId"`
	Source         json.RawMessage `json:"source"`
	ThreadSource   *string         `json:"threadSource"`
	Status         struct {
		Type string `json:"type"`
	} `json:"status"`
}

// skipped tells whether the thread is no peer. Like Codex itself, the adapter takes only user threads: not
// ephemeral, no sub-agent or internal source ("unknown" on the wire), and a threadSource of user or none.
func (info threadInfo) skipped() bool {
	if info.Ephemeral || info.ParentThreadID != "" {
		return true
	}
	if info.ThreadSource != nil && *info.ThreadSource != "user" {
		return true
	}
	var name string
	if json.Unmarshal(info.Source, &name) == nil {
		return name == "unknown"
	}
	var source map[string]json.RawMessage
	if json.Unmarshal(info.Source, &source) != nil {
		return false
	}
	_, sub := source["subAgent"]
	return sub
}

// discover reads a thread that the adapter does not know and registers it. A status from the event that
// found the thread wins over the read, which may be older. It blocks on the app-server, so it must not run in
// the read loop.
func (a *Adapter) discover(ctx context.Context, conn *codexrpc.Conn, id string, fromEvent bool, status string) {
	a.mu.Lock()
	if a.discovering[id] {
		a.mu.Unlock()
		return
	}
	a.discovering[id] = true
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		delete(a.discovering, id)
		a.mu.Unlock()
	}()
	var read struct {
		Thread threadInfo `json:"thread"`
	}
	if err := conn.Call(ctx, "thread/read", map[string]any{"threadId": id}, &read); err != nil || read.Thread.ID != id {
		read.Thread = threadInfo{ID: id}
	}
	if status != "" {
		read.Thread.Status.Type = status
	}
	a.ensure(read.Thread, fromEvent, a.opts.Clock.Now())
}

// ensure starts serving a thread, or restarts a stopped link when an event of the thread came. It does not
// block.
func (a *Adapter) ensure(info threadInfo, fromEvent bool, activity time.Time) *thread {
	a.mu.Lock()
	defer a.mu.Unlock()
	if info.skipped() {
		a.notPeers[info.ID] = true
		return nil
	}
	if a.notPeers[info.ID] {
		return nil
	}
	now := a.opts.Clock.Now()
	if t := a.threads[info.ID]; t != nil {
		if fromEvent {
			t.touch(now)
			t.restartIfStopped(now)
		}
		return t
	}
	if a.retired[info.ID] && !fromEvent {
		return nil
	}
	delete(a.retired, info.ID)
	t := newThread(a, info, activity)
	if info.Status.Type != "" {
		t.setStatus(info.Status.Type)
	}
	a.threads[info.ID] = t
	a.saved[info.ID] = t.savedLocked()
	a.nudgeSave()
	t.start()
	return t
}

// onEvent runs in the app-server read loop, so it never waits on the app-server or the hub.
func (a *Adapter) onEvent(msg codexrpc.Message) {
	if len(msg.ID) > 0 {
		// A server request, such as an approval. The adapter never answers: a human does in the TUI.
		return
	}
	var params struct {
		ThreadID   string     `json:"threadId"`
		ThreadName *string    `json:"threadName"`
		Thread     threadInfo `json:"thread"`
		Status     struct {
			Type string `json:"type"`
		} `json:"status"`
	}
	if json.Unmarshal(msg.Params, &params) != nil {
		return
	}
	switch msg.Method {
	case "thread/started":
		a.ensure(params.Thread, true, a.opts.Clock.Now())
	case "thread/status/changed":
		if t := a.known(params.ThreadID); t != nil {
			t.setStatus(params.Status.Type)
		} else {
			a.discoverAsync(params.ThreadID, params.Status.Type)
		}
	case "thread/name/updated":
		if t := a.known(params.ThreadID); t != nil {
			name := ""
			if params.ThreadName != nil {
				name = *params.ThreadName
			}
			t.rename(name)
		} else {
			a.discoverAsync(params.ThreadID, "")
		}
	case "thread/closed":
		a.mu.Lock()
		t := a.threads[params.ThreadID]
		a.mu.Unlock()
		if t != nil {
			t.setStatus("notLoaded")
		}
	case "turn/completed":
		a.mu.Lock()
		t := a.threads[params.ThreadID]
		a.mu.Unlock()
		if t != nil {
			t.turnDone()
		}
	}
}

// known returns a served thread and counts the event as activity, or restarts its stopped link.
func (a *Adapter) known(id string) *thread {
	if id == "" {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.notPeers[id] {
		return nil
	}
	t := a.threads[id]
	if t != nil {
		now := a.opts.Clock.Now()
		t.touch(now)
		t.restartIfStopped(now)
		a.saved[id] = t.savedLocked()
		a.nudgeSave()
	}
	return t
}

func (a *Adapter) discoverAsync(id, status string) {
	if id == "" {
		return
	}
	a.mu.Lock()
	conn := a.conn
	notPeer := a.notPeers[id]
	a.mu.Unlock()
	if conn == nil || notPeer || a.ctx == nil {
		return
	}
	go a.discover(a.ctx, conn, id, true, status)
}

// drop stops serving a thread and closes its link, so the hub shows it offline. A later event of the thread
// registers it again.
func (a *Adapter) drop(t *thread, why string) {
	a.mu.Lock()
	if a.threads[t.id] != t {
		a.mu.Unlock()
		return
	}
	delete(a.threads, t.id)
	delete(a.saved, t.id)
	a.retired[t.id] = true
	a.mu.Unlock()
	a.nudgeSave()
	a.log("thread " + t.id + " offline: " + why)
	t.stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.opts.Link.Close(ctx, t.id); err != nil {
		a.log("link close of " + t.id + " failed: " + err.Error())
	}
}

// sweepLoop takes threads offline after OfflineAfter without activity.
func (a *Adapter) sweepLoop() {
	defer a.wg.Done()
	ticker := a.opts.Clock.NewTicker(a.opts.SweepEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C():
		case <-a.ctx.Done():
			return
		}
		now := a.opts.Clock.Now()
		var stale []*thread
		a.mu.Lock()
		for _, t := range a.threads {
			if now.Sub(t.lastActivity()) >= a.opts.OfflineAfter {
				stale = append(stale, t)
			}
		}
		a.mu.Unlock()
		for _, t := range stale {
			a.drop(t, "no activity for "+a.opts.OfflineAfter.String())
		}
	}
}

// persist puts the current name, cwd, and activity of a served thread into the state file.
func (a *Adapter) persist(t *thread) {
	a.mu.Lock()
	if a.threads[t.id] == t {
		a.saved[t.id] = t.savedLocked()
	}
	a.mu.Unlock()
	a.nudgeSave()
}

func (a *Adapter) nudgeSave() {
	select {
	case a.saveNudge <- struct{}{}:
	default:
	}
}

func (a *Adapter) saveLoop() {
	defer a.wg.Done()
	for {
		select {
		case <-a.saveNudge:
		case <-a.ctx.Done():
			return
		}
		a.mu.Lock()
		copied := make(map[string]savedThread, len(a.saved))
		for id, s := range a.saved {
			copied[id] = s
		}
		a.mu.Unlock()
		if a.opts.StateFile == "" {
			continue
		}
		if err := saveState(a.opts.StateFile, copied, a.opts.Clock.Now().Add(-a.opts.OfflineAfter)); err != nil {
			a.log("state file write failed: " + err.Error())
		}
	}
}

// senderOf describes the sender of a message. The harness label needs a lookup, which the adapter caches.
func (a *Adapter) senderOf(ctx context.Context, agent, name, kind string) sender {
	if agent == "" {
		return sender{Name: name, Kind: kind}
	}
	a.mu.Lock()
	cached, ok := a.senders[agent]
	a.mu.Unlock()
	if !ok {
		info, err := a.opts.Link.Agent(ctx, agent)
		if err != nil {
			return sender{Name: name, Kind: kind}
		}
		cached = sender{Name: info.Name, Harness: info.Labels["harness"], Kind: info.Kind}
		a.mu.Lock()
		if len(a.senders) > 1000 {
			a.senders = map[string]sender{}
		}
		a.senders[agent] = cached
		a.mu.Unlock()
	}
	cached.Name = name
	if kind != "" {
		cached.Kind = kind
	}
	return cached
}

// gone tells whether an app-server error means that the thread does not exist.
func gone(err error) bool {
	var rpcErr *codexrpc.Error
	if !errors.As(err, &rpcErr) || rpcErr.Code != codexrpc.CodeInvalidRequest {
		return false
	}
	return strings.HasPrefix(rpcErr.Message, "no rollout found") || strings.HasPrefix(rpcErr.Message, "thread not found")
}

// shuttingDown tells whether the app-server refused a call because it stops. It comes back like a lost
// connection.
func shuttingDown(err error) bool {
	var rpcErr *codexrpc.Error
	if !errors.As(err, &rpcErr) || len(rpcErr.Data) == 0 {
		return false
	}
	var data struct {
		Reason string `json:"reason"`
	}
	return json.Unmarshal(rpcErr.Data, &data) == nil && data.Reason == "serverShuttingDown"
}

// busyTurn tells whether turn/start met a review or compact turn, which it cannot steer.
func busyTurn(err error) bool {
	var rpcErr *codexrpc.Error
	return errors.As(err, &rpcErr) && rpcErr.Code == codexrpc.CodeInvalidRequest &&
		(strings.Contains(rpcErr.Message, "cannot steer a review turn") || strings.Contains(rpcErr.Message, "cannot steer a compact turn"))
}
