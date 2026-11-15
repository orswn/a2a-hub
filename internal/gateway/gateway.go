// Package gateway sends the hub tasks of external A2A agents, such as Hermes, with SendStreamingMessage and
// maps the answers back to the hub tasks.
package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/orswn/a2a-hub/internal/a2aapi"
	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/external"
	"github.com/orswn/a2a-hub/internal/jsonx"
)

const (
	// ConnectTimeout is the time to the response headers of a request to an external agent.
	ConnectTimeout = 10 * time.Second
	// CardFailuresOffline is the number of failed card fetches in a row before the agent shows offline.
	CardFailuresOffline = 3
	// RateLimitRetries is the number of rate-limit answers before the task fails.
	RateLimitRetries = 5
	// BodyLimit caps a JSON answer and one stream event of an external agent.
	BodyLimit         = 4 << 20
	offlineBackoffMax = 60 * time.Second
	maxSkills         = 32
	askAgain          = "Ask again in the same context."
)

const (
	errTaskNotFound  = -32001
	errUnauthorized  = -32050
	errRateLimited   = -32051
	errUntrustedPeer = -32052
)

// connectErrors mean that the request never reached the agent, so a resend is safe.
var connectErrors = map[string]bool{
	"ECONNREFUSED": true, "ENOTFOUND": true, "EAI_AGAIN": true, "EHOSTUNREACH": true, "ENETUNREACH": true,
	"UND_ERR_CONNECT_TIMEOUT": true,
}

// Options are the settings of a Gateway. The zero value is fine.
type Options struct {
	// Client sends the requests. nil uses a client with the connect timeout and no redirects.
	Client         *http.Client
	Log            func(line string)
	ConnectTimeout time.Duration
}

type stopReason string

const (
	reasonStop    stopReason = "stop"
	reasonCancel  stopReason = "cancel"
	reasonRemoved stopReason = "removed"
)

type running struct {
	taskID  string
	agentID string
	ctx     context.Context
	cancel  context.CancelFunc
	reason  stopReason
}

type cardState struct {
	skills   []*jsonx.Object
	failures int
	timer    *time.Timer
}

type outcomeKind int

const (
	settled outcomeKind = iota
	drop
	retry
	unreachable
	failed
)

type outcome struct {
	kind   outcomeKind
	detail string
}

// failure carries an engine error through a panic to the goroutine boundary, where it is logged.
type failure struct{ err error }

func must[T any](value T, err error) T {
	if err != nil {
		panic(failure{err})
	}
	return value
}

func check(err error) {
	if err != nil {
		panic(failure{err})
	}
}

// Gateway runs the dispatches of the external agents. One dispatch per agent and context runs at a time: the
// agent may interrupt a running turn of the same context. Push is never sent to the agent. Hub clients keep
// using hub push on the hub task.
type Gateway struct {
	engine         *engine.Engine
	client         *http.Client
	log            func(string)
	connectTimeout time.Duration
	wake           chan struct{}
	shutdown       context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup

	// presenceMu orders the presence writes of card fetches, dispatches, and reloads, so a removed agent
	// does not come back online. It is taken before mu.
	presenceMu sync.Mutex

	mu      sync.Mutex
	agents  map[string]*external.Agent
	cards   map[string]*cardState
	running map[string]*running
	claimed map[int64]bool
	logged  map[string]bool
	off     func()
	started bool
	stopped bool
}

// New makes a Gateway. Configure sets the agents and Start begins the work.
func New(e *engine.Engine, opts Options) *Gateway {
	g := &Gateway{
		engine: e, client: opts.Client, log: opts.Log, connectTimeout: opts.ConnectTimeout,
		wake:   make(chan struct{}, 1),
		agents: map[string]*external.Agent{}, cards: map[string]*cardState{}, running: map[string]*running{},
		claimed: map[int64]bool{}, logged: map[string]bool{},
	}
	if g.log == nil {
		g.log = func(string) {}
	}
	if g.connectTimeout <= 0 {
		g.connectTimeout = ConnectTimeout
	}
	if g.client == nil {
		g.client = NewClient(g.connectTimeout)
	}
	g.shutdown, g.cancel = context.WithCancel(context.Background())
	return g
}

// NewClient makes the HTTP client for external agents: explicit dial and TLS timeouts, no proxy from the
// environment, and no redirects. The operator writes the agent URLs, so the push address guard does not
// apply.
func NewClient(connectTimeout time.Duration) *http.Client {
	dialer := &net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}
	return &http.Client{
		Transport: &http.Transport{
			DialContext:         dialer.DialContext,
			TLSHandshakeTimeout: connectTimeout,
			IdleConnTimeout:     90 * time.Second,
			ForceAttemptHTTP2:   true,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// Size is the number of configured agents.
func (g *Gateway) Size() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.agents)
}

// Card gives the configured description and the skills of the last good card. nil means not configured.
func (g *Gateway) Card(agentID string) *a2aapi.ExternalCard {
	g.mu.Lock()
	defer g.mu.Unlock()
	agent := g.agents[agentID]
	if agent == nil {
		return nil
	}
	card := &a2aapi.ExternalCard{Description: agent.Description}
	if state := g.cards[agentID]; state != nil {
		card.Skills = state.skills
	}
	return card
}

// Configure loads or reloads the agent list. A removed agent goes offline and its running dispatches fail.
func (g *Gateway) Configure(agents []external.Agent) {
	next := map[string]*external.Agent{}
	var order []string
	for _, agent := range agents {
		if err := g.engine.EnsureExternal(agent.ID, agent.Name); err != nil {
			g.log("external agent " + agent.ID + " skipped: " + explain(err))
			continue
		}
		copied := agent
		next[agent.ID] = &copied
		order = append(order, agent.ID)
	}
	g.presenceMu.Lock()
	g.mu.Lock()
	var removed []string
	for id := range g.agents {
		if next[id] != nil {
			continue
		}
		removed = append(removed, id)
		g.stopCard(id)
		delete(g.cards, id)
		for _, r := range g.running {
			if r.agentID == id {
				g.abort(r, reasonRemoved)
			}
		}
	}
	g.agents = next
	g.logged = map[string]bool{}
	started := g.started && !g.stopped
	g.mu.Unlock()
	for _, id := range removed {
		if err := g.engine.SetOnline(id, false); err != nil {
			g.log("external agent " + id + ": " + err.Error())
		}
	}
	g.presenceMu.Unlock()
	if started {
		for _, id := range order {
			g.refreshCard(id)
		}
		g.kick()
	}
}

// Start fetches the cards, recovers the dispatches that were open at the last stop, and runs the queue.
func (g *Gateway) Start() {
	g.mu.Lock()
	if g.started || g.stopped {
		g.mu.Unlock()
		return
	}
	g.started = true
	ids := make([]string, 0, len(g.agents))
	for id := range g.agents {
		ids = append(ids, id)
	}
	g.wg.Add(1)
	g.mu.Unlock()
	off := g.engine.OnExternal(g.kick)
	g.mu.Lock()
	g.off = off
	g.mu.Unlock()
	go g.loop()
	for _, id := range ids {
		g.refreshCard(id)
	}
	remotes, err := g.engine.ActiveRemotes()
	if err != nil {
		g.log("external recovery failed: " + err.Error())
	}
	for _, remote := range remotes {
		g.resume(remote)
	}
	g.kick()
}

// Stop aborts the open requests and streams and waits for every goroutine of the gateway. It leaves every row
// as it is, so the next start recovers the open dispatches.
func (g *Gateway) Stop() {
	g.mu.Lock()
	if g.stopped {
		g.mu.Unlock()
		g.wg.Wait()
		return
	}
	g.stopped = true
	for id := range g.cards {
		g.stopCard(id)
	}
	for _, r := range g.running {
		g.abort(r, reasonStop)
	}
	off := g.off
	g.mu.Unlock()
	if off != nil {
		off()
	}
	g.cancel()
	g.wg.Wait()
}

// goRun runs fn in a goroutine that Stop waits for. It reports false after Stop.
func (g *Gateway) goRun(fn func()) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return false
	}
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		fn()
	}()
	return true
}

// guard logs a failure that an engine call raised.
func (g *Gateway) guard(prefix string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			f, ok := r.(failure)
			if !ok {
				panic(r)
			}
			g.log(prefix + explain(f.err))
		}
	}()
	fn()
}

// setOnline writes the presence of an agent unless a reload removed or replaced it.
func (g *Gateway) setOnline(agent *external.Agent, online bool) {
	g.presenceMu.Lock()
	defer g.presenceMu.Unlock()
	g.mu.Lock()
	current := g.agents[agent.ID] == agent && !g.stopped
	g.mu.Unlock()
	if current {
		check(g.engine.SetOnline(agent.ID, online))
	}
}

// ---------- cards ----------

// stopCard needs mu.
func (g *Gateway) stopCard(id string) {
	if state := g.cards[id]; state != nil && state.timer != nil {
		state.timer.Stop()
		state.timer = nil
	}
}

func (g *Gateway) refreshCard(id string) {
	g.mu.Lock()
	g.stopCard(id)
	g.mu.Unlock()
	g.goRun(func() { g.fetchCard(id) })
}

// scheduleCard needs mu.
func (g *Gateway) scheduleCard(id string, after time.Duration) {
	state := g.cards[id]
	if state == nil || g.stopped {
		return
	}
	if state.timer != nil {
		state.timer.Stop()
	}
	state.timer = time.AfterFunc(after, func() { g.goRun(func() { g.fetchCard(id) }) })
}

func (g *Gateway) fetchCard(id string) {
	g.mu.Lock()
	agent := g.agents[id]
	if agent == nil || g.stopped {
		g.mu.Unlock()
		return
	}
	state := g.cards[id]
	if state == nil {
		state = &cardState{}
		g.cards[id] = state
	}
	g.mu.Unlock()
	skills, err := g.getCard(agent)
	g.presenceMu.Lock()
	defer g.presenceMu.Unlock()
	g.mu.Lock()
	if g.stopped || g.agents[id] != agent {
		g.mu.Unlock()
		return
	}
	if err == nil {
		state.skills = skills
		state.failures = 0
		g.scheduleCard(id, agent.CardTTL)
		g.mu.Unlock()
		if err := g.engine.SetOnline(id, true); err != nil {
			g.log("external agent " + id + ": " + err.Error())
		}
		return
	}
	state.failures++
	failures := state.failures
	g.scheduleCard(id, agent.Poll)
	g.mu.Unlock()
	if failures == CardFailuresOffline {
		g.log(fmt.Sprintf("external agent %s: card fetch failed %d times, offline: %s", id, failures, explain(err)))
	}
	if failures >= CardFailuresOffline {
		if err := g.engine.SetOnline(id, false); err != nil {
			g.log("external agent " + id + ": " + err.Error())
		}
	}
}

func (g *Gateway) getCard(agent *external.Agent) ([]*jsonx.Object, error) {
	ctx, cancel := context.WithTimeout(g.shutdown, g.connectTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, external.CardURL(agent.URL), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	res, err := g.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 64*1024))
		return nil, fmt.Errorf("HTTP %d", res.StatusCode)
	}
	body, err := readLimited(res.Body)
	if err != nil {
		return nil, err
	}
	card, err := jsonx.Parse(body)
	if err != nil {
		return nil, err
	}
	return ParseSkills(card), nil
}

func readLimited(body io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, BodyLimit+1))
	if err != nil {
		return nil, err
	}
	if len(data) > BodyLimit {
		return nil, fmt.Errorf("answer is larger than %d bytes", BodyLimit)
	}
	return data, nil
}

// ParseSkills keeps the well-formed skills of an agent card, at most 32.
func ParseSkills(card any) []*jsonx.Object {
	object, _ := card.(*jsonx.Object)
	skills, _ := object.Value("skills").([]any)
	out := []*jsonx.Object{}
	for i, item := range skills {
		if i == maxSkills {
			break
		}
		skill, _ := item.(*jsonx.Object)
		id, idOK := skill.String("id")
		name, nameOK := skill.String("name")
		if !idOK || !nameOK {
			continue
		}
		description, _ := skill.String("description")
		tags := []any{}
		if list, ok := skill.Value("tags").([]any); ok {
			for _, tag := range list {
				if s, ok := tag.(string); ok {
					tags = append(tags, s)
				}
			}
		}
		out = append(out, jsonx.NewObject("id", id, "name", name, "description", description, "tags", tags))
	}
	return out
}

// OutboundText puts the sender line in front of the text, so the external agent knows which pi session asks.
func OutboundText(text, senderName string, senderHost *string) string {
	if senderHost != nil && *senderHost != "" {
		return "[from " + senderName + " on " + *senderHost + "]\n" + text
	}
	return "[from " + senderName + "]\n" + text
}

// ---------- dispatch loop ----------

func (g *Gateway) kick() {
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

func (g *Gateway) loop() {
	defer g.wg.Done()
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	for {
		select {
		case <-g.shutdown.Done():
			return
		case <-g.wake:
		case <-timer.C:
		}
		timer.Stop()
		var next int64 = math.MaxInt64
		g.guard("external run failed: ", func() { next = g.run() })
		if next != math.MaxInt64 {
			timer.Reset(time.Duration(max(0, next-g.engine.Now())) * time.Millisecond)
		}
	}
}

// run starts every due job and gives the time of the next one that waits.
func (g *Gateway) run() int64 {
	now := g.engine.Now()
	held := map[string]bool{}
	next := int64(math.MaxInt64)
	for _, job := range must(g.engine.ExternalJobs()) {
		key := job.AgentID + "\x00" + job.ContextID
		g.mu.Lock()
		claimed := g.claimed[job.ID]
		g.mu.Unlock()
		if claimed {
			if job.Op == "send" {
				held[key] = true
			}
			continue
		}
		if job.Op == "send" {
			if held[key] {
				continue
			}
			held[key] = true
			if must(g.engine.RemoteBusy(job.AgentID, job.ContextID)) {
				continue
			}
		}
		if job.NextAt > now {
			next = min(next, job.NextAt)
			continue
		}
		g.mu.Lock()
		g.claimed[job.ID] = true
		g.mu.Unlock()
		started := g.goRun(func() {
			defer func() {
				g.mu.Lock()
				delete(g.claimed, job.ID)
				g.mu.Unlock()
				g.kick()
			}()
			g.guard(fmt.Sprintf("external job %d failed: ", job.ID), func() {
				if job.Op == "send" {
					g.dispatch(job)
				} else {
					g.cancelJob(job)
				}
			})
		})
		if !started {
			g.mu.Lock()
			delete(g.claimed, job.ID)
			g.mu.Unlock()
		}
	}
	return next
}

func (g *Gateway) fail(taskID, text string) {
	g.apply(taskID, engine.TaskChange{State: engine.StateFailed, Message: g.engine.AgentMessage(text)})
}

// apply returns false when the hub task is already final, for example after a cancel.
func (g *Gateway) apply(taskID string, change engine.TaskChange) bool {
	_, err := g.engine.ApplyChange(taskID, change)
	var hubErr *engine.Error
	if errors.As(err, &hubErr) && (hubErr.Reason == engine.ReasonTaskTerminal || hubErr.Reason == engine.ReasonTaskNotFound) {
		return false
	}
	check(err)
	return true
}

func (g *Gateway) working(taskID string) bool {
	state := must(g.engine.TaskState(taskID))
	if state == "" || engine.IsTerminal(state) {
		return false
	}
	if state == engine.StateWorking {
		return true
	}
	return g.apply(taskID, engine.TaskChange{State: engine.StateWorking})
}

func (g *Gateway) logOnce(key, line string) {
	g.mu.Lock()
	seen := g.logged[key]
	g.logged[key] = true
	g.mu.Unlock()
	if !seen {
		g.log(line)
	}
}

func (g *Gateway) dispatch(job engine.ExternalJob) {
	state := must(g.engine.TaskState(job.TaskID))
	if state == "" || engine.IsTerminal(state) {
		check(g.engine.DropExternalJob(job.ID))
		if state != engine.StateCanceled {
			shown := string(state)
			if shown == "" {
				shown = "gone"
			}
			g.log(fmt.Sprintf("external job %d: task %s is %s, message not sent", job.ID, job.TaskID, shown))
		}
		return
	}
	g.mu.Lock()
	agent := g.agents[job.AgentID]
	g.mu.Unlock()
	if agent == nil {
		check(g.engine.DropExternalJob(job.ID))
		g.fail(job.TaskID, job.AgentID+" is not configured on the hub")
		return
	}
	var outbound *engine.OutboundMessage
	if job.MessageID != nil {
		outbound = must(g.engine.OutboundMessage(job.TaskID, *job.MessageID))
	}
	if outbound == nil {
		check(g.engine.DropExternalJob(job.ID))
		messageID := "?"
		if job.MessageID != nil {
			messageID = *job.MessageID
		}
		g.log(fmt.Sprintf("external job %d: message %s of task %s is gone", job.ID, messageID, job.TaskID))
		return
	}
	remote := must(g.engine.StartRemote(job))
	r := g.track(remote)
	defer g.untrack(r)
	if r == nil {
		return
	}
	messageID := g.engine.NewID()
	if job.MessageID != nil {
		messageID = *job.MessageID
	}
	result := g.send(agent, remote, outbound.ContextID, messageID,
		OutboundText(outbound.Text, outbound.SenderName, outbound.SenderHost), r.ctx)
	if g.handleStop(r, remote) {
		return
	}
	switch result.kind {
	case settled:
		check(g.engine.RemoteDone(remote))
		g.setOnline(agent, true)
	case failed:
		check(g.engine.RemoteDone(remote))
		g.fail(job.TaskID, result.detail)
	case retry:
		g.retry(agent, job, remote)
	case unreachable:
		g.unreachable(agent, job, remote, result.detail)
	case drop:
		g.setOnline(agent, true)
		g.recover(agent, remote, r, "drop")
	}
}

// track registers a running dispatch. It returns nil after Stop, and the rows stay for the next start.
func (g *Gateway) track(remote *engine.RemoteTask) *running {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return nil
	}
	ctx, cancel := context.WithCancel(g.shutdown)
	r := &running{taskID: remote.TaskID, agentID: remote.AgentID, ctx: ctx, cancel: cancel}
	g.running[remote.TaskID] = r
	return r
}

func (g *Gateway) untrack(r *running) {
	if r == nil {
		return
	}
	r.cancel()
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.running[r.taskID] == r {
		delete(g.running, r.taskID)
	}
}

// abort needs mu.
func (g *Gateway) abort(r *running, reason stopReason) {
	if r.reason == "" {
		r.reason = reason
	}
	r.cancel()
}

// handleStop reports a dispatch that ended on purpose. A hub stop leaves the rows for the next start.
func (g *Gateway) handleStop(r *running, remote *engine.RemoteTask) bool {
	g.mu.Lock()
	stopped, reason := g.stopped, r.reason
	g.mu.Unlock()
	switch {
	case stopped || reason == reasonStop:
		return true
	case reason == reasonCancel:
		check(g.engine.RemoteDone(remote))
		return true
	case reason == reasonRemoved:
		check(g.engine.RemoteDone(remote))
		g.fail(remote.TaskID, remote.AgentID+" was removed from the hub config")
		return true
	}
	return false
}

func (g *Gateway) retry(agent *external.Agent, job engine.ExternalJob, remote *engine.RemoteTask) {
	attempts := job.Attempts + 1
	if attempts > RateLimitRetries {
		check(g.engine.RemoteDone(remote))
		g.fail(job.TaskID, fmt.Sprintf("%s rate limit: the hub gave up after %d retries", agent.Name, RateLimitRetries))
		return
	}
	job.Attempts = attempts
	job.NextAt = g.engine.Now() + agent.Retry.Milliseconds()
	check(g.engine.RequeueRemote(remote, job))
}

// Backoff is the wait before the next try of an agent that the hub cannot connect to: the poll interval,
// doubled for each attempt, at most 60 seconds.
func Backoff(poll time.Duration, attempts int64) time.Duration {
	delay := poll
	for range attempts {
		if delay >= offlineBackoffMax {
			break
		}
		delay *= 2
	}
	return min(delay, offlineBackoffMax)
}

func (g *Gateway) unreachable(agent *external.Agent, job engine.ExternalJob, remote *engine.RemoteTask, detail string) {
	g.setOnline(agent, false)
	now := g.engine.Now()
	if now-job.FirstAt >= agent.Offline.Milliseconds() {
		check(g.engine.RemoteDone(remote))
		g.fail(job.TaskID, agent.Name+" is unreachable: "+detail)
		return
	}
	g.logOnce("unreachable:"+agent.ID, "external agent "+agent.ID+" is unreachable, retrying: "+detail)
	job.NextAt = now + Backoff(agent.Poll, job.Attempts).Milliseconds()
	job.Attempts++
	check(g.engine.RequeueRemote(remote, job))
}

// ---------- one request ----------

func (g *Gateway) request(ctx context.Context, agent *external.Agent, body *jsonx.Object, accept string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, agent.URL, bytes.NewReader(jsonx.Stringify(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+agent.Token)
	req.Header.Set("A2A-Version", "1.0")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", accept)
	return req, nil
}

// errorOutcome maps an HTTP status or a JSON-RPC error code of the agent to the hub action.
func (g *Gateway) errorOutcome(agent *external.Agent, status int, rpcErr *jsonx.Object) outcome {
	code, hasCode := rpcErr.Value("code").(float64)
	message, _ := rpcErr.String("message")
	switch {
	case status == http.StatusUnauthorized || hasCode && code == errUnauthorized:
		g.logOnce("401:"+agent.ID, "external agent "+agent.ID+" rejected the hub token")
		return outcome{failed, "hub token rejected by " + agent.Name}
	case status == http.StatusForbidden || hasCode && code == errUntrustedPeer:
		g.logOnce("403:"+agent.ID, "external agent "+agent.ID+" does not trust the hub")
		return outcome{failed, "hub not in A2A_TRUSTED_PEERS of " + agent.Name}
	case status == http.StatusTooManyRequests || hasCode && code == errRateLimited:
		return outcome{kind: retry}
	case hasCode:
		return outcome{failed, agent.Name + " answered " + jsonx.FormatNumber(code) + ": " + message}
	}
	return outcome{failed, fmt.Sprintf("%s answered HTTP %d", agent.Name, status)}
}

func (g *Gateway) send(agent *external.Agent, remote *engine.RemoteTask, contextID, messageID, text string, ctx context.Context) outcome {
	body := jsonx.NewObject("jsonrpc", "2.0", "id", g.engine.NewID(), "method", "SendStreamingMessage",
		"params", jsonx.NewObject("message", jsonx.NewObject(
			"role", "ROLE_USER", "messageId", messageID, "contextId", contextID,
			"parts", []any{jsonx.NewObject("text", text)})))
	local, cancel := context.WithCancel(ctx)
	defer cancel()
	var headersLate atomic.Bool
	connect := time.AfterFunc(g.connectTimeout, func() {
		headersLate.Store(true)
		cancel()
	})
	var res *http.Response
	req, err := g.request(local, agent, body, "text/event-stream")
	if err == nil {
		res, err = g.client.Do(req)
	}
	connect.Stop()
	if err != nil {
		if ctx.Err() != nil {
			return outcome{kind: drop}
		}
		if code := errorCode(err); !headersLate.Load() && connectErrors[code] {
			return outcome{unreachable, explain(err)}
		}
		g.log(fmt.Sprintf("external agent %s: request of task %s dropped: %s", agent.ID, remote.TaskID, explain(err)))
		return outcome{kind: drop}
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK || !strings.Contains(res.Header.Get("Content-Type"), "text/event-stream") {
		data, _ := readLimited(res.Body)
		var rpcErr *jsonx.Object
		if parsed, err := jsonx.Parse(data); err == nil {
			envelope, _ := parsed.(*jsonx.Object)
			rpcErr, _ = envelope.Value("error").(*jsonx.Object)
		}
		return g.errorOutcome(agent, res.StatusCode, rpcErr)
	}
	return g.readStream(agent, remote, res.Body, cancel)
}

func (g *Gateway) readStream(agent *external.Agent, remote *engine.RemoteTask, body io.Reader, cancel context.CancelFunc) outcome {
	idle := time.AfterFunc(agent.Idle, cancel)
	defer idle.Stop()
	var late atomic.Bool
	left := time.Duration(remote.DispatchedAt+agent.ReplyTimeout.Milliseconds()-g.engine.Now()) * time.Millisecond
	deadline := time.AfterFunc(max(0, left), func() {
		late.Store(true)
		cancel()
	})
	defer deadline.Stop()
	timedOut := outcome{failed, fmt.Sprintf("%s gave no answer within %d s", agent.Name, int64(math.Round(agent.ReplyTimeout.Seconds())))}
	var parser Parser
	var artifacts []*jsonx.Object
	buf := make([]byte, 32*1024)
	for {
		n, err := body.Read(buf)
		if late.Load() {
			return timedOut
		}
		if n > 0 {
			idle.Reset(agent.Idle)
			for _, frame := range parser.Push(buf[:n]) {
				if result, done := g.frame(agent, remote, frame, &artifacts); done {
					return result
				}
			}
			if parser.Buffered() > BodyLimit {
				g.log(fmt.Sprintf("external agent %s: event is larger than %d bytes", agent.ID, BodyLimit))
				return outcome{kind: drop}
			}
		}
		if err != nil {
			return outcome{kind: drop}
		}
	}
}

// frame handles one SSE event. done is true once the hub task got its final or interrupted state.
func (g *Gateway) frame(agent *external.Agent, remote *engine.RemoteTask, frame Frame, artifacts *[]*jsonx.Object) (outcome, bool) {
	if frame.Data == "" {
		return outcome{}, false
	}
	parsed, err := jsonx.Parse([]byte(frame.Data))
	if err != nil {
		g.log("external agent " + agent.ID + ": frame is not JSON")
		return outcome{}, false
	}
	envelope, _ := parsed.(*jsonx.Object)
	if rpcErr, ok := envelope.Value("error").(*jsonx.Object); ok {
		return g.errorOutcome(agent, http.StatusOK, rpcErr), true
	}
	event, ok := envelope.Value("result").(*jsonx.Object)
	if !ok {
		event = envelope
	}
	if g.event(agent, remote, event, artifacts) {
		return outcome{kind: settled}, true
	}
	return outcome{}, false
}

func asState(value any) (engine.State, bool) {
	s, ok := value.(string)
	if !ok || !strings.HasPrefix(s, "TASK_STATE_") {
		return "", false
	}
	return engine.State(s), true
}

func ends(state engine.State) bool { return engine.IsTerminal(state) || engine.IsInterrupted(state) }

func asArtifact(value any) *jsonx.Object {
	artifact, _ := value.(*jsonx.Object)
	id, _ := artifact.String("artifactId")
	if _, ok := artifact.Value("parts").([]any); id == "" || !ok {
		return nil
	}
	return artifact
}

func statusOf(object *jsonx.Object) *jsonx.Object {
	status, _ := object.Value("status").(*jsonx.Object)
	return status
}

// wireText joins the text parts of a message.
func wireText(message any) string {
	object, _ := message.(*jsonx.Object)
	parts, _ := object.Value("parts").([]any)
	var texts []string
	for _, part := range parts {
		item, _ := part.(*jsonx.Object)
		if text, ok := item.String("text"); ok {
			texts = append(texts, text)
		}
	}
	return strings.Join(texts, "\n")
}

func (g *Gateway) noteRemote(remote *engine.RemoteTask, id, contextID any) {
	remoteID, ok := id.(string)
	if remote.RemoteTaskID != nil || !ok || remoteID == "" {
		return
	}
	remote.RemoteTaskID = &remoteID
	remote.RemoteContextID = nil
	if s, ok := contextID.(string); ok {
		remote.RemoteContextID = &s
	}
	remote.State = "running"
	check(g.engine.RemoteStarted(remote, remoteID, remote.RemoteContextID))
}

// event returns true when the hub task is settled for this dispatch: final, interrupted, or already final.
// Artifacts wait for the final status, so the sender gets the result in one update with the final state.
func (g *Gateway) event(agent *external.Agent, remote *engine.RemoteTask, event *jsonx.Object, artifacts *[]*jsonx.Object) bool {
	if value, ok := event.Get("task"); ok {
		task, _ := value.(*jsonx.Object)
		g.noteRemote(remote, task.Value("id"), task.Value("contextId"))
		if state, ok := asState(statusOf(task).Value("state")); ok && ends(state) {
			return g.settle(agent, remote, task, false)
		}
		return !g.working(remote.TaskID)
	}
	if value, ok := event.Get("statusUpdate"); ok {
		update, _ := value.(*jsonx.Object)
		g.noteRemote(remote, update.Value("taskId"), update.Value("contextId"))
		status := statusOf(update)
		state, ok := asState(status.Value("state"))
		if !ok || !ends(state) {
			return !g.working(remote.TaskID)
		}
		change := engine.TaskChange{State: state}
		if text := wireText(status.Value("message")); text != "" {
			change.Message = g.engine.AgentMessage(text)
		}
		for _, artifact := range *artifacts {
			change.Artifacts = append(change.Artifacts, engine.ArtifactChange{Artifact: artifact})
		}
		g.apply(remote.TaskID, change)
		return true
	}
	if value, ok := event.Get("artifactUpdate"); ok {
		update, _ := value.(*jsonx.Object)
		g.noteRemote(remote, update.Value("taskId"), update.Value("contextId"))
		artifact := asArtifact(update.Value("artifact"))
		if artifact == nil {
			return false
		}
		id, _ := artifact.String("artifactId")
		parts, _ := artifact.Value("parts").([]any)
		if appendFlag, _ := update.Value("append").(bool); appendFlag {
			for _, previous := range *artifacts {
				if previousID, _ := previous.String("artifactId"); previousID == id {
					old, _ := previous.Value("parts").([]any)
					previous.Set("parts", append(append([]any{}, old...), parts...))
					return false
				}
			}
		}
		*artifacts = append(*artifacts, artifact.Clone().Set("parts", append([]any{}, parts...)))
		return false
	}
	if value, ok := event.Get("message"); ok {
		message, _ := value.(*jsonx.Object)
		change := engine.TaskChange{State: engine.StateCompleted}
		if message != nil {
			if text := engine.MessageText(message); text != "" {
				change.Message = g.engine.AgentMessage(text)
			}
		}
		g.apply(remote.TaskID, change)
		return true
	}
	return false
}

// settle applies a final or interrupted remote task. A failure seen after a drop tells the sender to ask again.
func (g *Gateway) settle(agent *external.Agent, remote *engine.RemoteTask, task *jsonx.Object, polled bool) bool {
	status := statusOf(task)
	state, ok := asState(status.Value("state"))
	if !ok {
		return false
	}
	text := wireText(status.Value("message"))
	var artifacts []engine.ArtifactChange
	list, _ := task.Value("artifacts").([]any)
	for _, item := range list {
		if artifact := asArtifact(item); artifact != nil {
			artifacts = append(artifacts, engine.ArtifactChange{Artifact: artifact})
		}
	}
	change := engine.TaskChange{State: state}
	switch {
	case state == engine.StateFailed && polled:
		detail := ""
		if text != "" {
			detail = ": " + text
		}
		change.Message = g.engine.AgentMessage(agent.Name + " failed the task after the stream dropped" + detail + ". " + askAgain)
	case state == engine.StateCompleted && len(artifacts) > 0:
		change.Artifacts = artifacts
	case text != "":
		change.Message = g.engine.AgentMessage(text)
	}
	g.apply(remote.TaskID, change)
	return true
}

// ---------- recovery ----------

type rpcAnswer struct {
	status int
	result any
	err    *jsonx.Object
}

func (g *Gateway) rpc(ctx context.Context, agent *external.Agent, method string, params *jsonx.Object) (*rpcAnswer, error) {
	ctx, cancel := context.WithTimeout(ctx, g.connectTimeout)
	defer cancel()
	req, err := g.request(ctx, agent, jsonx.NewObject("jsonrpc", "2.0", "id", g.engine.NewID(), "method", method,
		"params", params), "application/json")
	if err != nil {
		return nil, err
	}
	res, err := g.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	data, err := readLimited(res.Body)
	if err != nil {
		return nil, err
	}
	answer := &rpcAnswer{status: res.StatusCode}
	if parsed, err := jsonx.Parse(data); err == nil {
		if body, ok := parsed.(*jsonx.Object); ok {
			answer.result = body.Value("result")
			answer.err, _ = body.Value("error").(*jsonx.Object)
		}
	}
	return answer, nil
}

// resume picks up a dispatch that was open when the hub stopped.
func (g *Gateway) resume(remote *engine.RemoteTask) {
	g.mu.Lock()
	agent := g.agents[remote.AgentID]
	g.mu.Unlock()
	if agent == nil {
		g.guard("external task "+remote.TaskID+" recovery failed: ", func() {
			check(g.engine.RemoteDone(remote))
			g.fail(remote.TaskID, remote.AgentID+" is not configured on the hub")
		})
		return
	}
	r := g.track(remote)
	if r == nil {
		return
	}
	started := g.goRun(func() {
		defer func() {
			g.untrack(r)
			g.kick()
		}()
		g.guard("external task "+remote.TaskID+" recovery failed: ", func() { g.recover(agent, remote, r, "restart") })
	})
	if !started {
		g.untrack(r)
	}
}

// recover finds the remote task after a dropped stream or a hub restart, then polls GetTask until it ends.
// Hermes fails a streamed task when its client goes away, so this mostly settles the hub task as failed.
func (g *Gateway) recover(agent *external.Agent, remote *engine.RemoteTask, r *running, cause string) {
	if remote.RemoteTaskID == nil {
		id, contextID, err := g.findRemote(r.ctx, agent, remote)
		if err != nil {
			g.log("external agent " + agent.ID + ": ListTasks failed: " + explain(err))
		}
		if g.handleStop(r, remote) {
			return
		}
		if id == "" {
			check(g.engine.RemoteDone(remote))
			text := agent.Name + " did not confirm the task before the stream dropped. " + askAgain
			if cause == "restart" {
				text = "hub restarted during dispatch"
			}
			g.fail(remote.TaskID, text)
			return
		}
		g.noteRemote(remote, id, contextID)
	}
	deadline := remote.DispatchedAt + agent.ReplyTimeout.Milliseconds()
	for {
		remoteID := jsonx.Null
		if remote.RemoteTaskID != nil {
			remoteID = *remote.RemoteTaskID
		}
		answer, err := g.rpc(r.ctx, agent, "GetTask", jsonx.NewObject("id", remoteID))
		if err != nil && r.ctx.Err() == nil {
			g.log("external agent " + agent.ID + ": GetTask failed: " + explain(err))
		}
		if g.handleStop(r, remote) {
			return
		}
		check(g.engine.RemotePolled(remote))
		if answer != nil {
			if code, _ := answer.err.Value("code").(float64); code == errTaskNotFound {
				check(g.engine.RemoteDone(remote))
				g.fail(remote.TaskID, agent.Name+" lost the task. "+askAgain)
				return
			}
			task, _ := answer.result.(*jsonx.Object)
			if state, ok := asState(statusOf(task).Value("state")); task != nil && ok && ends(state) {
				g.settle(agent, remote, task, true)
				check(g.engine.RemoteDone(remote))
				return
			}
		}
		if g.engine.Now() >= deadline {
			check(g.engine.RemoteDone(remote))
			g.fail(remote.TaskID, fmt.Sprintf("%s gave no answer within %d s", agent.Name, int64(math.Round(agent.ReplyTimeout.Seconds()))))
			return
		}
		if current := must(g.engine.TaskState(remote.TaskID)); current == "" || engine.IsTerminal(current) {
			check(g.engine.RemoteDone(remote))
			return
		}
		select {
		case <-time.After(agent.Poll):
		case <-r.ctx.Done():
		}
		if g.handleStop(r, remote) {
			return
		}
	}
}

// findRemote gives the newest remote task of the context that no dispatch claims yet. "" means none.
func (g *Gateway) findRemote(ctx context.Context, agent *external.Agent, remote *engine.RemoteTask) (string, any, error) {
	answer, err := g.rpc(ctx, agent, "ListTasks", jsonx.NewObject("contextId", remote.ContextID, "pageSize", float64(20)))
	if err != nil {
		return "", nil, err
	}
	result, _ := answer.result.(*jsonx.Object)
	tasks, _ := result.Value("tasks").([]any)
	for _, item := range tasks {
		task, _ := item.(*jsonx.Object)
		id, ok := task.String("id")
		if !ok || must(g.engine.RemoteKnown(agent.ID, id)) {
			continue
		}
		return id, task.Value("contextId"), nil
	}
	return "", nil, nil
}

// ---------- cancel ----------

func (g *Gateway) cancelJob(job engine.ExternalJob) {
	check(g.engine.DropExternalJob(job.ID))
	remote := must(g.engine.LatestRemote(job.TaskID))
	g.mu.Lock()
	agent := g.agents[job.AgentID]
	r := g.running[job.TaskID]
	g.mu.Unlock()
	if agent != nil && remote != nil && remote.RemoteTaskID != nil && remote.State != "done" {
		ctx := g.shutdown
		if r != nil {
			ctx = r.ctx
		}
		answer, err := g.rpc(ctx, agent, "CancelTask", jsonx.NewObject("id", *remote.RemoteTaskID))
		switch {
		case err != nil:
			g.log(fmt.Sprintf("external agent %s: CancelTask %s failed: %s", agent.ID, *remote.RemoteTaskID, explain(err)))
		case answer.err != nil:
			message, ok := answer.err.String("message")
			if !ok {
				message = "undefined"
			}
			g.log(fmt.Sprintf("external agent %s: CancelTask %s: %s", agent.ID, *remote.RemoteTaskID, message))
		}
	}
	g.mu.Lock()
	if r != nil {
		g.abort(r, reasonCancel)
	}
	stopped := g.stopped
	g.mu.Unlock()
	if r == nil && remote != nil && remote.State != "done" && !stopped {
		check(g.engine.RemoteDone(remote))
	}
}

// ---------- errors ----------

// errorCode names a connect failure with the code that Node gives it, so the task text and the logs read the
// same as with the TypeScript hub.
func errorCode(err error) string {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		if dnsErr.IsNotFound {
			return "ENOTFOUND"
		}
		return "EAI_AGAIN"
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.ECONNREFUSED:
			return "ECONNREFUSED"
		case syscall.EHOSTUNREACH:
			return "EHOSTUNREACH"
		case syscall.ENETUNREACH:
			return "ENETUNREACH"
		case syscall.ECONNRESET:
			return "ECONNRESET"
		}
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" && opErr.Timeout() {
		return "UND_ERR_CONNECT_TIMEOUT"
	}
	return ""
}

// explain gives the error text without the method and URL that net/http puts in front, plus the code.
func explain(err error) string {
	message := err.Error()
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		message = urlErr.Err.Error()
	}
	if code := errorCode(err); code != "" && !strings.Contains(message, code) {
		message += " (" + code + ")"
	}
	return message
}
