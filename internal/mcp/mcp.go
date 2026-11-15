// Package mcp is the MCP endpoint of the hub: streamable HTTP with hand-written JSON-RPC, so that the hub
// controls the protocol version that Claude Code negotiates.
package mcp

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/orswn/a2a-hub/internal/a2aapi"
	"github.com/orswn/a2a-hub/internal/auth"
	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/external"
	"github.com/orswn/a2a-hub/internal/jsonx"
	"github.com/orswn/a2a-hub/internal/link"
)

// The harnesses that the X-A2A-Harness header names.
const (
	HarnessClaude = "claude-code"
	HarnessCodex  = "codex"
	HarnessHermes = "hermes"
)

// The request headers of the endpoint.
const (
	HarnessHeader = "X-A2A-Harness"
	AgentHeader   = "X-A2A-Agent"
	SessionHeader = "X-A2A-Session"
	CwdHeader     = "X-A2A-Cwd"
	// SessionIDHeader carries the MCP session id.
	SessionIDHeader = "Mcp-Session-Id"
)

// ProtocolVersion is the MCP version that the hub offers. Claude Code skips a channel server that negotiates a
// later version, so the hub never answers server/discover.
const ProtocolVersion = "2025-11-25"

var knownVersions = map[string]bool{"2025-11-25": true, "2025-06-18": true, "2025-03-26": true, "2024-11-05": true}

const (
	codeParseError     = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeNoSession      = -32001
)

// BodyLimit is the largest request body.
const BodyLimit = 1024 * 1024

// IdleTimeout frees an MCP session without a push stream when no request came for this long.
const IdleTimeout = 24 * time.Hour

// Webhooks gives the webhook of an external agent. nil means none.
type Webhooks interface {
	Webhook(agentID string) *external.Webhook
}

// Options configures a Server.
type Options struct {
	Engine *engine.Engine
	Link   *link.Hub
	// Builtins are started like the A2A route starts them.
	Builtins map[string]a2aapi.Builtin
	Webhooks Webhooks
	Version  string
	// PresenceGrace frees a session this long after its push stream closed.
	PresenceGrace time.Duration
	// IdleTimeout defaults to IdleTimeout.
	IdleTimeout time.Duration
	// RetryDelays are the waits between the tries of a webhook post. Their count plus one is the number of tries.
	RetryDelays []time.Duration
	// Client posts webhooks. nil uses a client with a 30 s timeout.
	Client *http.Client
	// Now gives the webhook timestamp. nil uses time.Now.
	Now func() time.Time
	Log func(line string)
}

// Server is the MCP endpoint.
type Server struct {
	opts Options

	mu       sync.Mutex
	sessions map[string]*session
	// owner is the Claude Code session whose stream an agent opened last.
	owner map[string]*session

	waitMu  sync.Mutex
	waiters map[*waiter]struct{}

	pushes   chan engine.InboxNotice
	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}
	offInbox func()
}

type session struct {
	id        string
	harness   string
	principal auth.Principal
	// agent is the bound agent. Codex binds per call.
	agent string
	cwd   string
	// streamed is set once a push stream opened.
	streamed bool
	// streams counts the open push streams. A client may reconnect before the hub sees the old stream end.
	streams  int
	replaced bool
	expiry   *time.Timer
}

// New makes a Server and starts its webhook sender. Stop ends it.
func New(opts Options) *Server {
	if opts.Log == nil {
		opts.Log = func(string) {}
	}
	if opts.IdleTimeout <= 0 {
		opts.IdleTimeout = IdleTimeout
	}
	if opts.RetryDelays == nil {
		opts.RetryDelays = []time.Duration{time.Second, 4 * time.Second}
	}
	if opts.Client == nil {
		// The webhook URL skips the push guard, so a receiver must not move the signed post to another address.
		opts.Client = &http.Client{
			Timeout:       30 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	s := &Server{
		opts: opts, sessions: map[string]*session{}, owner: map[string]*session{}, waiters: map[*waiter]struct{}{},
		pushes: make(chan engine.InboxNotice, 256), stop: make(chan struct{}), done: make(chan struct{}),
	}
	s.offInbox = opts.Engine.OnInbox(s.onInbox)
	go s.sendPushes()
	return s
}

// Stop ends the webhook sender and the session timers. Open push streams end with the link hub.
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		s.offInbox()
		close(s.stop)
		<-s.done
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, sess := range s.sessions {
			s.drop(sess)
		}
	})
}

// Served implements a2aapi.Served: a Claude Code agent with an MCP session gets pushes, not link events.
func (s *Server) Served(agentID string) (bound, open bool) {
	s.mu.Lock()
	for _, sess := range s.sessions {
		if sess.harness == HarnessClaude && sess.agent == agentID {
			bound = true
			break
		}
	}
	s.mu.Unlock()
	if !bound {
		return false, false
	}
	return true, s.opts.Link.Open(agentID)
}

// SessionCount is the number of open MCP sessions.
func (s *Server) SessionCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
}

func newSessionID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// drop frees a session. It needs s.mu.
func (s *Server) drop(sess *session) {
	if sess.expiry != nil {
		sess.expiry.Stop()
		sess.expiry = nil
	}
	delete(s.sessions, sess.id)
	if s.owner[sess.agent] == sess {
		delete(s.owner, sess.agent)
	}
}

// touch restarts the expiry of a session without an open push stream. It needs s.mu.
func (s *Server) touch(sess *session) {
	if sess.expiry != nil {
		sess.expiry.Stop()
		sess.expiry = nil
	}
	if sess.streams > 0 {
		return
	}
	wait := s.opts.IdleTimeout
	if sess.streamed {
		wait = s.opts.PresenceGrace
	}
	var timer *time.Timer
	timer = time.AfterFunc(wait, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if sess.expiry == timer && s.sessions[sess.id] == sess {
			s.drop(sess)
		}
	})
	sess.expiry = timer
}

// Serve answers /mcp for an authenticated principal.
func (s *Server) Serve(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	switch r.Method {
	case http.MethodPost:
		s.post(w, r, principal)
	case http.MethodGet:
		s.get(w, r, principal)
	case http.MethodDelete:
		s.remove(w, r, principal)
	default:
		w.Header().Set("Allow", "GET, POST, DELETE")
		writeJSON(w, http.StatusMethodNotAllowed, jsonx.NewObject("error", "method not allowed"), "")
	}
}

func writeJSON(w http.ResponseWriter, status int, body any, sessionID string) {
	header := w.Header()
	header.Set("Content-Type", "application/json")
	if sessionID != "" {
		header.Set(SessionIDHeader, sessionID)
	}
	w.WriteHeader(status)
	_, _ = w.Write(jsonx.Stringify(body))
}

func rpcError(id any, code int, message string) *jsonx.Object {
	if id == nil {
		id = jsonx.Null
	}
	return jsonx.NewObject("jsonrpc", "2.0", "id", id, "error", jsonx.NewObject("code", float64(code), "message", message))
}

func rpcResult(id any, result any) *jsonx.Object {
	return jsonx.NewObject("jsonrpc", "2.0", "id", id, "result", result)
}

// lookup finds the session of a request. It writes the refusal and gives nil when there is none.
func (s *Server) lookup(w http.ResponseWriter, r *http.Request, principal auth.Principal, id any) *session {
	sessionID := r.Header.Get(SessionIDHeader)
	if sessionID == "" {
		writeJSON(w, http.StatusBadRequest, rpcError(id, codeInvalidRequest, "Mcp-Session-Id header is required"), "")
		return nil
	}
	s.mu.Lock()
	sess := s.sessions[sessionID]
	if sess != nil && sess.principal != principal {
		sess = nil
	}
	if sess != nil {
		s.touch(sess)
	}
	s.mu.Unlock()
	if sess == nil {
		writeJSON(w, http.StatusNotFound, rpcError(id, codeNoSession, "Session not found"), "")
	}
	return sess
}

func (s *Server) post(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, BodyLimit))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, rpcError(nil, codeInvalidRequest, "request body is too large"), "")
			return
		}
		writeJSON(w, http.StatusBadRequest, rpcError(nil, codeParseError, "cannot read the request body"), "")
		return
	}
	parsed, err := jsonx.Parse(data)
	request, isObject := parsed.(*jsonx.Object)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, rpcError(nil, codeParseError, "body is not JSON"), "")
		return
	}
	if !isObject {
		writeJSON(w, http.StatusBadRequest, rpcError(nil, codeInvalidRequest, "body must be one JSON-RPC message"), "")
		return
	}
	id := request.Value("id")
	method, _ := request.String("method")
	switch method {
	case "server/discover":
		// Claude Code falls back to initialize only on exactly this error in an HTTP 200 response.
		writeJSON(w, http.StatusOK, rpcError(id, codeMethodNotFound, "Method not found: server/discover"), "")
		return
	case "initialize":
		s.initialize(w, r, principal, request)
		return
	}
	sess := s.lookup(w, r, principal, id)
	if sess == nil {
		return
	}
	if id == nil || method == "" {
		// A notification or a response to the hub: nothing to answer.
		w.WriteHeader(http.StatusAccepted)
		return
	}
	params, _ := request.Object("params")
	var response *jsonx.Object
	switch method {
	case "ping":
		response = rpcResult(id, jsonx.NewObject())
	case "tools/list":
		response = rpcResult(id, jsonx.NewObject("tools", toolList(sess.harness)))
	case "tools/call":
		response = s.callTool(r, sess, id, params)
	default:
		response = rpcError(id, codeMethodNotFound, "Method not found: "+method)
	}
	writeJSON(w, http.StatusOK, response, "")
}

var claudeFallbackID = regexp.MustCompile(`^claude-.+-(\d+)-\d+$`)

// claudeName is claude-<first 6 hex of the session id>, or claude-<pid> for the fallback agent id.
func claudeName(agentID, sessionID string) string {
	hexDigits := strings.Map(func(r rune) rune {
		if strings.ContainsRune("0123456789abcdefABCDEF", r) {
			return r
		}
		return -1
	}, sessionID)
	if len(hexDigits) >= 6 {
		return "claude-" + strings.ToLower(hexDigits[:6])
	}
	if match := claudeFallbackID.FindStringSubmatch(agentID); match != nil {
		return "claude-" + match[1]
	}
	return ""
}

func headerText(r *http.Request, name string) string {
	value := strings.TrimSpace(r.Header.Get(name))
	// headersHelper output may percent-encode a path with spaces or non-ASCII bytes.
	if decoded, err := url.PathUnescape(value); err == nil {
		return decoded
	}
	return value
}

func (s *Server) initialize(w http.ResponseWriter, r *http.Request, principal auth.Principal, request *jsonx.Object) {
	id := request.Value("id")
	refuse := func(status int, message string) {
		writeJSON(w, status, rpcError(id, codeInvalidRequest, message), "")
	}
	harness := strings.TrimSpace(r.Header.Get(HarnessHeader))
	sess := &session{id: newSessionID(), harness: harness, principal: principal, cwd: headerText(r, CwdHeader)}
	switch harness {
	case HarnessClaude:
		if principal.Kind != auth.KindHost {
			refuse(http.StatusForbidden, "claude-code sessions need a host token")
			return
		}
		agentID := strings.TrimSpace(r.Header.Get(AgentHeader))
		if agentID == "" {
			refuse(http.StatusBadRequest, "X-A2A-Agent is required for claude-code")
			return
		}
		labels := jsonx.NewObject(engine.HarnessLabel, HarnessClaude)
		claudeSession := headerText(r, SessionHeader)
		if claudeSession != "" {
			labels.Set("session", claudeSession)
		}
		body := jsonx.NewObject("agent", agentID, "protocol", float64(engine.HubProtocol), "labels", labels)
		existing, err := s.opts.Engine.Agent(agentID)
		if err != nil {
			s.refuseEngine(w, id, err)
			return
		}
		// The session id changes after /clear. Peers keep the first name.
		if name := claudeName(agentID, claudeSession); name != "" && existing == nil {
			body.Set("name", name)
		}
		if sess.cwd != "" {
			body.Set("cwd", sess.cwd)
		}
		if _, err := s.opts.Engine.Register(principal, body); err != nil {
			s.refuseEngine(w, id, err)
			return
		}
		sess.agent = agentID
	case HarnessCodex:
		if principal.Kind != auth.KindHost {
			refuse(http.StatusForbidden, "codex sessions need a host token")
			return
		}
	case HarnessHermes:
		row, err := s.opts.Engine.Agent(principal.Name)
		if err != nil {
			s.refuseEngine(w, id, err)
			return
		}
		if row == nil || row.Kind != engine.KindExternal {
			refuse(http.StatusForbidden, "no external agent "+principal.Name+" for this token")
			return
		}
		sess.agent = row.ID
	default:
		refuse(http.StatusBadRequest, "X-A2A-Harness must be claude-code, codex, or hermes")
		return
	}
	params, _ := request.Object("params")
	version, _ := params.String("protocolVersion")
	if !knownVersions[version] {
		version = ProtocolVersion
	}
	capabilities := jsonx.NewObject("tools", jsonx.NewObject())
	if harness == HarnessClaude {
		capabilities.Set("experimental", jsonx.NewObject("claude/channel", jsonx.NewObject()))
	}
	s.mu.Lock()
	if harness == HarnessClaude {
		// The newest session of an agent owns its pushes. An older one keeps its tools but no push stream.
		for _, other := range s.sessions {
			if other.harness == HarnessClaude && other.agent == sess.agent {
				other.replaced = true
			}
		}
	}
	s.sessions[sess.id] = sess
	s.touch(sess)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, rpcResult(id, jsonx.NewObject(
		"protocolVersion", version,
		"capabilities", capabilities,
		"serverInfo", jsonx.NewObject("name", "a2a-hub", "version", s.opts.Version),
		"instructions", Instructions,
	)), sess.id)
}

func (s *Server) refuseEngine(w http.ResponseWriter, id any, err error) {
	var hubErr *engine.Error
	if errors.As(err, &hubErr) {
		writeJSON(w, hubErr.Status, rpcError(id, codeInvalidRequest, hubErr.Message), "")
		return
	}
	s.opts.Log("mcp initialize failed: " + err.Error())
	writeJSON(w, http.StatusInternalServerError, rpcError(id, codeInvalidRequest, "internal error"), "")
}

func (s *Server) get(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	sess := s.lookup(w, r, principal, nil)
	if sess == nil {
		return
	}
	s.mu.Lock()
	push := sess.harness == HarnessClaude && !sess.replaced
	if push {
		sess.streams++
		sess.streamed = true
		s.owner[sess.agent] = sess
		s.touch(sess)
	}
	s.mu.Unlock()
	if !push {
		// Only the newest Claude Code session of an agent gets a push stream. 405 tells the client to stop asking.
		w.Header().Set("Allow", "POST, DELETE")
		writeJSON(w, http.StatusMethodNotAllowed, jsonx.NewObject("error", "this session has no push stream"), "")
		return
	}
	sink := &channelSink{engine: s.opts.Engine, agent: sess.agent}
	w.Header().Set(SessionIDHeader, sess.id)
	s.opts.Link.ServeWith(w, r, sess.agent, nil, sink)
	s.mu.Lock()
	sess.streams--
	// A newer stream of the same session replaces this one too. Only a stream of another session takes over.
	if sink.replaced && s.owner[sess.agent] != sess {
		sess.replaced = true
	}
	if s.sessions[sess.id] == sess {
		s.touch(sess)
	}
	s.mu.Unlock()
}

func (s *Server) remove(w http.ResponseWriter, r *http.Request, principal auth.Principal) {
	sess := s.lookup(w, r, principal, nil)
	if sess == nil {
		return
	}
	s.mu.Lock()
	ownsStream := sess.streams > 0 && s.owner[sess.agent] == sess
	s.drop(sess)
	s.mu.Unlock()
	if ownsStream {
		if err := s.opts.Link.OfflineNow(sess.agent); err != nil {
			s.opts.Log("mcp session end failed: " + err.Error())
		}
	}
	w.WriteHeader(http.StatusOK)
}
