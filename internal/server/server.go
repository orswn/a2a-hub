// Package server is the HTTP front of the hub.
package server

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/orswn/a2a-hub/internal/a2aapi"
	"github.com/orswn/a2a-hub/internal/auth"
	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/jsonx"
	"github.com/orswn/a2a-hub/internal/link"
)

// BodyLimit is the largest request body.
const BodyLimit = 1024 * 1024

const cardMaxAge = 60

// Options configures a Server.
type Options struct {
	// Version is the hub version that /healthz and the cards report.
	Version string
	Tokens  *auth.TokenTable
	// Engine answers every route but /healthz. Without it the other routes give 404.
	Engine *engine.Engine
	// Agents counts the online agents for /healthz. nil uses Engine, or reports 0 without one.
	Agents   AgentCounter
	Link     *link.Hub
	Builtins map[string]a2aapi.Builtin
	// PublicURL is the base URL of the cards. Empty means the URL of the request.
	PublicURL string
	// Listen is host:port, the base URL when a request has no Host header.
	Listen    string
	Heartbeat time.Duration
	Holds     *a2aapi.HoldLimiter
	Log       func(line string)
}

// AgentCounter counts the online agents.
type AgentCounter interface {
	OnlineCount() (int64, error)
}

// Server answers the hub routes.
type Server struct {
	opts   Options
	tokens atomic.Pointer[auth.TokenTable]
	agents AgentCounter
	log    func(line string)
	open   atomic.Int64
}

// New makes a Server.
func New(opts Options) *Server {
	s := &Server{opts: opts, agents: opts.Agents, log: opts.Log}
	if s.log == nil {
		s.log = func(string) {}
	}
	if s.agents == nil && opts.Engine != nil {
		s.agents = opts.Engine
	}
	if s.opts.Holds == nil {
		s.opts.Holds = a2aapi.NewHoldLimiter(0)
	}
	if s.opts.Heartbeat <= 0 {
		s.opts.Heartbeat = 15 * time.Second
	}
	tokens := opts.Tokens
	if tokens == nil {
		tokens = &auth.TokenTable{}
	}
	s.tokens.Store(tokens)
	return s
}

// SetTokens replaces the token table, for a reload of the token file. Open streams stay.
func (s *Server) SetTokens(tokens *auth.TokenTable) { s.tokens.Store(tokens) }

type health struct {
	OK      bool   `json:"ok"`
	Version string `json:"version"`
	Agents  int64  `json:"agents"`
}

// httpError is a refusal with an HTTP status and the error body {"error": message}.
type httpError struct {
	status  int
	message string
}

func (e *httpError) Error() string { return e.message }

func fail(status int, message string) error { return &httpError{status: status, message: message} }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.open.Add(1)
	defer s.open.Add(-1)
	tracked := &trackingWriter{ResponseWriter: w}
	defer func() {
		if recovered := recover(); recovered != nil {
			if err, ok := recovered.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				panic(recovered)
			}
			s.sendError(tracked, fmt.Errorf("%v", recovered))
		}
	}()
	if err := s.handle(tracked, r); err != nil {
		s.sendError(tracked, err)
	}
}

// WaitHandlers waits until no request is open, or until the timeout. It reports whether none is open.
// http.Server.Close does not wait for the handlers, and they may still call the engine.
func (s *Server) WaitHandlers(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for s.open.Load() > 0 {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
	return true
}

// trackingWriter knows whether the status line went out, so a late error does not write a second one.
type trackingWriter struct {
	http.ResponseWriter
	started bool
}

func (w *trackingWriter) WriteHeader(status int) {
	w.ResponseWriter.WriteHeader(status)
	w.started = true
}

func (w *trackingWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.started = true
	return n, err
}

func (w *trackingWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (s *Server) sendError(w *trackingWriter, err error) {
	if w.started {
		return
	}
	var hubErr *engine.Error
	if errors.As(err, &hubErr) {
		body := jsonx.NewObject("error", hubErr.Message)
		if hubErr.Details != nil {
			if details, err := jsonx.ToValue(hubErr.Details); err == nil {
				if obj, ok := details.(*jsonx.Object); ok {
					for _, key := range obj.Keys() {
						body.Set(key, obj.Value(key))
					}
				}
			}
		}
		writeJSON(w, hubErr.Status, body, nil)
		return
	}
	var plain *httpError
	if errors.As(err, &plain) {
		writeJSON(w, plain.status, jsonx.NewObject("error", plain.message), nil)
		return
	}
	s.log("internal error: " + err.Error())
	writeJSON(w, http.StatusInternalServerError, jsonx.NewObject("error", "internal error"), nil)
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) error {
	segments, ok := pathSegments(r.URL.EscapedPath())
	if !ok {
		return fail(http.StatusBadRequest, "malformed percent escape in path")
	}
	route := matchRoute(r.Method, segments)
	if route == "GET /healthz" {
		var agents int64
		if s.agents != nil {
			n, err := s.agents.OnlineCount()
			if err != nil {
				return err
			}
			agents = n
		}
		writeJSON(w, http.StatusOK, health{OK: true, Version: s.opts.Version, Agents: agents}, nil)
		return nil
	}
	if s.opts.Engine == nil {
		return fail(http.StatusNotFound, "not found")
	}
	var root string
	var rest []string
	if len(segments) > 0 {
		root, rest = segments[0], segments[1:]
	}
	switch {
	case root == "a2a" && len(rest) > 0 && rest[0] != "":
		ref := rest[0]
		if r.Method == http.MethodGet && len(rest) == 3 && rest[1] == ".well-known" &&
			(rest[2] == "agent-card.json" || rest[2] == "agent.json") {
			return s.card(w, r, ref)
		}
		if r.Method == http.MethodPost && len(rest) == 1 {
			return s.jsonRPC(w, r, ref)
		}
		return fail(http.StatusNotFound, "not found")
	case root == "link":
		return s.linkRoute(w, r, route, rest)
	case route == "GET /agents" || route == "GET /agents/:id":
		if _, err := s.requireHost(r); err != nil {
			return err
		}
		if route == "GET /agents/:id" {
			row, err := s.opts.Engine.RequireAgent(rest[0])
			if err != nil {
				return err
			}
			writeJSON(w, http.StatusOK, s.opts.Engine.AgentInfo(row), nil)
			return nil
		}
		agents, err := s.opts.Engine.ListAgents(r.URL.Query().Get("offline") == "1")
		if err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, struct {
			Agents []engine.AgentInfo `json:"agents"`
		}{agents}, nil)
		return nil
	}
	return fail(http.StatusNotFound, "not found")
}

// principal authenticates the request. anonymous reports a request without a token that allowAnonymous let in.
func (s *Server) principal(r *http.Request, allowAnonymous bool) (principal auth.Principal, anonymous bool, err error) {
	header := firstHeader(r, "Authorization")
	principal, ok := auth.Authenticate(header, s.tokens.Load(), allowAnonymous)
	if !ok {
		return auth.Principal{}, false, fail(http.StatusUnauthorized, "unauthorized")
	}
	return principal, header == "", nil
}

func (s *Server) requireHost(r *http.Request) (auth.Principal, error) {
	principal, _, err := s.principal(r, false)
	if err != nil {
		return principal, err
	}
	if principal.Kind != auth.KindHost {
		return principal, fail(http.StatusForbidden, "host principal required")
	}
	return principal, nil
}

func firstHeader(r *http.Request, name string) string {
	values := r.Header.Values(name)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func firstListItem(value string) string {
	first, _, _ := strings.Cut(value, ",")
	return strings.TrimSpace(first)
}

func (s *Server) baseURL(r *http.Request) string {
	if s.opts.PublicURL != "" {
		return s.opts.PublicURL
	}
	proto := firstListItem(firstHeader(r, "X-Forwarded-Proto"))
	host := firstListItem(firstHeader(r, "X-Forwarded-Host"))
	if proto != "" && host != "" {
		return proto + "://" + host
	}
	if r.Host != "" {
		return "http://" + r.Host
	}
	return "http://" + s.opts.Listen
}

func (s *Server) resolve(ref string) (*engine.RefResult, error) {
	found, err := s.opts.Engine.ResolveRef(ref)
	if err != nil {
		return nil, err
	}
	if found == nil {
		return nil, fail(http.StatusNotFound, "unknown agent "+ref)
	}
	return found, nil
}

func (s *Server) cardFor(r *http.Request, agent *engine.AgentRow) *jsonx.Object {
	return a2aapi.BuildCard(a2aapi.CardInput{
		Agent: agent, BaseURL: s.baseURL(r), Version: s.opts.Version, Builtin: s.opts.Builtins[agent.ID],
	})
}

// isBuiltinRef reports a built-in agent. It answers without a token in test mode; its ids and names are public.
func (s *Server) isBuiltinRef(ref string) bool {
	if _, ok := s.opts.Builtins[ref]; ok {
		return true
	}
	if !strings.HasPrefix(ref, "@") {
		return false
	}
	for _, agent := range s.opts.Builtins {
		if agent.Name() == ref[1:] {
			return true
		}
	}
	return false
}

func (s *Server) card(w http.ResponseWriter, r *http.Request, ref string) error {
	found, err := s.resolve(ref)
	if err != nil {
		return err
	}
	if found.Agent == nil {
		return fail(http.StatusConflict, "agent alias "+ref+" is ambiguous")
	}
	body := jsonx.Stringify(s.cardFor(r, found.Agent))
	etag := a2aapi.CardEtag(body)
	header := w.Header()
	header.Set("Cache-Control", "max-age="+strconv.Itoa(cardMaxAge))
	header.Set("ETag", etag)
	if match := firstHeader(r, "If-None-Match"); match != "" {
		for _, tag := range strings.Split(match, ",") {
			if tag = strings.TrimSpace(tag); tag == etag || tag == "*" {
				w.WriteHeader(http.StatusNotModified)
				return nil
			}
		}
	}
	writeRaw(w, http.StatusOK, body)
	return nil
}

func readBody(r *http.Request) ([]byte, error) {
	if r.ContentLength > BodyLimit {
		return nil, fail(http.StatusRequestEntityTooLarge, "body is larger than 1 MB")
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, BodyLimit+1))
	if err != nil {
		return nil, err
	}
	if len(body) > BodyLimit {
		return nil, fail(http.StatusRequestEntityTooLarge, "body is larger than 1 MB")
	}
	return body, nil
}

func isJSSpace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0xa0, 0x1680, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000, 0xfeff:
		return true
	}
	return r >= 0x2000 && r <= 0x200a
}

// readJSON reads a JSON object body. An empty body is an empty object.
func readJSON(r *http.Request) (*jsonx.Object, error) {
	body, err := readBody(r)
	if err != nil {
		return nil, err
	}
	if strings.TrimFunc(string(body), isJSSpace) == "" {
		return jsonx.NewObject(), nil
	}
	value, err := jsonx.Parse(body)
	if err != nil {
		return nil, fail(http.StatusBadRequest, "body is not valid JSON")
	}
	obj, ok := value.(*jsonx.Object)
	if !ok {
		return nil, fail(http.StatusBadRequest, "body must be a JSON object")
	}
	return obj, nil
}

var digits = regexp.MustCompile(`^\d+$`)

const maxSafeInteger = 1<<53 - 1

// lastEventID reads the Last-Event-ID header, else the lastEventId query parameter. A value that is not a
// non-negative safe integer is ignored: the stream then replays every unacked event.
func lastEventID(r *http.Request) *int64 {
	raw := firstHeader(r, "Last-Event-ID")
	if len(r.Header.Values("Last-Event-ID")) == 0 {
		raw = r.URL.Query().Get("lastEventId")
	}
	value := strings.TrimFunc(raw, isJSSpace)
	if !digits.MatchString(value) {
		return nil
	}
	n, err := strconv.ParseFloat(value, 64)
	if err != nil || n > maxSafeInteger {
		return nil
	}
	seq := int64(n)
	return &seq
}

var versionPattern = regexp.MustCompile(`^(\d+)\.(\d+)(?:\.\d+)?$`)

// requestedVersion reads the A2A-Version header or query parameter. 1.0.3 counts as 1.0, and none means 0.3.
func requestedVersion(r *http.Request) string {
	raw := firstHeader(r, "A2A-Version")
	if len(r.Header.Values("A2A-Version")) == 0 {
		raw = r.URL.Query().Get("A2A-Version")
	}
	raw = strings.TrimFunc(raw, isJSSpace)
	if raw == "" {
		return "0.3"
	}
	if match := versionPattern.FindStringSubmatch(raw); match != nil {
		return match[1] + "." + match[2]
	}
	return raw
}

// pathSegments splits a path into its non-empty decoded segments. false means a malformed percent escape.
// Dot segments stay, and a leading "//" is not an authority: the TypeScript hub differs in both.
func pathSegments(escaped string) ([]string, bool) {
	var segments []string
	for _, part := range strings.Split(escaped, "/") {
		if part == "" {
			continue
		}
		decoded, err := url.PathUnescape(part)
		if err != nil || !utf8.ValidString(decoded) {
			return nil, false
		}
		segments = append(segments, decoded)
	}
	return segments, true
}

// writeJSON writes the body like JSON.stringify with exactly "Content-Type: application/json".
func writeJSON(w http.ResponseWriter, status int, body any, header http.Header) {
	text, err := jsonx.Marshal(body)
	if err != nil {
		status = http.StatusInternalServerError
		text = []byte(`{"error":"internal error"}`)
	}
	for key, values := range header {
		w.Header()[key] = values
	}
	writeRaw(w, status, text)
}

func writeRaw(w http.ResponseWriter, status int, text []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(text)))
	w.WriteHeader(status)
	// The body is JSON sent as application/json, not HTML. gosec reports G705 here in some runs only.
	_, _ = w.Write(text) //nolint:gosec,nolintlint // G705: JSON body, not HTML.
}
