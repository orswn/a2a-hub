// Package server is the HTTP front of the hub.
package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/orswn/a2a-hub/internal/auth"
)

// Options configures a Server.
type Options struct {
	// Version is the hub version that /healthz reports.
	Version string
	Tokens  *auth.TokenTable
	Log     func(line string)
}

// Server answers the hub routes.
type Server struct {
	version string
	tokens  atomic.Pointer[auth.TokenTable]
	log     func(line string)
}

// New makes a Server.
func New(opts Options) *Server {
	s := &Server{version: opts.Version, log: opts.Log}
	if s.log == nil {
		s.log = func(string) {}
	}
	tokens := opts.Tokens
	if tokens == nil {
		tokens = &auth.TokenTable{}
	}
	s.tokens.Store(tokens)
	return s
}

// SetTokens replaces the token table, for a reload of the token file.
func (s *Server) SetTokens(tokens *auth.TokenTable) { s.tokens.Store(tokens) }

type health struct {
	OK      bool   `json:"ok"`
	Version string `json:"version"`
	Agents  int    `json:"agents"`
}

type errorBody struct {
	Error string `json:"error"`
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if err, ok := recovered.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				panic(recovered)
			}
			s.log(fmt.Sprintf("internal error: %v", recovered))
			writeJSON(w, http.StatusInternalServerError, errorBody{Error: "internal error"})
		}
	}()
	segments, ok := pathSegments(r.URL.EscapedPath())
	if !ok {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "malformed percent escape in path"})
		return
	}
	if r.Method == http.MethodGet && len(segments) == 1 && segments[0] == "healthz" {
		// FIXME(okka): report the online agent count once the engine exists.
		writeJSON(w, http.StatusOK, health{OK: true, Version: s.version, Agents: 0})
		return
	}
	writeJSON(w, http.StatusNotFound, errorBody{Error: "not found"})
}

// pathSegments splits a path into its non-empty decoded segments, like the TypeScript hub. false means a
// malformed percent escape.
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

// writeJSON writes the body with exactly "Content-Type: application/json" and no HTML escapes, like JSON.stringify.
func writeJSON(w http.ResponseWriter, status int, body any) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(body); err != nil {
		status = http.StatusInternalServerError
		buf.Reset()
		buf.WriteString(`{"error":"internal error"}`)
	}
	text := bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(text)))
	w.WriteHeader(status)
	_, _ = w.Write(text)
}
