package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orswn/a2a-hub/internal/auth"
	"github.com/orswn/a2a-hub/internal/jsonx"
	"github.com/orswn/a2a-hub/internal/store"
)

var (
	mars   = auth.Principal{Name: "mars", Kind: auth.KindHost}
	venus  = auth.Principal{Name: "venus", Kind: auth.KindHost}
	hermes = auth.Principal{Name: "hermes", Kind: auth.KindPeer}

	fromHermes = Caller{Principal: hermes}
)

const day = 24 * 60 * 60 * 1000

var startTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

type agentEvent struct {
	agent string
	event LinkEvent
}

type testEnv struct {
	t       *testing.T
	db      *sql.DB
	engine  *Engine
	clock   atomic.Int64
	counter atomic.Int64
	mu      sync.Mutex
	events  []agentEvent
	logs    []string
}

// setup makes an engine on an in-memory database with a fixed clock and ids id-0001, id-0002, ...
func setup(t *testing.T) *testEnv {
	t.Helper()
	return setupAt(t, store.Memory, startTime, Options{})
}

func setupAt(t *testing.T, path string, start int64, options Options) *testEnv {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	h := &testEnv{t: t, db: db}
	h.clock.Store(start)
	options.Now = func() int64 { return h.clock.Load() }
	options.NewID = func(int64) string { return fmt.Sprintf("id-%04d", h.counter.Add(1)) }
	options.Log = func(line string) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.logs = append(h.logs, line)
	}
	h.engine, err = New(ctx, db, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = h.engine.Close()
		_ = db.Close()
	})
	h.engine.OnLink(func(agent string, event LinkEvent) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.events = append(h.events, agentEvent{agent, event})
	})
	return h
}

func (h *testEnv) advance(ms int64) { h.clock.Add(ms) }

func (h *testEnv) linkEvents() []agentEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]agentEvent(nil), h.events...)
}

func (h *testEnv) eventsOf(eventType string) []agentEvent {
	var out []agentEvent
	for _, e := range h.linkEvents() {
		if e.event.Type == eventType {
			out = append(out, e)
		}
	}
	return out
}

// register registers an agent for a principal, mars by default. extra holds key and value pairs of the body.
func (h *testEnv) register(agent string, extra ...any) RegisterResult {
	h.t.Helper()
	return got(h.registerAs(mars, agent, extra...)).ok(h.t)
}

func (h *testEnv) registerAs(principal auth.Principal, agent string, extra ...any) (RegisterResult, error) {
	body := jsonx.NewObject("agent", agent, "protocol", float64(HubProtocol))
	for i := 0; i+1 < len(extra); i += 2 {
		body.Set(extra[i].(string), value(extra[i+1]))
	}
	return h.engine.Register(principal, body)
}

func (h *testEnv) send(agentID string, caller Caller, msg *jsonx.Object) *SendResult {
	h.t.Helper()
	return got(h.engine.Send(SendInput{AgentID: agentID, Caller: caller, Message: msg})).ok(h.t)
}

func (h *testEnv) taskID(agentID string, caller Caller, msg *jsonx.Object) string {
	h.t.Helper()
	return h.send(agentID, caller, msg).Task.ID
}

func (h *testEnv) linkUpdate(agentID, taskID string, pairs ...any) *Task {
	h.t.Helper()
	return got(h.engine.LinkUpdate(agentID, taskID, obj(pairs...))).ok(h.t)
}

type result[T any] struct {
	v   T
	err error
}

// got holds the results of a call: got(f()).ok(t) gives the value and fails the test on an error.
func got[T any](v T, err error) result[T] { return result[T]{v, err} }

func (r result[T]) ok(t *testing.T) T {
	t.Helper()
	if r.err != nil {
		t.Fatalf("unexpected error: %v", r.err)
	}
	return r.v
}

// reason gives the reason of the hub error of the call, "" for none.
func (r result[T]) reason(t *testing.T) Reason {
	t.Helper()
	return reason(t, r.err)
}

func ok(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// value turns test literals into parsed JSON values: ints become float64, maps are not allowed.
func value(v any) any {
	switch t := v.(type) {
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case []string:
		out := make([]any, len(t))
		for i, s := range t {
			out[i] = s
		}
		return out
	}
	return v
}

// obj makes an object from key and value pairs of test literals.
func obj(pairs ...any) *jsonx.Object {
	o := &jsonx.Object{}
	for i := 0; i+1 < len(pairs); i += 2 {
		o.Set(pairs[i].(string), value(pairs[i+1]))
	}
	return o
}

// message is a user message with id m-<text>. extra holds more key and value pairs.
func message(text string, extra ...any) *jsonx.Object {
	msg := jsonx.NewObject("messageId", "m-"+text, "role", "ROLE_USER", "parts", []any{jsonx.NewObject("text", text)})
	for i := 0; i+1 < len(extra); i += 2 {
		msg.Set(extra[i].(string), value(extra[i+1]))
	}
	return msg
}

// withMeta sets the bus extension metadata of a message.
func withMeta(msg *jsonx.Object, pairs ...any) *jsonx.Object {
	return msg.Set("metadata", jsonx.NewObject(BusExtensionURI, obj(pairs...)))
}

// reason gives the reason of a hub error, "" for nil. Another error fails the test.
func reason(t *testing.T, err error) Reason {
	t.Helper()
	if err == nil {
		return ""
	}
	var hubErr *Error
	if !errors.As(err, &hubErr) {
		t.Fatalf("not a hub error: %v", err)
	}
	return hubErr.Reason
}

// toValue turns a Go value into a parsed JSON value through its JSON form.
func toValue(t *testing.T, v any) any {
	t.Helper()
	if raw, ok := v.(interface{ MarshalJSON() ([]byte, error) }); ok {
		if _, isObject := v.(*jsonx.Object); !isObject {
			data, err := raw.MarshalJSON()
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := jsonx.Parse(data)
			if err != nil {
				t.Fatal(err)
			}
			return parsed
		}
	}
	parsed, err := jsonx.ToValue(v)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

// jsonOf is the JSON text of v as JSON.stringify writes it.
func jsonOf(t *testing.T, v any) string {
	t.Helper()
	return string(jsonx.Stringify(toValue(t, v)))
}

// equalJSON checks that got has exactly the JSON form of want, key order included.
func equalJSON(t *testing.T, label string, got any, want any) {
	t.Helper()
	if g, w := jsonOf(t, got), jsonOf(t, want); g != w {
		t.Errorf("%s\n got %s\nwant %s", label, g, w)
	}
}

// anyString matches any string in matchJSON and equalish checks.
const anyString = "\x00any string"

// matchJSON checks that got holds want like toMatchObject: objects need the wanted keys only, arrays match by
// length and item.
func matchJSON(t *testing.T, label string, got any, want any) {
	t.Helper()
	g, w := toValue(t, got), toValue(t, want)
	if path, ok := subset(g, w, "$"); !ok {
		t.Errorf("%s: mismatch at %s\n got %s\nwant %s", label, path, jsonx.Stringify(g), jsonx.Stringify(w))
	}
}

func subset(got, want any, path string) (string, bool) {
	switch w := want.(type) {
	case *jsonx.Object:
		g, ok := got.(*jsonx.Object)
		if !ok {
			return path, false
		}
		for _, key := range w.Keys() {
			gv, present := g.Get(key)
			if !present {
				return path + "." + key, false
			}
			if p, ok := subset(gv, w.Value(key), path+"."+key); !ok {
				return p, false
			}
		}
		return "", true
	case []any:
		g, ok := got.([]any)
		if !ok || len(g) != len(w) {
			return path, false
		}
		for i := range w {
			if p, ok := subset(g[i], w[i], fmt.Sprintf("%s[%d]", path, i)); !ok {
				return p, false
			}
		}
		return "", true
	case string:
		if w == anyString {
			_, ok := got.(string)
			return path, ok
		}
	}
	return path, reflect.DeepEqual(got, want) || jsonx.IsNull(got) && jsonx.IsNull(want)
}

// parse reads a JSON object literal.
func parse(t *testing.T, text string) *jsonx.Object {
	t.Helper()
	v, err := jsonx.ParseString(text)
	if err != nil {
		t.Fatalf("%v: %s", err, text)
	}
	return v.(*jsonx.Object)
}

// payload parses the payload of a link event.
func payload(t *testing.T, event LinkEvent) *jsonx.Object {
	t.Helper()
	v, err := jsonx.Parse(event.Payload)
	if err != nil {
		t.Fatal(err)
	}
	return v.(*jsonx.Object)
}
