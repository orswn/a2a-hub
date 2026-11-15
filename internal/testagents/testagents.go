// Package testagents holds the built-in agents of test mode: tck for the A2A TCK and echo.
package testagents

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/orswn/a2a-hub/internal/a2aapi"
	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/jsonx"
)

type stepKind int

const (
	working stepKind = iota
	input
	done
	reject
	wait
	artifact
)

type step struct {
	do        stepKind
	text      string
	ms        int
	part      func() *jsonx.Object
	append    *bool
	lastChunk *bool
}

// StreamTimeout is the stream timeout of the TCK. The resubscribe scenario waits twice as long.
const StreamTimeout = 2000 * time.Millisecond

func textPart(value string) func() *jsonx.Object {
	return func() *jsonx.Object { return jsonx.NewObject("text", value) }
}

func filePart() *jsonx.Object {
	return jsonx.NewObject("raw", base64.StdEncoding.EncodeToString([]byte("tck")), "filename", "output.txt", "mediaType", "text/plain")
}

func fileURLPart() *jsonx.Object {
	return jsonx.NewObject("url", "https://example.com/output.txt", "filename", "output.txt", "mediaType", "text/plain")
}

func dataPart() *jsonx.Object {
	return jsonx.NewObject("data", jsonx.NewObject("key", "value", "count", float64(42)))
}

func lifecycle(part func() *jsonx.Object) []step {
	return []step{{do: working}, {do: artifact, part: part}, {do: done}}
}

func flag(v bool) *bool { return &v }

// tckTable is the a2a-tck scenario table (scenarios/*.feature), by messageId prefix. Longer prefixes come first.
var tckTable = []struct {
	prefix string
	steps  []step
}{
	{"tck-stream-artifact-chunked", []step{
		{do: working},
		{do: artifact, part: textPart("chunk-1 "), append: flag(true), lastChunk: flag(false)},
		{do: artifact, part: textPart("chunk-2"), append: flag(true), lastChunk: flag(true)},
		{do: done},
	}},
	{"tck-stream-artifact-text", lifecycle(textPart("Streamed text content"))},
	{"tck-stream-artifact-file", lifecycle(filePart)},
	{"test-resubscribe-message-id", []step{{do: working}, {do: wait, ms: int(2 * StreamTimeout / time.Millisecond)}, {do: done}}},
	{"tck-stream-ordering-001", lifecycle(textPart("Ordered output"))},
	{"tck-artifact-file-url", []step{{do: artifact, part: fileURLPart}, {do: done}}},
	{"tck-input-required", []step{{do: input}}},
	{"tck-complete-task", []step{{do: done, text: "Hello from TCK"}}},
	{"tck-artifact-text", []step{{do: artifact, part: textPart("Generated text content")}, {do: done}}},
	{"tck-artifact-file", []step{{do: artifact, part: filePart}, {do: done}}},
	{"tck-artifact-data", []step{{do: artifact, part: dataPart}, {do: done}}},
	{"tck-reject-task", []step{{do: reject}}},
	{"tck-stream-001", lifecycle(textPart("Stream hello from TCK"))},
	{"tck-stream-002", []step{{do: done}}},
	{"tck-stream-003", lifecycle(textPart("Stream task lifecycle"))},
}

func tckSteps(messageID string) []step {
	for _, row := range tckTable {
		if strings.HasPrefix(messageID, row.prefix) {
			return row.steps
		}
	}
	return []step{{do: done, text: "Unhandled messageId prefix: " + messageID}}
}

func hasReason(err error, reasons ...engine.Reason) bool {
	var hubErr *engine.Error
	return errors.As(err, &hubErr) && slices.Contains(reasons, hubErr.Reason)
}

func (a *Agents) runSteps(e *engine.Engine, taskID string, steps []step) {
	artifactID := e.NewID()
	for _, s := range steps {
		// The send call returns before the first change, like the next macrotask in the TypeScript hub.
		runtime.Gosched()
		if a.ctx.Err() != nil {
			return
		}
		var change engine.TaskChange
		switch s.do {
		case wait:
			select {
			case <-time.After(time.Duration(s.ms) * time.Millisecond):
			case <-a.ctx.Done():
				return
			}
			continue
		case working:
			change = engine.TaskChange{State: engine.StateWorking}
		case input:
			change = engine.TaskChange{State: engine.StateInputRequired, Message: e.AgentMessage("need input")}
		case done:
			change = engine.TaskChange{State: engine.StateCompleted}
			if s.text != "" {
				change.Message = e.AgentMessage(s.text)
			}
		case reject:
			change = engine.TaskChange{State: engine.StateRejected, Message: e.AgentMessage("rejected")}
		case artifact:
			item := engine.ArtifactChange{
				Artifact:  jsonx.NewObject("artifactId", artifactID, "name", "tck", "parts", []any{s.part()}),
				LastChunk: s.lastChunk,
			}
			if s.append != nil {
				item.Append = *s.append
			}
			change = engine.TaskChange{Artifacts: []engine.ArtifactChange{item}}
		}
		if _, err := e.ApplyChange(taskID, change); err != nil {
			// A canceled task ends the scenario.
			if !hasReason(err, engine.ReasonTaskTerminal, engine.ReasonTaskNotFound) {
				a.report(err)
			}
			return
		}
	}
}

func (a *Agents) report(err error) { a.log("test agent failed: " + err.Error()) }

// Agents holds the built-in agents of test mode.
type Agents struct {
	log    func(line string)
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	wg     sync.WaitGroup
	closed bool
}

// goRun runs a scenario that Stop can end and wait for. It does nothing after Stop.
func (a *Agents) goRun(run func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		run()
	}()
}

// Stop ends the open scenarios and waits for them, so none calls the engine after it closes.
func (a *Agents) Stop() {
	a.mu.Lock()
	a.closed = true
	a.mu.Unlock()
	a.cancel()
	a.wg.Wait()
}

type tck struct{ *Agents }

func (tck) ID() string   { return "tck" }
func (tck) Name() string { return "tck" }
func (tck) Description() string {
	return "Built-in A2A TCK scenario agent. It acts on the messageId prefix."
}

func (tck) Reply(message *jsonx.Object, contextID string) *jsonx.Object {
	id, _ := message.String("messageId")
	if !strings.HasPrefix(id, "tck-message-response") {
		return nil
	}
	return jsonx.NewObject("messageId", randomUUID(), "contextId", contextID, "role", "ROLE_AGENT",
		"parts", []any{jsonx.NewObject("text", "Direct message response")})
}

func (t tck) Start(e *engine.Engine, taskID string, message *jsonx.Object) {
	id, _ := message.String("messageId")
	t.goRun(func() { t.runSteps(e, taskID, tckSteps(id)) })
}

type echo struct{ *Agents }

func (echo) ID() string   { return "echo" }
func (echo) Name() string { return "echo" }
func (echo) Description() string {
	return "Built-in echo agent. It completes each task at once with an artifact that echoes the text."
}

func (echo) Reply(*jsonx.Object, string) *jsonx.Object { return nil }

func (a echo) Start(e *engine.Engine, taskID string, message *jsonx.Object) {
	a.goRun(func() {
		runtime.Gosched()
		body := engine.MessageText(message)
		_, err := e.ApplyChange(taskID, engine.TaskChange{
			State:   engine.StateCompleted,
			Message: e.AgentMessage(body),
			Artifacts: []engine.ArtifactChange{{Artifact: jsonx.NewObject(
				"artifactId", e.NewID(), "name", "echo", "parts", []any{jsonx.NewObject("text", body)})}},
		})
		if err != nil && !hasReason(err, engine.ReasonTaskTerminal) {
			a.report(err)
		}
	})
}

// Install makes the built-in agents known to the engine and returns them by id. log gets the failures. Stop the
// returned Agents before the engine closes.
func Install(e *engine.Engine, log func(line string)) (map[string]a2aapi.Builtin, *Agents, error) {
	agents := &Agents{log: log}
	agents.ctx, agents.cancel = context.WithCancel(context.Background())
	out := map[string]a2aapi.Builtin{}
	for _, agent := range []a2aapi.Builtin{tck{agents}, echo{agents}} {
		if err := e.EnsureBuiltin(agent.ID(), agent.Name()); err != nil {
			agents.cancel()
			return nil, nil, err
		}
		out[agent.ID()] = agent
	}
	return out, agents, nil
}

func randomUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
