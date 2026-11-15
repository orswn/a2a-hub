// Package a2aapi answers the A2A methods of one hub agent on top of the engine.
package a2aapi

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"
	"sync"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/jsonx"
)

const (
	// BlockingHold is the longest time a blocking SendMessage waits for its task.
	BlockingHold = time.Hour
	// HoldLimit is the number of open blocking requests and streams per principal.
	HoldLimit = 64
	// DeliveryWait is how long a send waits for the link ack of the target.
	DeliveryWait = 2 * time.Second
	// DefaultPageSize and MaxPageSize bound ListTasks.
	DefaultPageSize = 50
	MaxPageSize     = 100
)

// The delivery states that a send reports in its bus metadata.
const (
	Delivered   = "delivered"
	Queued      = "queued"
	Unconfirmed = "unconfirmed"
	Failed      = "failed"
)

var busExtensions = func() []*a2a.AgentExtension {
	out := make([]*a2a.AgentExtension, len(engine.BusExtensionURIs))
	for i, uri := range engine.BusExtensionURIs {
		out[i] = &a2a.AgentExtension{URI: uri}
	}
	return out
}()

// ToA2AError maps a hub error to an A2A error. Other errors pass unchanged.
func ToA2AError(err error) error {
	var hubErr *engine.Error
	if !errors.As(err, &hubErr) {
		return err
	}
	switch hubErr.Reason {
	case engine.ReasonTaskNotFound:
		return a2a.NewError(a2a.ErrTaskNotFound, hubErr.Message)
	case engine.ReasonTaskTerminal:
		return a2a.NewError(a2a.ErrUnsupportedOperation, hubErr.Message)
	case engine.ReasonNotCancelable:
		return a2a.NewError(a2a.ErrTaskNotCancelable, hubErr.Message)
	case engine.ReasonInvalid:
		return malformed("%s", hubErr.Message)
	}
	return a2a.NewError(a2a.ErrInternalError, hubErr.Message)
}

func malformed(format string, args ...any) error {
	return a2a.NewError(a2a.ErrInvalidParams, fmt.Sprintf(format, args...))
}

func requireID(id, field string) error {
	if strings.TrimSpace(id) == "" {
		return malformed("%s is required", field)
	}
	return nil
}

// EndsWait reports a state that ends a blocking request and a stream: terminal or interrupted.
func EndsWait(state engine.State) bool {
	return engine.IsTerminal(state) || engine.IsInterrupted(state)
}

func checkHistoryLength(value *int) error {
	if value != nil && *value < 0 {
		return malformed("historyLength must not be negative")
	}
	return nil
}

// HoldLimiter counts the open blocking requests and streams of each principal.
type HoldLimiter struct {
	limit int
	mu    sync.Mutex
	open  map[string]int
}

// NewHoldLimiter makes a limiter. A limit of zero or less means HoldLimit.
func NewHoldLimiter(limit int) *HoldLimiter {
	if limit <= 0 {
		limit = HoldLimit
	}
	return &HoldLimiter{limit: limit, open: map[string]int{}}
}

// Count is the number of open holds of a principal.
func (l *HoldLimiter) Count(principal string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.open[principal]
}

// Acquire takes a hold or refuses above the limit. The returned function frees the hold once.
func (l *HoldLimiter) Acquire(principal string) (func(), error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.open[principal] >= l.limit {
		return nil, a2a.NewError(a2a.ErrInternalError,
			fmt.Sprintf("too many open streams and blocking requests for %s: the limit is %d", principal, l.limit))
	}
	l.open[principal]++
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			if l.open[principal] <= 1 {
				delete(l.open, principal)
			} else {
				l.open[principal]--
			}
		})
	}, nil
}

// Handler answers the A2A methods for one agent and one caller. The request context ends the waits of
// blocking requests and streams.
type Handler struct {
	Engine  *engine.Engine
	Agent   *engine.AgentRow
	Caller  engine.Caller
	Builtin Builtin
	Holds   *HoldLimiter
	Log     func(line string)
}

var _ a2asrv.RequestHandler = (*Handler)(nil)

// publicError keeps A2A and hub errors. Any other error may hold internal text: it is logged, and the client
// gets a generic one.
func (h *Handler) publicError(err error) error {
	var a2aErr *a2a.Error
	if errors.As(err, &a2aErr) {
		return a2aErr
	}
	var hubErr *engine.Error
	if errors.As(err, &hubErr) {
		return ToA2AError(hubErr)
	}
	if h.Log != nil {
		h.Log("a2a request failed: " + err.Error())
	}
	return a2a.NewError(a2a.ErrInternalError, "internal error")
}

func (h *Handler) hold() (func(), error) { return h.Holds.Acquire(h.Caller.Principal.Name) }

// activate activates each bus extension URI that the request names in A2A-Extensions.
func activate(ctx context.Context) {
	extensions, ok := a2asrv.ExtensionsFrom(ctx)
	if !ok {
		return
	}
	for _, extension := range busExtensions {
		if extensions.Requested(extension) {
			extensions.Activate(extension)
		}
	}
}

// busURI is the bus extension URI of the answer: the one that the message metadata uses, else the one of the
// request metadata.
func busURI(req *a2a.SendMessageRequest) string {
	if req.Message != nil {
		if uri := busKey(req.Message.Metadata); uri != "" {
			return uri
		}
	}
	if uri := busKey(req.Metadata); uri != "" {
		return uri
	}
	return engine.BusExtensionURI
}

func busKey(metadata map[string]any) string {
	for _, uri := range engine.BusExtensionURIs {
		if _, ok := metadata[uri]; ok {
			return uri
		}
	}
	return ""
}

type sent struct {
	result  *engine.SendResult
	direct  *jsonx.Object
	message *jsonx.Object
}

func (h *Handler) send(ctx context.Context, req *a2a.SendMessageRequest, watch func(engine.TaskEvent)) (*sent, error) {
	if req.Message == nil || req.Message.ID == "" {
		return nil, malformed("message.messageId is required.")
	}
	wire, err := messageToWire(req.Message, rawMessage(ctx))
	if err != nil {
		return nil, err
	}
	if h.Builtin != nil && req.Message.TaskID == "" {
		contextID := req.Message.ContextID
		if contextID == "" {
			contextID = h.Engine.NewID()
		}
		if direct := h.Builtin.Reply(wire, contextID); direct != nil {
			return &sent{direct: direct}, nil
		}
	}
	input := engine.SendInput{AgentID: h.Agent.ID, Caller: h.Caller, Message: wire, Watch: watch}
	if req.Config != nil && req.Config.PushConfig != nil {
		push := req.Config.PushConfig
		input.PushConfig = &engine.PushConfigInput{ID: push.ID, URL: push.URL, Token: push.Token}
		if push.Auth != nil {
			input.PushConfig.Authentication = &engine.Authentication{Scheme: push.Auth.Scheme, Credentials: push.Auth.Credentials}
		}
	}
	result, err := h.Engine.Send(input)
	if err != nil {
		return nil, err
	}
	return &sent{result: result, message: wire}, nil
}

func (h *Handler) startBuiltin(s *sent) {
	if h.Builtin != nil && s.result.Task != nil {
		h.Builtin.Start(h.Engine, s.result.Task.ID, s.message)
	}
}

func (h *Handler) delivery(ctx context.Context, result *engine.SendResult) string {
	if h.Agent.Kind != engine.KindSession {
		return Delivered
	}
	agent, err := h.Engine.Agent(h.Agent.ID)
	if err != nil || agent == nil || agent.Online != 1 || result.EventSeq == nil {
		return Queued
	}
	if h.Engine.WaitForAck(ctx, agent.ID, *result.EventSeq, DeliveryWait) {
		return Delivered
	}
	return Unconfirmed
}

func requestsDeliveryWait(metadata map[string]any) bool {
	meta, ok := metadata[busKey(metadata)].(map[string]any)
	return ok && meta["deliveryWait"] == true
}

func stamp(metadata *jsonx.Object, uri, delivery string) *jsonx.Object {
	out := metadata.Clone()
	if out == nil {
		out = jsonx.NewObject()
	}
	out.Set(uri, jsonx.NewObject("delivery", delivery))
	return out
}

func (h *Handler) readTask(id string, historyLength *int) (*engine.Task, error) {
	return h.Engine.GetTask(h.Agent.ID, h.Caller, id, historyLength)
}

func taskFromWire(task *engine.Task) (*a2a.Task, error) { return fromWire[a2a.Task](task) }

// SendMessage implements a2asrv.RequestHandler.
func (h *Handler) SendMessage(ctx context.Context, req *a2a.SendMessageRequest) (a2a.SendMessageResult, error) {
	result, err := h.sendMessage(ctx, req)
	if err != nil {
		return nil, h.publicError(err)
	}
	return result, nil
}

func (h *Handler) sendMessage(ctx context.Context, req *a2a.SendMessageRequest) (a2a.SendMessageResult, error) {
	activate(ctx)
	var historyLength *int
	blocking := true
	if req.Config != nil {
		historyLength = req.Config.HistoryLength
		blocking = !req.Config.ReturnImmediately
	}
	if err := checkHistoryLength(historyLength); err != nil {
		return nil, err
	}
	if blocking {
		release, err := h.hold()
		if err != nil {
			return nil, err
		}
		defer release()
	}
	s, err := h.send(ctx, req, nil)
	if err != nil {
		return nil, err
	}
	if s.direct != nil {
		return fromWire[a2a.Message](s.direct)
	}
	result := s.result
	deliveryWait := result.Meta.DeliveryWait || requestsDeliveryWait(req.Metadata)
	delivered := make(chan string, 1)
	go func() {
		if deliveryWait {
			delivered <- h.delivery(ctx, result)
		} else {
			delivered <- ""
		}
	}()
	h.startBuiltin(s)
	if result.Task != nil && blocking {
		h.Engine.WaitForTask(ctx, result.Task.ID, EndsWait, BlockingHold)
	}
	delivery := <-delivered
	if result.Task == nil {
		reply := result.Message
		if delivery != "" {
			reply = reply.Clone()
			reply.Set("metadata", stamp(objectOf(reply.Value("metadata")), busURI(req), delivery))
		}
		return fromWire[a2a.Message](reply)
	}
	task, err := h.readTask(result.Task.ID, historyLength)
	if err != nil {
		return nil, err
	}
	if delivery != "" {
		copied := *task
		copied.Metadata = stamp(task.Metadata, busURI(req), delivery)
		task = &copied
	}
	return taskFromWire(task)
}

func objectOf(v any) *jsonx.Object {
	obj, _ := v.(*jsonx.Object)
	return obj
}

// eventQueue collects the task events of a stream. push never blocks and is safe after close, because a
// removed engine listener may still be called once.
type eventQueue struct {
	mu     sync.Mutex
	items  []engine.TaskEvent
	closed bool
	wake   chan struct{}
}

func newEventQueue() *eventQueue { return &eventQueue{wake: make(chan struct{}, 1)} }

func (q *eventQueue) push(event engine.TaskEvent) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.items = append(q.items, event)
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *eventQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.items = nil
	q.mu.Unlock()
}

// next waits for the next event. false means ctx is done.
func (q *eventQueue) next(ctx context.Context) (engine.TaskEvent, bool) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			event := q.items[0]
			q.items = q.items[1:]
			q.mu.Unlock()
			return event, true
		}
		q.mu.Unlock()
		select {
		case <-q.wake:
		case <-ctx.Done():
			return engine.TaskEvent{}, false
		}
	}
}

func eventFromWire(response engine.StreamResponse) (a2a.Event, error) {
	switch {
	case response.Task != nil:
		return taskFromWire(response.Task)
	case response.Message != nil:
		return fromWire[a2a.Message](response.Message)
	case response.StatusUpdate != nil:
		return fromWire[a2a.TaskStatusUpdateEvent](response.StatusUpdate)
	case response.ArtifactUpdate != nil:
		return fromWire[a2a.TaskArtifactUpdateEvent](response.ArtifactUpdate)
	}
	return nil, errors.New("empty stream response")
}

// drain yields the queued events until a status update ends the wait, the consumer stops, or ctx is done.
func drain(ctx context.Context, queue *eventQueue, yield func(a2a.Event, error) bool) error {
	for {
		event, ok := queue.next(ctx)
		if !ok {
			return nil
		}
		out, err := eventFromWire(event.Response)
		if err != nil {
			return err
		}
		if !yield(out, nil) {
			return nil
		}
		if event.Response.StatusUpdate != nil && EndsWait(event.State) {
			return nil
		}
	}
}

func (h *Handler) stream(run func(yield func(a2a.Event, error) bool) error) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		stopped := false
		guarded := func(event a2a.Event, err error) bool {
			if stopped {
				return false
			}
			stopped = !yield(event, err)
			return !stopped
		}
		if err := run(guarded); err != nil && !stopped {
			yield(nil, h.publicError(err))
		}
	}
}

// SendStreamingMessage implements a2asrv.RequestHandler.
func (h *Handler) SendStreamingMessage(ctx context.Context, req *a2a.SendMessageRequest) iter.Seq2[a2a.Event, error] {
	return h.stream(func(yield func(a2a.Event, error) bool) error {
		activate(ctx)
		var historyLength *int
		if req.Config != nil {
			historyLength = req.Config.HistoryLength
		}
		if err := checkHistoryLength(historyLength); err != nil {
			return err
		}
		release, err := h.hold()
		if err != nil {
			return err
		}
		defer release()
		queue := newEventQueue()
		defer queue.close()
		s, err := h.send(ctx, req, queue.push)
		if err != nil {
			return err
		}
		if s.direct != nil {
			return yieldWire[a2a.Message](s.direct, yield)
		}
		defer s.result.Unwatch()
		if s.result.Task == nil {
			return yieldWire[a2a.Message](s.result.Message, yield)
		}
		h.startBuiltin(s)
		task, err := taskFromWire(engine.ApplyHistoryLength(s.result.Task, historyLength))
		if err != nil {
			return err
		}
		if !yield(task, nil) || EndsWait(s.result.Task.Status.State) {
			return nil
		}
		return drain(ctx, queue, yield)
	})
}

func yieldWire[T any, P interface {
	*T
	a2a.Event
}](v any, yield func(a2a.Event, error) bool) error {
	out, err := fromWire[T](v)
	if err != nil {
		return err
	}
	yield(P(out), nil)
	return nil
}

// SubscribeToTask implements a2asrv.RequestHandler.
func (h *Handler) SubscribeToTask(ctx context.Context, req *a2a.SubscribeToTaskRequest) iter.Seq2[a2a.Event, error] {
	return h.stream(func(yield func(a2a.Event, error) bool) error {
		id := string(req.ID)
		if err := requireID(id, "id"); err != nil {
			return err
		}
		release, err := h.hold()
		if err != nil {
			return err
		}
		defer release()
		queue := newEventQueue()
		defer queue.close()
		task, unwatch, err := h.Engine.WatchTask(h.Agent.ID, h.Caller, id, nil, queue.push)
		if err != nil {
			return err
		}
		defer unwatch()
		if engine.IsTerminal(task.Status.State) {
			return a2a.NewError(a2a.ErrUnsupportedOperation,
				fmt.Sprintf("Task %s is in a terminal state and cannot be subscribed to.", id))
		}
		out, err := taskFromWire(task)
		if err != nil {
			return err
		}
		if !yield(out, nil) {
			return nil
		}
		return drain(ctx, queue, yield)
	})
}

// GetTask implements a2asrv.RequestHandler.
func (h *Handler) GetTask(_ context.Context, req *a2a.GetTaskRequest) (*a2a.Task, error) {
	task, err := h.getTask(req)
	if err != nil {
		return nil, h.publicError(err)
	}
	return task, nil
}

func (h *Handler) getTask(req *a2a.GetTaskRequest) (*a2a.Task, error) {
	if err := checkHistoryLength(req.HistoryLength); err != nil {
		return nil, err
	}
	if err := requireID(string(req.ID), "id"); err != nil {
		return nil, err
	}
	task, err := h.readTask(string(req.ID), req.HistoryLength)
	if err != nil {
		return nil, err
	}
	return taskFromWire(task)
}

// ListTasks implements a2asrv.RequestHandler. A page size of zero means the default.
func (h *Handler) ListTasks(_ context.Context, req *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	out, err := h.listTasks(req)
	if err != nil {
		return nil, h.publicError(err)
	}
	return out, nil
}

func (h *Handler) listTasks(req *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	if err := checkHistoryLength(req.HistoryLength); err != nil {
		return nil, err
	}
	pageSize := req.PageSize
	if pageSize == 0 {
		pageSize = DefaultPageSize
	}
	if pageSize < 1 || pageSize > MaxPageSize {
		return nil, malformed("pageSize must be between 1 and %d", MaxPageSize)
	}
	input := engine.ListTasksInput{
		ContextID:        req.ContextID,
		PageSize:         pageSize,
		PageToken:        req.PageToken,
		HistoryLength:    req.HistoryLength,
		IncludeArtifacts: req.IncludeArtifacts,
	}
	if req.Status != "" && req.Status != a2a.TaskStateUnspecified {
		input.State = engine.State(req.Status)
	}
	if req.StatusTimestampAfter != nil {
		input.StatusTimestampAfter = engine.ISO(req.StatusTimestampAfter.UnixMilli())
	}
	result, err := h.Engine.ListTasks(h.Agent.ID, h.Caller, input)
	if err != nil {
		return nil, err
	}
	out := &a2a.ListTasksResponse{
		Tasks:         make([]*a2a.Task, 0, len(result.Tasks)),
		TotalSize:     int(result.TotalSize),
		PageSize:      result.PageSize,
		NextPageToken: result.NextPageToken,
	}
	for _, task := range result.Tasks {
		converted, err := taskFromWire(task)
		if err != nil {
			return nil, err
		}
		out.Tasks = append(out.Tasks, converted)
	}
	return out, nil
}

// CancelTask implements a2asrv.RequestHandler.
func (h *Handler) CancelTask(_ context.Context, req *a2a.CancelTaskRequest) (*a2a.Task, error) {
	task, err := h.cancelTask(req)
	if err != nil {
		return nil, h.publicError(err)
	}
	return task, nil
}

func (h *Handler) cancelTask(req *a2a.CancelTaskRequest) (*a2a.Task, error) {
	if err := requireID(string(req.ID), "id"); err != nil {
		return nil, err
	}
	task, err := h.Engine.Cancel(h.Agent.ID, h.Caller, string(req.ID))
	if err != nil {
		return nil, err
	}
	return taskFromWire(task)
}

func pushConfigFromWire(config *engine.PushConfig) *a2a.PushConfig {
	out := &a2a.PushConfig{ID: config.ID, TaskID: a2a.TaskID(config.TaskID), URL: config.URL, Token: config.Token}
	if config.Authentication != nil {
		out.Auth = &a2a.PushAuthInfo{Scheme: config.Authentication.Scheme, Credentials: config.Authentication.Credentials}
	}
	return out
}

// CreateTaskPushConfig implements a2asrv.RequestHandler.
func (h *Handler) CreateTaskPushConfig(_ context.Context, req *a2a.PushConfig) (*a2a.PushConfig, error) {
	out, err := h.createPushConfig(req)
	if err != nil {
		return nil, h.publicError(err)
	}
	return out, nil
}

func (h *Handler) createPushConfig(req *a2a.PushConfig) (*a2a.PushConfig, error) {
	if err := requireID(string(req.TaskID), "taskId"); err != nil {
		return nil, err
	}
	if req.URL == "" {
		return nil, malformed("url is required")
	}
	input := engine.PushConfigInput{ID: req.ID, TaskID: string(req.TaskID), URL: req.URL, Token: req.Token}
	if req.Auth != nil {
		input.Authentication = &engine.Authentication{Scheme: req.Auth.Scheme, Credentials: req.Auth.Credentials}
	}
	saved, err := h.Engine.CreatePushConfig(h.Agent.ID, h.Caller, input)
	if err != nil {
		return nil, err
	}
	return pushConfigFromWire(saved), nil
}

// GetTaskPushConfig implements a2asrv.RequestHandler.
func (h *Handler) GetTaskPushConfig(_ context.Context, req *a2a.GetTaskPushConfigRequest) (*a2a.PushConfig, error) {
	if err := requireID(string(req.TaskID), "taskId"); err != nil {
		return nil, h.publicError(err)
	}
	config, err := h.Engine.GetPushConfig(h.Agent.ID, h.Caller, string(req.TaskID), req.ID)
	if err != nil {
		return nil, h.publicError(err)
	}
	return pushConfigFromWire(config), nil
}

// ListTaskPushConfigs implements a2asrv.RequestHandler.
func (h *Handler) ListTaskPushConfigs(_ context.Context, req *a2a.ListTaskPushConfigRequest) (*a2a.ListTaskPushConfigResponse, error) {
	if err := requireID(string(req.TaskID), "taskId"); err != nil {
		return nil, h.publicError(err)
	}
	configs, err := h.Engine.ListPushConfigs(h.Agent.ID, h.Caller, string(req.TaskID))
	if err != nil {
		return nil, h.publicError(err)
	}
	out := &a2a.ListTaskPushConfigResponse{Configs: make([]*a2a.PushConfig, 0, len(configs))}
	for _, config := range configs {
		out.Configs = append(out.Configs, pushConfigFromWire(config))
	}
	return out, nil
}

// DeleteTaskPushConfig implements a2asrv.RequestHandler.
func (h *Handler) DeleteTaskPushConfig(_ context.Context, req *a2a.DeleteTaskPushConfigRequest) error {
	if err := requireID(string(req.TaskID), "taskId"); err != nil {
		return h.publicError(err)
	}
	if err := h.Engine.DeletePushConfig(h.Agent.ID, h.Caller, string(req.TaskID), req.ID); err != nil {
		return h.publicError(err)
	}
	return nil
}

// GetExtendedAgentCard implements a2asrv.RequestHandler.
func (h *Handler) GetExtendedAgentCard(context.Context, *a2a.GetExtendedAgentCardRequest) (*a2a.AgentCard, error) {
	return nil, a2a.NewError(a2a.ErrUnsupportedOperation, "Agent does not support an extended agent card.")
}
