package server

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/orswn/a2a-hub/internal/a2aapi"
	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/jsonx"
)

const (
	codeParseError      = -32700
	codeMethodNotFound  = -32601
	codeAmbiguousAlias  = -32602
	errorInfoType       = "type.googleapis.com/google.rpc.ErrorInfo"
	a2aErrorDomain      = "a2a-protocol.org"
	supportedA2AVersion = "1.0"
)

var jsonTypes = map[string]bool{"application/json": true, "application/a2a+json": true}

// The JSON-RPC code and ErrorInfo reason of each A2A error, as @a2a-js/sdk writes them.
var rpcCodes = []struct {
	err    error
	code   int
	reason string
}{
	{a2a.ErrTaskNotFound, -32001, "TASK_NOT_FOUND"},
	{a2a.ErrTaskNotCancelable, -32002, "TASK_NOT_CANCELABLE"},
	{a2a.ErrPushNotificationNotSupported, -32003, "PUSH_NOTIFICATION_NOT_SUPPORTED"},
	{a2a.ErrUnsupportedOperation, -32004, "UNSUPPORTED_OPERATION"},
	{a2a.ErrUnsupportedContentType, -32005, "CONTENT_TYPE_NOT_SUPPORTED"},
	{a2a.ErrInvalidAgentResponse, -32006, "INVALID_AGENT_RESPONSE"},
	{a2a.ErrExtendedCardNotConfigured, -32007, "EXTENDED_AGENT_CARD_NOT_CONFIGURED"},
	{a2a.ErrExtensionSupportRequired, -32008, "EXTENSION_SUPPORT_REQUIRED"},
	{a2a.ErrVersionNotSupported, -32009, "VERSION_NOT_SUPPORTED"},
	{a2a.ErrInvalidParams, -32602, "INVALID_PARAMS"},
	{a2a.ErrInternalError, -32603, "INTERNAL_ERROR"},
}

func errorInfo(reason, domain string, metadata *jsonx.Object) *jsonx.Object {
	info := jsonx.NewObject("@type", errorInfoType, "reason", reason, "domain", domain)
	if metadata != nil {
		info.Set("metadata", metadata)
	}
	return info
}

// rpcError writes an error as a JSON-RPC error object with an ErrorInfo, like toJsonRpcError of @a2a-js/sdk.
func rpcError(err error) *jsonx.Object {
	if errors.Is(err, errMethodNotFound) {
		return jsonx.NewObject("code", float64(codeMethodNotFound), "message", err.Error())
	}
	for _, known := range rpcCodes {
		if errors.Is(err, known.err) {
			return jsonx.NewObject("code", float64(known.code), "message", err.Error(),
				"data", []any{errorInfo(known.reason, a2aErrorDomain, nil)})
		}
	}
	return jsonx.NewObject("code", float64(-32603), "message", err.Error())
}

func rpcResponse(id any, key string, value any) *jsonx.Object {
	if id == nil {
		id = jsonx.Null
	}
	return jsonx.NewObject("jsonrpc", "2.0", "id", id).Set(key, value)
}

func malformed(message string) error { return a2a.NewError(a2a.ErrInvalidParams, message) }

// paramsError keeps the decode detail for the log. The client gets a fixed text without Go type names.
type paramsError struct{ detail string }

func (e *paramsError) Error() string { return "Invalid method parameters." }

func (e *paramsError) Unwrap() error { return a2a.ErrInvalidParams }

// callError logs the detail of a params error and gives the JSON-RPC error object.
func (s *Server) callError(call rpcCall, err error) *jsonx.Object {
	var params *paramsError
	if errors.As(err, &params) {
		s.log("a2a " + call.method + " params refused: " + params.detail)
	}
	return rpcError(err)
}

func (s *Server) jsonRPC(w http.ResponseWriter, r *http.Request, ref string) error {
	// Authenticate first, so an anonymous caller cannot learn which agents exist.
	principal, anonymous, err := s.principal(r, s.isBuiltinRef(ref))
	if err != nil {
		return err
	}
	found, err := s.resolve(ref)
	if err != nil {
		return err
	}
	var builtin a2aapi.Builtin
	if found.Agent != nil {
		builtin = s.opts.Builtins[found.Agent.ID]
	}
	if anonymous && builtin == nil {
		return fail(http.StatusUnauthorized, "unauthorized")
	}
	caller := engine.Caller{Principal: principal}
	if agentHeader := firstHeader(r, engine.AgentHeader); agentHeader != "" {
		if _, err := s.opts.Engine.RequireOwnAgent(principal, agentHeader); err != nil {
			return err
		}
		caller.Agent = agentHeader
	}
	contentType, _, _ := strings.Cut(firstHeader(r, "Content-Type"), ";")
	contentType = strings.ToLower(strings.TrimFunc(contentType, isJSSpace))
	if contentType != "" && !jsonTypes[contentType] {
		refusal := a2a.NewError(a2a.ErrUnsupportedContentType,
			`Unsupported Content-Type "`+contentType+`"; expected application/json.`)
		writeJSON(w, http.StatusOK, rpcResponse(nil, "error", rpcError(refusal)), nil)
		return nil
	}
	text, err := readBody(r)
	if err != nil {
		return err
	}
	body, err := jsonx.Parse(text)
	if err != nil {
		writeJSON(w, http.StatusOK, rpcResponse(nil, "error",
			jsonx.NewObject("code", float64(codeParseError), "message", "Invalid JSON payload.")), nil)
		return nil
	}
	request, _ := body.(*jsonx.Object)
	id := request.Value("id")
	if found.Agent == nil {
		writeJSON(w, http.StatusOK, rpcResponse(id, "error", jsonx.NewObject(
			"code", float64(codeAmbiguousAlias),
			"message", "agent alias "+ref+" matches more than one online agent",
			"data", []any{errorInfo("AMBIGUOUS_ALIAS", "pimon",
				jsonx.NewObject("agents", strings.Join(found.Ambiguous, ",")))},
		)), nil)
		return nil
	}
	if version := requestedVersion(r); version != supportedA2AVersion {
		refusal := a2a.NewError(a2a.ErrVersionNotSupported, "The requested A2A protocol version '"+version+
			"' is not supported. Supported versions: "+supportedA2AVersion)
		writeJSON(w, http.StatusOK, rpcResponse(id, "error", rpcError(refusal)), nil)
		return nil
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	ctx, callCtx := a2asrv.NewCallContext(ctx, a2asrv.NewServiceParams(r.Header))
	handler := &a2aapi.Handler{
		Engine: s.opts.Engine, Agent: found.Agent, Caller: caller, Builtin: builtin, Holds: s.opts.Holds, Log: s.log,
	}
	call, refusal := parseCall(body)
	if refusal != nil {
		writeJSON(w, http.StatusOK, rpcResponse(call.id, "error", refusal), nil)
		return nil
	}
	if call.method == "SendStreamingMessage" || call.method == "SubscribeToTask" {
		events, err := streamCall(ctx, handler, call)
		if err != nil {
			writeJSON(w, http.StatusOK, rpcResponse(call.id, "error", s.callError(call, err)), nil)
			return nil
		}
		s.writeStream(ctx, cancel, w, call.id, events)
		return nil
	}
	result, err := unaryCall(ctx, handler, call)
	var header http.Header
	if call.method == "SendMessage" {
		if activated := callCtx.Extensions().ActivatedURIs(); len(activated) > 0 {
			header = http.Header{"A2A-Extensions": {strings.Join(activated, ",")}}
		}
	}
	if err != nil {
		writeJSON(w, http.StatusOK, rpcResponse(call.id, "error", s.callError(call, err)), header)
		return nil
	}
	writeJSON(w, http.StatusOK, rpcResponse(call.id, "result", result), header)
	return nil
}

type rpcCall struct {
	id     any
	method string
	params *jsonx.Object
	raw    []byte
}

// parseCall checks the request envelope like the JSON-RPC transport of @a2a-js/sdk.
func parseCall(body any) (rpcCall, *jsonx.Object) {
	if text, ok := body.(string); ok {
		parsed, err := jsonx.Parse([]byte(text))
		if err != nil {
			return rpcCall{}, rpcError(malformed("Failed to parse JSON request."))
		}
		body = parsed
	}
	if _, ok := body.([]any); ok {
		return rpcCall{}, rpcError(malformed("Invalid JSON-RPC Request."))
	}
	request, ok := body.(*jsonx.Object)
	if !ok {
		return rpcCall{}, rpcError(malformed("Invalid request body type."))
	}
	call := rpcCall{id: request.Value("id")}
	method, _ := request.Value("method").(string)
	if request.Value("jsonrpc") != "2.0" || !validID(request) || method == "" {
		return call, rpcError(malformed("Invalid JSON-RPC Request."))
	}
	call.method = method
	params, ok := request.Value("params").(*jsonx.Object)
	if method != "GetExtendedAgentCard" {
		_, empty := params.Get("")
		if !ok || empty {
			return call, rpcError(malformed("Invalid method parameters."))
		}
	}
	if params == nil {
		params = jsonx.NewObject()
	}
	call.params = protoNames(params)
	call.raw = jsonx.Stringify(call.params)
	return call, nil
}

// protoNames renames the proto field names of a request, such as task_id, to their JSON names, as ProtoJSON
// parsers and @a2a-js/sdk accept both. The JSON name wins when both are set. Struct values keep their keys.
func protoNames(params *jsonx.Object) *jsonx.Object {
	out := jsonx.NewObject()
	for _, key := range params.Keys() {
		value := params.Value(key)
		name := key
		if protoFieldNames[key] {
			name = lowerCamel(key)
			if _, taken := params.Get(name); taken {
				continue
			}
		}
		if name != "metadata" && name != "data" {
			value = protoNamesIn(value)
		}
		if number, ok := value.(float64); ok && name == "role" && number >= 0 && int(number) < len(roleNames) && number == float64(int(number)) {
			value = roleNames[int(number)]
		}
		out.Set(name, value)
	}
	return out
}

// roleNames are the Role enum values by number; ProtoJSON parsers accept an enum as its number.
var roleNames = []string{"ROLE_UNSPECIFIED", "ROLE_USER", "ROLE_AGENT"}

// protoFieldNames are the proto names with an underscore of the A2A messages.
var protoFieldNames = map[string]bool{
	"accepted_output_modes": true, "api_key_security_scheme": true, "artifact_id": true, "artifact_update": true,
	"authorization_code": true, "authorization_url": true, "bearer_format": true, "client_credentials": true,
	"context_id": true, "default_input_modes": true, "default_output_modes": true,
	"device_authorization_url": true, "device_code": true, "documentation_url": true,
	"extended_agent_card": true, "history_length": true, "http_auth_security_scheme": true, "icon_url": true,
	"include_artifacts": true, "input_modes": true, "last_chunk": true, "media_type": true, "message_id": true,
	"mtls_security_scheme": true, "next_page_token": true, "open_id_connect_security_scheme": true,
	"open_id_connect_url": true, "output_modes": true, "page_size": true, "page_token": true,
	"pkce_required": true, "protocol_binding": true, "protocol_version": true, "push_notifications": true,
	"reference_task_ids": true, "refresh_url": true, "return_immediately": true, "security_requirements": true,
	"security_schemes": true, "status_timestamp_after": true, "status_update": true,
	"supported_interfaces": true, "task_id": true, "task_push_notification_config": true, "token_url": true,
	"total_size": true,
}

func protoNamesIn(value any) any {
	switch v := value.(type) {
	case *jsonx.Object:
		return protoNames(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = protoNamesIn(item)
		}
		return out
	}
	return value
}

func lowerCamel(name string) string {
	parts := strings.Split(name, "_")
	for i := 1; i < len(parts); i++ {
		if parts[i] != "" {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return strings.Join(parts, "")
}

func validID(request *jsonx.Object) bool {
	id, present := request.Get("id")
	if !present {
		return true
	}
	switch v := id.(type) {
	case string:
		return true
	case float64:
		return v == math.Trunc(v) && !math.IsInf(v, 0)
	}
	return jsonx.IsNull(id)
}

func decode[T any](call rpcCall) (*T, error) {
	var out T
	if err := json.Unmarshal(call.raw, &out); err != nil {
		return nil, &paramsError{detail: err.Error()}
	}
	return &out, nil
}

var taskStates = map[string]bool{
	"TASK_STATE_UNSPECIFIED": true, "TASK_STATE_SUBMITTED": true, "TASK_STATE_WORKING": true,
	"TASK_STATE_COMPLETED": true, "TASK_STATE_FAILED": true, "TASK_STATE_CANCELED": true,
	"TASK_STATE_INPUT_REQUIRED": true, "TASK_STATE_REJECTED": true, "TASK_STATE_AUTH_REQUIRED": true,
}

var taskStateNumbers = []a2a.TaskState{
	a2a.TaskStateUnspecified, a2a.TaskStateSubmitted, a2a.TaskStateWorking, a2a.TaskStateCompleted,
	a2a.TaskStateFailed, a2a.TaskStateCanceled, a2a.TaskStateInputRequired, a2a.TaskStateRejected,
	a2a.TaskStateAuthRequired,
}

// listTasksRequest reads the ListTasks fields whose checks need the raw value.
func listTasksRequest(call rpcCall) (*a2a.ListTasksRequest, error) {
	params := call.params.Clone()
	var status a2a.TaskState
	if raw := params.Value("status"); raw != nil && !jsonx.IsNull(raw) {
		switch v := raw.(type) {
		case string:
			if !taskStates[v] {
				return nil, malformed("Invalid status filter: -1")
			}
			status = a2a.TaskState(v)
		case float64:
			if v != math.Trunc(v) || v < 0 || int(v) >= len(taskStateNumbers) {
				return nil, malformed("Invalid status filter: -1")
			}
			status = taskStateNumbers[int(v)]
		default:
			return nil, malformed("Invalid status filter: -1")
		}
	}
	params.Delete("status")
	pageSize := 0
	if raw := params.Value("pageSize"); raw != nil && !jsonx.IsNull(raw) {
		n, ok := raw.(float64)
		if !ok || n != math.Trunc(n) || n < 1 || n > a2aapi.MaxPageSize {
			return nil, malformed("pageSize must be between 1 and 100")
		}
		pageSize = int(n)
	}
	params.Delete("pageSize")
	var after *time.Time
	if raw, ok := params.Value("statusTimestampAfter").(string); ok && raw != "" {
		ms, valid := engine.ParseTimestamp(raw)
		if !valid {
			return nil, malformed("statusTimestampAfter must be a valid ISO 8601 timestamp")
		}
		at := time.UnixMilli(ms).UTC()
		after = &at
	}
	params.Delete("statusTimestampAfter")
	req, err := decode[a2a.ListTasksRequest](rpcCall{raw: jsonx.Stringify(params)})
	if err != nil {
		return nil, err
	}
	req.Status, req.PageSize, req.StatusTimestampAfter = status, pageSize, after
	return req, nil
}

func withMessage(ctx context.Context, call rpcCall) context.Context {
	message, _ := call.params.Value("message").(*jsonx.Object)
	return a2aapi.WithRawMessage(ctx, message)
}

func unaryCall(ctx context.Context, h *a2aapi.Handler, call rpcCall) (any, error) {
	switch call.method {
	case "SendMessage":
		req, err := decode[a2a.SendMessageRequest](call)
		if err != nil {
			return nil, err
		}
		result, err := h.SendMessage(withMessage(ctx, call), req)
		if err != nil {
			return nil, err
		}
		return a2aapi.WireEvent(result)
	case "GetTask":
		req, err := decode[a2a.GetTaskRequest](call)
		if err != nil {
			return nil, err
		}
		return wireTask(h.GetTask(ctx, req))
	case "ListTasks":
		req, err := listTasksRequest(call)
		if err != nil {
			return nil, err
		}
		result, err := h.ListTasks(ctx, req)
		if err != nil {
			return nil, err
		}
		tasks := make([]any, 0, len(result.Tasks))
		for _, task := range result.Tasks {
			obj, err := wireTask(task, nil)
			if err != nil {
				return nil, err
			}
			if _, ok := obj.Get("artifacts"); req.IncludeArtifacts && !ok {
				obj.Set("artifacts", []any{})
			}
			tasks = append(tasks, obj)
		}
		return jsonx.NewObject("tasks", tasks, "nextPageToken", result.NextPageToken,
			"pageSize", float64(result.PageSize), "totalSize", float64(result.TotalSize)), nil
	case "CancelTask":
		req, err := decode[a2a.CancelTaskRequest](call)
		if err != nil {
			return nil, err
		}
		return wireTask(h.CancelTask(ctx, req))
	case "CreateTaskPushNotificationConfig":
		req, err := decode[a2a.PushConfig](call)
		if err != nil {
			return nil, err
		}
		return wirePushConfig(h.CreateTaskPushConfig(ctx, req))
	case "GetTaskPushNotificationConfig":
		req, err := decode[a2a.GetTaskPushConfigRequest](call)
		if err != nil {
			return nil, err
		}
		return wirePushConfig(h.GetTaskPushConfig(ctx, req))
	case "ListTaskPushNotificationConfigs":
		req, err := decode[a2a.ListTaskPushConfigRequest](call)
		if err != nil {
			return nil, err
		}
		result, err := h.ListTaskPushConfigs(ctx, req)
		if err != nil {
			return nil, err
		}
		out := jsonx.NewObject()
		if len(result.Configs) > 0 {
			configs := make([]any, 0, len(result.Configs))
			for _, config := range result.Configs {
				obj, err := wirePushConfig(config, nil)
				if err != nil {
					return nil, err
				}
				configs = append(configs, obj)
			}
			out.Set("configs", configs)
		}
		if result.NextPageToken != "" {
			out.Set("nextPageToken", result.NextPageToken)
		}
		return out, nil
	case "DeleteTaskPushNotificationConfig":
		req, err := decode[a2a.DeleteTaskPushConfigRequest](call)
		if err != nil {
			return nil, err
		}
		if err := h.DeleteTaskPushConfig(ctx, req); err != nil {
			return nil, err
		}
		return jsonx.Null, nil
	case "GetExtendedAgentCard":
		return h.GetExtendedAgentCard(ctx, &a2a.GetExtendedAgentCardRequest{})
	}
	return nil, errMethodNotFound
}

var errMethodNotFound = errors.New("Invalid method.") //nolint:staticcheck // ST1005: the text is the wire text of the TypeScript hub.

func wireTask(task *a2a.Task, err error) (*jsonx.Object, error) {
	if err != nil {
		return nil, err
	}
	event, err := a2aapi.WireEvent(task)
	if err != nil {
		return nil, err
	}
	return event.Value("task").(*jsonx.Object), nil
}

func wirePushConfig(config *a2a.PushConfig, err error) (*jsonx.Object, error) {
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	value, err := jsonx.Parse(data)
	if err != nil {
		return nil, err
	}
	obj := value.(*jsonx.Object)
	if obj.Value("taskId") == "" {
		obj.Delete("taskId")
	}
	if obj.Value("url") == "" {
		obj.Delete("url")
	}
	return a2aapi.WirePushConfig(obj), nil
}

func streamCall(ctx context.Context, h *a2aapi.Handler, call rpcCall) (iter.Seq2[a2a.Event, error], error) {
	if call.method == "SendStreamingMessage" {
		req, err := decode[a2a.SendMessageRequest](call)
		if err != nil {
			return nil, err
		}
		return h.SendStreamingMessage(withMessage(ctx, call), req), nil
	}
	req, err := decode[a2a.SubscribeToTaskRequest](call)
	if err != nil {
		return nil, err
	}
	return h.SubscribeToTask(ctx, req), nil
}

type streamItem struct {
	event a2a.Event
	err   error
}

// writeStream writes the events as SSE with the framing of @a2a-js/sdk: data lines only, and an error after
// the start as an "error" event. An error before the first event is a plain JSON-RPC response.
func (s *Server) writeStream(ctx context.Context, cancel context.CancelFunc, w http.ResponseWriter, id any, events iter.Seq2[a2a.Event, error]) {
	items := make(chan streamItem)
	produced := make(chan struct{})
	go func() {
		defer close(produced)
		defer close(items)
		for event, err := range events {
			select {
			case items <- streamItem{event, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() {
		cancel()
		<-produced
	}()

	var first streamItem
	var ok bool
	select {
	case first, ok = <-items:
	case <-ctx.Done():
		return
	}
	if ok && first.err != nil {
		writeJSON(w, http.StatusOK, rpcResponse(id, "error", rpcError(first.err)), nil)
		return
	}
	header := w.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	header.Set("Connection", "keep-alive")
	header.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	control := http.NewResponseController(w)
	write := func(frame string) bool {
		if _, err := w.Write([]byte(frame)); err != nil {
			return false
		}
		return control.Flush() == nil
	}
	if !write("") {
		return
	}
	send := func(item streamItem) bool {
		if item.err != nil {
			write("event: error\ndata: " + string(jsonx.Stringify(rpcResponse(id, "error", rpcError(item.err)))) + "\n\n")
			return false
		}
		event, err := a2aapi.WireEvent(item.event)
		if err != nil {
			s.log("a2a stream failed: " + err.Error())
			write("event: error\ndata: " + string(jsonx.Stringify(rpcResponse(id, "error",
				rpcError(a2a.NewError(a2a.ErrInternalError, "internal error"))))) + "\n\n")
			return false
		}
		return write("data: " + string(jsonx.Stringify(rpcResponse(id, "result", event))) + "\n\n")
	}
	if !ok || !send(first) {
		return
	}
	heartbeat := time.NewTicker(s.opts.Heartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case item, ok := <-items:
			if !ok || !send(item) {
				return
			}
		case <-heartbeat.C:
			if !write(": hb\n\n") {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
