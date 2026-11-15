package a2aapi

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/orswn/a2a-hub/internal/jsonx"
)

// The key orders of the ProtoJSON writer of @a2a-js/sdk. The TypeScript hub stores and sends its JSON in these
// orders, so the Go hub writes them too. a2a-go writes its fields in another order.
var (
	taskKeys           = []string{"id", "contextId", "status", "artifacts", "history", "metadata"}
	statusKeys         = []string{"state", "message", "timestamp"}
	messageKeys        = []string{"messageId", "contextId", "taskId", "role", "parts", "metadata", "extensions", "referenceTaskIds"}
	partKeys           = []string{"text", "raw", "url", "data", "metadata", "filename", "mediaType"}
	artifactKeys       = []string{"artifactId", "name", "description", "parts", "metadata", "extensions"}
	statusUpdateKeys   = []string{"taskId", "contextId", "status", "metadata"}
	artifactUpdateKeys = []string{"taskId", "contextId", "artifact", "append", "lastChunk", "metadata"}
	pushConfigKeys     = []string{"tenant", "id", "taskId", "url", "token", "authentication"}
	authKeys           = []string{"scheme", "credentials"}
)

// reorder copies the keys of obj in the given order. A key that is not in the list goes last.
func reorder(obj *jsonx.Object, keys []string, nested map[string]func(any) any) *jsonx.Object {
	if obj == nil {
		return nil
	}
	out := jsonx.NewObject()
	put := func(key string) {
		value, ok := obj.Get(key)
		if _, done := out.Get(key); !ok || done {
			return
		}
		if fix := nested[key]; fix != nil {
			value = fix(value)
		}
		out.Set(key, value)
	}
	for _, key := range keys {
		put(key)
	}
	for _, key := range obj.Keys() {
		put(key)
	}
	return out
}

func each(fix func(*jsonx.Object) *jsonx.Object) func(any) any {
	return func(v any) any {
		items, ok := v.([]any)
		if !ok {
			return v
		}
		out := make([]any, len(items))
		for i, item := range items {
			if obj, ok := item.(*jsonx.Object); ok {
				out[i] = fix(obj)
			} else {
				out[i] = item
			}
		}
		return out
	}
}

func one(fix func(*jsonx.Object) *jsonx.Object) func(any) any {
	return func(v any) any {
		if obj, ok := v.(*jsonx.Object); ok {
			return fix(obj)
		}
		return v
	}
}

func wirePart(obj *jsonx.Object) *jsonx.Object { return reorder(obj, partKeys, nil) }

// WireMessage orders a message like the ProtoJSON writer.
func WireMessage(obj *jsonx.Object) *jsonx.Object {
	return reorder(obj, messageKeys, map[string]func(any) any{"parts": each(wirePart)})
}

// WireArtifact orders an artifact like the ProtoJSON writer.
func WireArtifact(obj *jsonx.Object) *jsonx.Object {
	return reorder(obj, artifactKeys, map[string]func(any) any{"parts": each(wirePart)})
}

func wireStatus(obj *jsonx.Object) *jsonx.Object {
	return reorder(obj, statusKeys, map[string]func(any) any{"message": one(WireMessage)})
}

// WireTask orders a task like the ProtoJSON writer.
func WireTask(obj *jsonx.Object) *jsonx.Object {
	return reorder(obj, taskKeys, map[string]func(any) any{
		"status":    one(wireStatus),
		"artifacts": each(WireArtifact),
		"history":   each(WireMessage),
	})
}

func wireStatusUpdate(obj *jsonx.Object) *jsonx.Object {
	return reorder(obj, statusUpdateKeys, map[string]func(any) any{"status": one(wireStatus)})
}

func wireArtifactUpdate(obj *jsonx.Object) *jsonx.Object {
	return reorder(obj, artifactUpdateKeys, map[string]func(any) any{"artifact": one(WireArtifact)})
}

// WirePushConfig orders a push config like the ProtoJSON writer.
func WirePushConfig(obj *jsonx.Object) *jsonx.Object {
	return reorder(obj, pushConfigKeys, map[string]func(any) any{"authentication": one(func(o *jsonx.Object) *jsonx.Object {
		return reorder(o, authKeys, nil)
	})})
}

// WireEvent writes a stream event as a StreamResponse object.
func WireEvent(event a2a.Event) (*jsonx.Object, error) {
	obj, err := toObject(event)
	if err != nil {
		return nil, err
	}
	switch event.(type) {
	case *a2a.Task:
		return jsonx.NewObject("task", WireTask(obj)), nil
	case *a2a.Message:
		return jsonx.NewObject("message", WireMessage(obj)), nil
	case *a2a.TaskStatusUpdateEvent:
		return jsonx.NewObject("statusUpdate", wireStatusUpdate(obj)), nil
	case *a2a.TaskArtifactUpdateEvent:
		return jsonx.NewObject("artifactUpdate", wireArtifactUpdate(obj)), nil
	}
	return nil, fmt.Errorf("unknown event type %T", event)
}

// toObject writes an a2a value as a JSON object.
func toObject(v any) (*jsonx.Object, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	parsed, err := jsonx.Parse(data)
	if err != nil {
		return nil, err
	}
	obj, ok := parsed.(*jsonx.Object)
	if !ok {
		return nil, fmt.Errorf("%T is not a JSON object", v)
	}
	return obj, nil
}

func keepRaw(obj, raw *jsonx.Object, key string) {
	if _, ok := obj.Get(key); !ok {
		return
	}
	if value, ok := raw.Get(key); ok {
		obj.Set(key, value)
	}
}

// fromWire reads a JSON value into an a2a type.
func fromWire[T any](v any) (*T, error) {
	var out T
	if err := json.Unmarshal(jsonx.Stringify(v), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

type rawMessageKey struct{}

// WithRawMessage gives the send calls of ctx the request message as the client wrote it. The metadata and data
// values of the stored message then keep the key order of the client, as in the TypeScript hub; a2a-go
// decodes them into maps.
func WithRawMessage(ctx context.Context, message *jsonx.Object) context.Context {
	return context.WithValue(ctx, rawMessageKey{}, message)
}

func rawMessage(ctx context.Context) *jsonx.Object {
	message, _ := ctx.Value(rawMessageKey{}).(*jsonx.Object)
	return message
}

// messageToWire turns a request message into the JSON that the engine stores. raw, when set, is the same
// message as the client wrote it.
func messageToWire(message *a2a.Message, raw *jsonx.Object) (*jsonx.Object, error) {
	obj, err := toObject(message)
	if err != nil {
		return nil, err
	}
	if raw != nil {
		keepRaw(obj, raw, "metadata")
		parts, _ := obj.Value("parts").([]any)
		rawParts, _ := raw.Value("parts").([]any)
		if len(parts) == len(rawParts) {
			for i, part := range parts {
				partObj, ok := part.(*jsonx.Object)
				rawPart, rawOK := rawParts[i].(*jsonx.Object)
				if ok && rawOK {
					keepRaw(partObj, rawPart, "metadata")
					keepRaw(partObj, rawPart, "data")
				}
			}
		}
	}
	// a2a-go writes an empty parts list and the unspecified role; the ProtoJSON writer leaves both out.
	if parts, ok := obj.Value("parts").([]any); ok && len(parts) == 0 {
		obj.Delete("parts")
	}
	if obj.Value("role") == "ROLE_UNSPECIFIED" {
		obj.Delete("role")
	}
	return WireMessage(obj), nil
}
