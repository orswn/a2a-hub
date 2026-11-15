package codexrpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orswn/a2a-hub/internal/codexrpc"
	"github.com/orswn/a2a-hub/internal/codexrpc/codexrpctest"
)

func socketPath(t *testing.T) string {
	t.Helper()
	// A unix socket path must stay under 108 bytes, so the dir name stays short.
	dir, err := os.MkdirTemp("", "rpc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

func TestCallsGetAnswersAndErrorsWithoutAJSONRPCMember(t *testing.T) {
	server := codexrpctest.Start(t, socketPath(t), func(call codexrpctest.Call) (any, *codexrpc.Error) {
		switch call.Method {
		case "thread/loaded/list":
			return map[string]any{"data": []string{"t1"}, "nextCursor": nil}, nil
		case "turn/start":
			return nil, &codexrpc.Error{Code: codexrpc.CodeOverloaded, Message: "Server overloaded; retry later."}
		}
		return nil, nil
	})
	ctx := context.Background()
	conn, err := codexrpc.Dial(ctx, server.Path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.Initialize(ctx, codexrpc.ClientInfo{Name: "test", Version: "1"}, []string{"item/agentMessage/delta"}); err != nil {
		t.Fatal(err)
	}
	var loaded struct {
		Data []string `json:"data"`
	}
	if err := conn.Call(ctx, "thread/loaded/list", map[string]any{}, &loaded); err != nil || len(loaded.Data) != 1 {
		t.Fatalf("loaded = %+v, %v", loaded, err)
	}
	err = conn.Call(ctx, "turn/start", map[string]any{"threadId": "t1"}, nil)
	if !codexrpc.IsCode(err, codexrpc.CodeOverloaded) {
		t.Fatalf("turn/start error = %v", err)
	}
	init, err := server.WaitCall(time.Second, 0, codexrpctest.Method("initialize"))
	if err != nil {
		t.Fatal(err)
	}
	var params map[string]any
	if err := json.Unmarshal(init.Params, &params); err != nil {
		t.Fatal(err)
	}
	if params["capabilities"].(map[string]any)["optOutNotificationMethods"].([]any)[0] != "item/agentMessage/delta" {
		t.Fatalf("initialize params = %v", params)
	}
	if _, err := server.WaitCall(time.Second, 0, codexrpctest.Method("initialized")); err != nil {
		t.Fatal("no initialized notification")
	}
}

func TestNotificationsAndServerRequestsArriveInOrder(t *testing.T) {
	server := codexrpctest.Start(t, socketPath(t), nil)
	got := make(chan codexrpc.Message, 4)
	conn, err := codexrpc.Dial(context.Background(), server.Path, func(m codexrpc.Message) { got <- m })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.Call(context.Background(), "ping", nil, nil); err != nil {
		t.Fatal(err)
	}
	server.Notify("thread/started", map[string]any{"thread": map[string]any{"id": "t1"}})
	server.Request(7, "item/commandExecution/requestApproval", map[string]any{"threadId": "t1"})
	first, second := <-got, <-got
	if first.Method != "thread/started" || len(first.ID) != 0 {
		t.Fatalf("first = %+v", first)
	}
	if second.Method != "item/commandExecution/requestApproval" || string(second.ID) != "7" {
		t.Fatalf("second = %+v", second)
	}
}

func TestAPendingCallFailsWhenTheServerGoes(t *testing.T) {
	block := make(chan struct{})
	server := codexrpctest.Start(t, socketPath(t), func(call codexrpctest.Call) (any, *codexrpc.Error) {
		if call.Method == "slow" {
			<-block
		}
		return nil, nil
	})
	defer close(block)
	conn, err := codexrpc.Dial(context.Background(), server.Path, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- conn.Call(context.Background(), "slow", nil, nil) }()
	if _, err := server.WaitCall(time.Second, 0, codexrpctest.Method("slow")); err != nil {
		t.Fatal(err)
	}
	server.Close()
	select {
	case err := <-result:
		if !errors.Is(err, codexrpc.ErrClosed) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("call did not end")
	}
	<-conn.Done()
	if err := conn.Call(context.Background(), "ping", nil, nil); !errors.Is(err, codexrpc.ErrClosed) {
		t.Fatalf("call after close: %v", err)
	}
}
