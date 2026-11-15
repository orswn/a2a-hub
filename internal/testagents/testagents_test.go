package testagents

import (
	"context"
	"testing"
	"time"

	"github.com/orswn/a2a-hub/internal/auth"
	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/jsonx"
	"github.com/orswn/a2a-hub/internal/store"
)

func TestStopEndsAWaitingScenarioAndWaitsForIt(t *testing.T) {
	db, err := store.Open(context.Background(), store.Memory)
	if err != nil {
		t.Fatal(err)
	}
	e, err := engine.New(context.Background(), db, engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = e.Close()
		_ = db.Close()
	}()
	builtins, agents, err := Install(e, func(line string) { t.Error(line) })
	if err != nil {
		t.Fatal(err)
	}
	message := jsonx.NewObject("messageId", "test-resubscribe-message-id-1", "role", "ROLE_USER",
		"parts", []any{jsonx.NewObject("text", "x")})
	sent, err := e.Send(engine.SendInput{
		AgentID: "tck", Caller: engine.Caller{Principal: auth.Principal{Name: "mars", Kind: auth.KindHost}}, Message: message,
	})
	if err != nil {
		t.Fatal(err)
	}
	builtins["tck"].Start(e, sent.Task.ID, message)
	deadline := time.Now().Add(5 * time.Second)
	for {
		state, err := e.TaskState(sent.Task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if state == engine.StateWorking {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("state %s", state)
		}
		time.Sleep(5 * time.Millisecond)
	}
	start := time.Now()
	agents.Stop()
	if took := time.Since(start); took > time.Second {
		t.Errorf("Stop took %v", took)
	}
	if state, _ := e.TaskState(sent.Task.ID); state != engine.StateWorking {
		t.Errorf("state after Stop %s", state)
	}
	builtins["echo"].Start(e, sent.Task.ID, message)
	time.Sleep(50 * time.Millisecond)
	if state, _ := e.TaskState(sent.Task.ID); state != engine.StateWorking {
		t.Errorf("a scenario ran after Stop: %s", state)
	}
}
