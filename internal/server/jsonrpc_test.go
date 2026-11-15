package server

import (
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/orswn/a2a-hub/internal/jsonx"
)

func parse(t *testing.T, text string) any {
	t.Helper()
	value, err := jsonx.Parse([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func TestParseCallReadsTheBodyLikeTheJSONRPCTransportOfTheJSSDK(t *testing.T) {
	cases := []struct{ body, refusal string }{
		{`[]`, "Invalid JSON-RPC Request."},
		{`null`, "Invalid request body type."},
		{`5`, "Invalid request body type."},
		{`"{nope"`, "Failed to parse JSON request."},
		{`"[]"`, "Invalid JSON-RPC Request."},
		{`{"jsonrpc":"1.0","id":1,"method":"GetTask","params":{}}`, "Invalid JSON-RPC Request."},
		{`{"jsonrpc":"2.0","id":1,"method":"GetTask","params":[]}`, "Invalid method parameters."},
		{`"{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"GetTask\",\"params\":{\"id\":\"x\"}}"`, ""},
	}
	for _, c := range cases {
		call, refusal := parseCall(parse(t, c.body))
		got := ""
		if refusal != nil {
			got, _ = refusal.Value("message").(string)
		}
		if got != c.refusal {
			t.Errorf("%s: refusal %q, want %q", c.body, got, c.refusal)
		}
		if c.refusal == "" && (call.method != "GetTask" || call.id != float64(4)) {
			t.Errorf("%s: call %+v", c.body, call)
		}
	}
}

func TestParseCallTakesProtoFieldNamesAndRoleNumbers(t *testing.T) {
	call, refusal := parseCall(parse(t, `{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{
		"message":{"message_id":"m1","context_id":"c1","role":1,
			"metadata":{"z_key":{"task_id":"kept"},"a":1},
			"parts":[{"media_type":"text/plain","data":{"last_chunk":true},"text":"x"}]},
		"configuration":{"return_immediately":true,"history_length":0},
		"task_id":"snake","taskId":"camel"}}`))
	if refusal != nil {
		t.Fatal(string(jsonx.Stringify(refusal)))
	}
	want := `{"message":{"messageId":"m1","contextId":"c1","role":"ROLE_USER",` +
		`"metadata":{"z_key":{"task_id":"kept"},"a":1},` +
		`"parts":[{"mediaType":"text/plain","data":{"last_chunk":true},"text":"x"}]},` +
		`"configuration":{"returnImmediately":true,"historyLength":0},"taskId":"camel"}`
	if got := string(call.raw); got != want {
		t.Errorf("params\n got %s\nwant %s", got, want)
	}
}

func TestParseCallKeepsARoleNumberOutsideTheEnum(t *testing.T) {
	call, refusal := parseCall(parse(t, `{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"role":7}}}`))
	if refusal != nil {
		t.Fatal(string(jsonx.Stringify(refusal)))
	}
	if got := string(call.raw); got != `{"message":{"role":7}}` {
		t.Errorf("params %s", got)
	}
}

func TestParamsOfAWrongJSONTypeGetAFixedTextAndTheDetailGoesToTheLog(t *testing.T) {
	var logged []string
	s := &Server{log: func(line string) { logged = append(logged, line) }}
	call := rpcCall{method: "GetTask", raw: []byte(`{"id":"t1","historyLength":"2"}`)}
	_, err := decode[a2a.GetTaskRequest](call)
	if err == nil {
		t.Fatal("decode took a string historyLength")
	}
	got := string(jsonx.Stringify(s.callError(call, err)))
	want := `{"code":-32602,"message":"Invalid method parameters.","data":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"INVALID_PARAMS","domain":"a2a-protocol.org"}]}`
	if got != want {
		t.Errorf("error %s", got)
	}
	if len(logged) != 1 || !strings.HasPrefix(logged[0], "a2a GetTask params refused: ") ||
		!strings.Contains(logged[0], "historyLength") {
		t.Errorf("log %q", logged)
	}
}
