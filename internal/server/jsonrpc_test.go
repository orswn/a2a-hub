package server

import (
	"testing"

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
