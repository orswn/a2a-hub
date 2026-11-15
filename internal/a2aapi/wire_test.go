package a2aapi

import (
	"context"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/orswn/a2a-hub/internal/jsonx"
)

func TestStoredMessageKeepsTheKeyOrderOfTheClientInStructValues(t *testing.T) {
	parsed, err := jsonx.Parse([]byte(`{"messageId":"m1","role":"ROLE_USER",` +
		`"parts":[{"data":{"z":1,"a":{"y":2,"b":3}},"metadata":{"z":1,"a":2}}],"metadata":{"zz":{"kind":"note","deliveryWait":true}}}`))
	if err != nil {
		t.Fatal(err)
	}
	raw := parsed.(*jsonx.Object)
	message, err := fromWire[a2a.Message](raw)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithRawMessage(context.Background(), raw)
	wire, err := messageToWire(message, rawMessage(ctx))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"messageId":"m1","role":"ROLE_USER","parts":[{"data":{"z":1,"a":{"y":2,"b":3}},"metadata":{"z":1,"a":2}}],` +
		`"metadata":{"zz":{"kind":"note","deliveryWait":true}}}`
	if got := string(jsonx.Stringify(wire)); got != want {
		t.Errorf("stored\n got %s\nwant %s", got, want)
	}
	sorted, err := messageToWire(message, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(jsonx.Stringify(sorted.Value("metadata"))); got != `{"zz":{"deliveryWait":true,"kind":"note"}}` {
		t.Errorf("without the raw message: %s", got)
	}
}
