package mcp

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/orswn/a2a-hub/internal/jsonx"
)

var update = flag.Bool("update", false, "rewrite the model-visible text snapshot")

// snapshot records the model-visible text of the endpoint with its char counts. Clients send the tool schemas
// with each model request, so a change in size shows in review.
func snapshot(t *testing.T, lists map[string]*jsonx.Object) string {
	t.Helper()
	head := "instructions: " + strconv.Itoa(utf8.RuneCountInString(Instructions)) + " chars\n"
	var body strings.Builder
	for _, harness := range []string{HarnessClaude, HarnessCodex, HarnessHermes} {
		compact := jsonx.Stringify(lists[harness])
		head += "tools/list " + harness + ": " + strconv.Itoa(utf8.RuneCount(compact)) + " chars of compact JSON\n"
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, compact, "", "  "); err != nil {
			t.Fatal(err)
		}
		body.WriteString("\n## tools/list " + harness + "\n\n" + pretty.String() + "\n")
	}
	return head + "\n" + Instructions + "\n" + body.String()
}

func TestModelVisibleTextMatchesTheSnapshot(t *testing.T) {
	h := setup(t)
	if err := h.e.EnsureExternal("hermes", "hermes"); err != nil {
		t.Fatal(err)
	}
	clients := map[string]*client{
		HarnessClaude: h.client("mars-token", HarnessClaude, AgentHeader, "claude-1"),
		HarnessCodex:  h.client("mars-token", HarnessCodex),
		HarnessHermes: h.client("hermes-token", HarnessHermes),
	}
	lists := map[string]*jsonx.Object{}
	for harness, c := range clients {
		c.initialize()
		got := c.rpc("tools/list", nil)
		result, ok := got.body.Object("result")
		if !ok {
			t.Fatalf("tools/list %s: %s", harness, jsonx.Stringify(got.body))
		}
		lists[harness] = result
	}
	text := snapshot(t, lists)
	path := filepath.Join("testdata", "model-visible.txt")
	if *update {
		if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if text != string(want) {
		t.Errorf("model-visible text changed; run go test ./internal/mcp -run Snapshot -update and state the char counts in the commit\n got:\n%s", text)
	}
	if n := utf8.RuneCountInString(Instructions); n < 400 || n > 800 {
		t.Errorf("instructions are %d chars, want 400 to 800", n)
	}
}
