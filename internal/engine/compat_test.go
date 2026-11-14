package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orswn/a2a-hub/internal/jsonx"
	"github.com/orswn/a2a-hub/internal/store"
)

// The TypeScript hub must open a file that the Go hub wrote and the reverse. The scenario in
// testdata/compat.mjs runs here through the Go engine with the same clock and ids. Both files must hold
// the same rows, and both engines must read the same JSON from either file.
//
// testdata/compat-node holds the file and the reads that the TypeScript hub made, so the test runs without
// a pimon checkout. With PIMON_DIR it also runs the TypeScript side again.

var (
	compatStart = startTime
	compatEnd   = startTime + 60_000
)

// close closes the engine and the database before the test ends, so the next connection finds a closed
// file.
func (h *testEnv) close() {
	_ = h.engine.Close()
	_ = h.db.Close()
}

func runCompatScenario(t *testing.T, dir string) {
	h := setupAt(t, dir, compatStart, Options{})
	e := h.engine
	bus := string(jsonx.Stringify(BusExtensionURI))
	askerCaller := Caller{Principal: mars, Agent: "asker"}
	helper := Caller{Principal: mars, Agent: "helper"}

	got(e.Register(mars, parse(t, `{"agent":"asker","protocol":2,"name":"Asker","cwd":"/work","role":"lead",
		"labels":{"team":"core","model":"m1"},"topics":["build","build","ci"]}`))).ok(t)
	got(e.Register(mars, parse(t, `{"agent":"helper","protocol":2,"name":"Helper","parent":"asker","ephemeral":true}`))).ok(t)
	ok(t, e.EnsureBuiltin("tck", "tck"))
	ok(t, e.EnsureExternal("ext", "External"))
	ok(t, e.SetOnline("asker", true))
	ok(t, e.SetOnline("helper", true))
	h.advance(1000)
	sent := got(e.Send(SendInput{
		AgentID: "helper",
		Caller:  askerCaller,
		Message: parse(t, `{"messageId":"m-go","role":"ROLE_USER",
			"parts":[{"text":"go <&> \u2028 héllo ✓","metadata":{"k":1}},{"data":{"n":1.5,"big":1e21,"list":[]}}],
			"extensions":["urn:x"],
			"metadata":{`+bus+`:{"kind":"task","expectReply":true,"subject":"build it"},"other":{"keep":true}}}`),
		PushConfig: &PushConfigInput{
			URL: "https://hooks.example/x", Token: "tok",
			Authentication: &Authentication{Scheme: "Bearer", Credentials: "cred"},
		},
	})).ok(t)
	taskID := sent.Task.ID
	h.advance(1000)
	got(e.LinkUpdate("helper", taskID, parse(t, `{"state":"working"}`))).ok(t)
	h.advance(1000)
	got(e.SetStatus("helper", "  reading\n files  ")).ok(t)
	h.advance(1000)
	notLast := false
	got(e.ApplyChange(taskID, TaskChange{Artifacts: []ArtifactChange{{
		Artifact:  parse(t, `{"artifactId":"art-1","name":"out","parts":[{"text":"a"}],"extra":"x"}`),
		LastChunk: &notLast,
	}}})).ok(t)
	got(e.ApplyChange(taskID, TaskChange{Artifacts: []ArtifactChange{{
		Artifact: parse(t, `{"artifactId":"art-1","parts":[{"text":"b"}]}`),
		Append:   true,
	}}})).ok(t)
	h.advance(1000)
	got(e.LinkUpdate("helper", taskID, parse(t, `{"state":"input-required","text":"which?"}`))).ok(t)
	h.advance(1000)
	got(e.Send(SendInput{AgentID: "helper", Caller: askerCaller,
		Message: parse(t, `{"messageId":"m-that","role":"ROLE_USER","parts":[{"text":"that"}],"taskId":"`+taskID+`"}`)})).ok(t)
	h.advance(1000)
	got(e.LinkUpdate("helper", taskID, parse(t, `{"state":"completed","text":"done",
		"artifact":{"name":"report","text":"body","data":{"ok":true}}}`))).ok(t)
	h.advance(1000)
	note := got(e.Send(SendInput{AgentID: "helper", Caller: askerCaller,
		Message: parse(t, `{"messageId":"m-note","role":"ROLE_USER","parts":[{"text":"fyi"}],"contextId":"ctx-note",
			"metadata":{`+bus+`:{"kind":"note","wake":"never","priority":"urgent","expectReply":true,"data":null}}}`)})).ok(t)
	h.advance(1000)
	noteContext, _ := note.Message.String("contextId")
	got(e.Send(SendInput{AgentID: "asker", Caller: helper,
		Message: parse(t, `{"messageId":"m-reply","role":"ROLE_USER","parts":[{"text":"ack"}],"contextId":"`+noteContext+`",
			"metadata":{`+bus+`:{"kind":"answer","replyTo":"m-note"}}}`)})).ok(t)
	h.advance(1000)
	hermesTask := got(e.Send(SendInput{AgentID: "helper", Caller: fromHermes,
		Message: parse(t, `{"messageId":"m-hermes","role":"ROLE_USER","parts":[{"text":"peer job"}],"contextId":"ctx-hermes"}`)})).ok(t).Task.ID
	got(e.CreatePushConfig("helper", fromHermes, PushConfigInput{TaskID: hermesTask, URL: "http://hooks.example:8080/p"})).ok(t)
	got(e.LinkUpdate("helper", hermesTask, parse(t, `{"state":"working","text":"on it"}`))).ok(t)
	h.advance(1000)
	first := got(e.Unacked("helper")).ok(t)[0]
	got(e.Ack("helper", float64(first.Seq))).ok(t)
	got(e.MarkRead("helper", []any{"m-go", "m-note"})).ok(t)
	h.advance(1000)
	extTask := got(e.Send(SendInput{AgentID: "ext", Caller: askerCaller,
		Message: parse(t, `{"messageId":"m-ext","role":"ROLE_USER","parts":[{"text":"research"}]}`)})).ok(t).Task.ID
	remote := got(e.StartRemote(got(e.ExternalJobs()).ok(t)[0])).ok(t)
	remoteContext := "remote-ctx"
	ok(t, e.RemoteStarted(remote, "remote-1", &remoteContext))
	ok(t, e.RemotePolled(remote))
	h.advance(1000)
	got(e.Cancel("ext", askerCaller, extTask)).ok(t)
	h.advance(1000)
	got(e.SetTopics("asker", []any{"deploy"}, []any{"ci"})).ok(t)
	ok(t, e.SetState("helper", "busy", "compiling"))
	ok(t, e.SetOnline("helper", false))
	h.close()
}

// compatReads reads the file back like the read mode of testdata/compat.mjs.
func compatReads(t *testing.T, dir string) string {
	h := setupAt(t, dir, compatEnd, Options{})
	e := h.engine
	host := Caller{Principal: mars}
	askerCaller := Caller{Principal: mars, Agent: "asker"}
	helperTasks := got(e.ListTasks("helper", host, ListTasksInput{PageSize: 10, IncludeArtifacts: true})).ok(t)
	extTasks := got(e.ListTasks("ext", askerCaller, ListTasksInput{PageSize: 10, IncludeArtifacts: true})).ok(t)
	var tasks []any
	for _, task := range append(helperTasks.Tasks, extTasks.Tasks...) {
		tasks = append(tasks, toValue(t, got(e.ReadTask(task.ID)).ok(t)))
	}
	one := 1
	var inbox, unacked, pushConfigs []any
	for _, agent := range []string{"asker", "helper"} {
		inbox = append(inbox, toValue(t, got(e.Inbox(agent, false, 50)).ok(t)))
		unacked = append(unacked, toValue(t, got(e.Unacked(agent)).ok(t)))
	}
	for _, task := range helperTasks.Tasks {
		for _, caller := range []Caller{host, fromHermes} {
			list, err := e.ListPushConfigs("helper", caller, task.ID)
			if err != nil {
				pushConfigs = append(pushConfigs, string(reason(t, err)))
			} else {
				pushConfigs = append(pushConfigs, toValue(t, list))
			}
		}
	}
	out := jsonx.NewObject(
		"agents", toValue(t, got(e.ListAgents(true)).ok(t)),
		"tasks", tasks,
		"page", toValue(t, got(e.ListTasks("helper", host, ListTasksInput{PageSize: 1, HistoryLength: &one})).ok(t)),
		"inbox", inbox,
		"unacked", unacked,
		"thread", toValue(t, got(e.Thread("asker", "ctx-note")).ok(t)),
		"message", toValue(t, got(e.Message("asker", "m-go")).ok(t)),
		"pending", toValue(t, got(e.Pending("asker")).ok(t)),
		"owed", toValue(t, got(e.Owed("helper")).ok(t)),
		"pushConfigs", pushConfigs,
		"due", toValue(t, got(e.DuePushes(compatEnd, 50)).ok(t)),
		"jobs", toValue(t, got(e.ExternalJobs()).ok(t)),
		"remotes", toValue(t, got(e.ActiveRemotes()).ok(t)),
		"outbound", toValue(t, got(e.OutboundMessage(extTasks.Tasks[0].ID, "m-ext")).ok(t)),
	)
	h.close()
	return string(jsonx.Stringify(out))
}

// dumpRows lists every row of every table with the sqlite type of each value.
func dumpRows(t *testing.T, dir string) string {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var out strings.Builder
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(&out, "user_version %d\n", version)
	var tables []string
	rows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, name)
	}
	_ = rows.Close()
	for _, table := range tables {
		fmt.Fprintf(&out, "== %s\n", table)
		rows, err := db.QueryContext(ctx, "SELECT * FROM "+table+" ORDER BY rowid")
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := rows.Columns()
		for rows.Next() {
			values := make([]any, len(cols))
			dest := make([]any, len(cols))
			for i := range values {
				dest[i] = &values[i]
			}
			if err := rows.Scan(dest...); err != nil {
				t.Fatal(err)
			}
			for i, v := range values {
				if i > 0 {
					out.WriteString(" | ")
				}
				switch v := v.(type) {
				case string:
					fmt.Fprintf(&out, "%s=%q", cols[i], v)
				default:
					fmt.Fprintf(&out, "%s=%T(%v)", cols[i], v, v)
				}
			}
			out.WriteString("\n")
		}
		_ = rows.Close()
	}
	return out.String()
}

func copyDir(t *testing.T, from string) string {
	t.Helper()
	dir := t.TempDir()
	in, err := os.Open(filepath.Join(from, store.FileName))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = in.Close() }()
	out, err := os.Create(filepath.Join(dir, store.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(bytes.TrimSpace(data))
}

func diffText(t *testing.T, label, got, want string) {
	t.Helper()
	if got == want {
		return
	}
	gotLines, wantLines := strings.Split(got, "\n"), strings.Split(want, "\n")
	for i := range max(len(gotLines), len(wantLines)) {
		var g, w string
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if g != w {
			t.Errorf("%s: first difference at line %d\n got %s\nwant %s", label, i+1, g, w)
			return
		}
	}
}

func diffJSON(t *testing.T, label, got, want string) {
	t.Helper()
	if got == want {
		return
	}
	for i := range min(len(got), len(want)) {
		if got[i] != want[i] {
			from := max(0, i-120)
			t.Errorf("%s: first difference at byte %d\n got …%s\nwant …%s", label, i, got[from:min(len(got), i+80)], want[from:min(len(want), i+80)])
			return
		}
	}
	t.Errorf("%s: lengths differ: got %d, want %d bytes", label, len(got), len(want))
}

const compatNode = "testdata/compat-node"

func TestGoWritesTheSameRowsAsTheTypeScriptEngine(t *testing.T) {
	dir := t.TempDir()
	runCompatScenario(t, dir)
	diffText(t, "rows", dumpRows(t, dir), dumpRows(t, copyDir(t, compatNode)))
}

func TestGoReadsTheSameJSONFromBothFiles(t *testing.T) {
	want := readFile(t, filepath.Join(compatNode, "reads.json"))
	goDir := t.TempDir()
	runCompatScenario(t, goDir)
	diffJSON(t, "go engine on the go file", compatReads(t, goDir), want)
	diffJSON(t, "go engine on the node file", compatReads(t, copyDir(t, compatNode)), want)
}

func nodeCompat(t *testing.T, mode, dir string) string {
	t.Helper()
	pimon := os.Getenv("PIMON_DIR")
	if pimon == "" {
		t.Skip("PIMON_DIR is not set")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not on PATH")
	}
	cmd := exec.Command(node, "--no-warnings", filepath.Join("testdata", "compat.mjs"), mode, pimon, dir)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("compat.mjs %s: %v\n%s", mode, err, stderr.String())
	}
	return string(out)
}

// PIMON_DIR names a pimon checkout. With it and node on PATH, the TypeScript engine runs the scenario
// again and reads the file that the Go engine wrote.
func TestTypeScriptEngineReadsTheGoFile(t *testing.T) {
	want := readFile(t, filepath.Join(compatNode, "reads.json"))
	nodeDir := t.TempDir()
	nodeCompat(t, "write", nodeDir)
	diffText(t, "committed node rows", dumpRows(t, copyDir(t, compatNode)), dumpRows(t, nodeDir))
	diffJSON(t, "node engine on the node file", nodeCompat(t, "read", copyDir(t, nodeDir)), want)
	goDir := t.TempDir()
	runCompatScenario(t, goDir)
	diffJSON(t, "node engine on the go file", nodeCompat(t, "read", goDir), want)
}
