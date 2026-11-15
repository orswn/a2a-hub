package claudeid

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const pid = 4242

type fakeEnv struct {
	Env
	mu  sync.Mutex
	now time.Time
	// onSleep runs at each poll, after the fake clock moved.
	onSleep func()
}

func newEnv(t *testing.T) *fakeEnv {
	t.Helper()
	root := t.TempDir()
	f := &fakeEnv{now: time.Unix(1_800_000_000, 0)}
	f.Env = Env{
		RuntimeDir: filepath.Join(root, "run"),
		ProcRoot:   filepath.Join(root, "proc"),
		Hostname:   "mars",
		Now: func() time.Time {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.now
		},
		Sleep: func(d time.Duration) {
			f.mu.Lock()
			f.now = f.now.Add(d)
			hook := f.onSleep
			f.mu.Unlock()
			if hook != nil {
				hook()
			}
		},
	}
	f.setStart(t, pid, "777")
	return f
}

// setStart writes a fake /proc/<pid>/stat whose command name holds a space and a parenthesis.
func (f *fakeEnv) setStart(t *testing.T, p int, start string) {
	t.Helper()
	dir := filepath.Join(f.ProcRoot, fmt.Sprint(p))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	fields := make([]string, 50)
	for i := range fields {
		fields[i] = "0"
	}
	fields[18] = start
	stat := fmt.Sprintf("%d (cl aude)) S %s\n", p, strings.Join(fields, " "))
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(stat), 0o644); err != nil {
		t.Fatal(err)
	}
}

func hookInput(session, source string) *strings.Reader {
	raw, _ := json.Marshal(map[string]string{"session_id": session, "source": source, "cwd": "/work", "hook_event_name": "SessionStart"})
	return strings.NewReader(string(raw))
}

func TestStartTimeReadsField22AfterTheCommandName(t *testing.T) {
	f := newEnv(t)
	start, err := f.StartTime(pid)
	if err != nil || start != "777" {
		t.Fatalf("StartTime = %q, %v", start, err)
	}
}

func TestTheHookSessionBecomesTheAgentWhenTheHookRunsFirst(t *testing.T) {
	f := newEnv(t)
	if err := Hook(f.Env, pid, hookInput("sess-1", "startup")); err != nil {
		t.Fatal(err)
	}
	before := f.Now()
	id, err := Resolve(f.Env, pid)
	if err != nil {
		t.Fatal(err)
	}
	if id != (Identity{Agent: "sess-1", Session: "sess-1", Cwd: "/work"}) {
		t.Fatalf("identity = %+v", id)
	}
	if f.Now() != before {
		t.Fatal("Resolve waited although the hook had run")
	}
}

func TestTheFirstHeadersCallWaitsForAHookThatRunsLate(t *testing.T) {
	f := newEnv(t)
	polls := 0
	f.onSleep = func() {
		polls++
		if polls == 10 {
			if err := Hook(f.Env, pid, hookInput("sess-late", "startup")); err != nil {
				t.Error(err)
			}
		}
	}
	id, err := Resolve(f.Env, pid)
	if err != nil {
		t.Fatal(err)
	}
	if id.Agent != "sess-late" || polls != 10 {
		t.Fatalf("identity = %+v after %d polls", id, polls)
	}
}

func TestWithoutAHookTheAgentIsThePidAndStartAfterThreeSeconds(t *testing.T) {
	f := newEnv(t)
	start := f.Now()
	id, err := Resolve(f.Env, pid)
	if err != nil {
		t.Fatal(err)
	}
	if id.Agent != "claude-mars-4242-777" || id.Session != "" {
		t.Fatalf("identity = %+v", id)
	}
	if waited := f.Now().Sub(start); waited != HookWait {
		t.Fatalf("waited %v, want %v", waited, HookWait)
	}
	// A hook that runs after the choice changes only the session label.
	if err := Hook(f.Env, pid, hookInput("sess-2", "startup")); err != nil {
		t.Fatal(err)
	}
	again, err := Resolve(f.Env, pid)
	if err != nil {
		t.Fatal(err)
	}
	if again.Agent != "claude-mars-4242-777" || again.Session != "sess-2" {
		t.Fatalf("after hook: %+v", again)
	}
}

func TestClearKeepsTheAgentAndAReconnectSendsTheNewSession(t *testing.T) {
	f := newEnv(t)
	if err := Hook(f.Env, pid, hookInput("sess-1", "startup")); err != nil {
		t.Fatal(err)
	}
	if _, err := Resolve(f.Env, pid); err != nil {
		t.Fatal(err)
	}
	if err := Hook(f.Env, pid, hookInput("sess-after-clear", "clear")); err != nil {
		t.Fatal(err)
	}
	id, err := Resolve(f.Env, pid)
	if err != nil {
		t.Fatal(err)
	}
	if id.Agent != "sess-1" || id.Session != "sess-after-clear" {
		t.Fatalf("identity = %+v", id)
	}
	headers := Headers("tok", id, "/elsewhere")
	want := map[string]string{
		"Authorization": "Bearer tok", "X-A2A-Harness": "claude-code", "X-A2A-Agent": "sess-1",
		"X-A2A-Session": "sess-after-clear", "X-A2A-Cwd": "%2Fwork",
	}
	if fmt.Sprint(headers) != fmt.Sprint(want) {
		t.Fatalf("headers = %v", headers)
	}
}

func TestAFileOfAnOlderProcessWithTheSamePidIsReplaced(t *testing.T) {
	f := newEnv(t)
	if err := Hook(f.Env, pid, hookInput("old-session", "startup")); err != nil {
		t.Fatal(err)
	}
	if _, err := Resolve(f.Env, pid); err != nil {
		t.Fatal(err)
	}
	f.setStart(t, pid, "999")
	id, err := Resolve(f.Env, pid)
	if err != nil {
		t.Fatal(err)
	}
	if id.Agent != "claude-mars-4242-999" || id.Session != "" {
		t.Fatalf("identity = %+v", id)
	}
}

func TestAResumeInANewProcessMapsToTheResumedSession(t *testing.T) {
	f := newEnv(t)
	f.setStart(t, 5000, "1")
	if err := Hook(f.Env, 5000, hookInput("sess-1", "resume")); err != nil {
		t.Fatal(err)
	}
	id, err := Resolve(f.Env, 5000)
	if err != nil || id.Agent != "sess-1" {
		t.Fatalf("identity = %+v, %v", id, err)
	}
}

func TestTheHookRemovesTheFilesOfEndedProcesses(t *testing.T) {
	f := newEnv(t)
	f.setStart(t, 100, "5")
	if err := Hook(f.Env, 100, hookInput("gone", "startup")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(f.ProcRoot, "100")); err != nil {
		t.Fatal(err)
	}
	if err := Hook(f.Env, pid, hookInput("live", "startup")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(f.RuntimeDir, "a2a", "claude", "100.json")); !os.IsNotExist(err) {
		t.Fatalf("stale file still there: %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.RuntimeDir, "a2a", "claude", fmt.Sprint(pid)+".json")); err != nil {
		t.Fatalf("live file: %v", err)
	}
}

func TestStateFilesArePrivate(t *testing.T) {
	f := newEnv(t)
	if err := Hook(f.Env, pid, hookInput("s", "startup")); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(f.RuntimeDir, "a2a", "claude")
	for _, path := range []string{dir, filepath.Join(dir, fmt.Sprint(pid)+".json")} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Fatalf("%s mode %v", path, info.Mode())
		}
	}
}

func TestConcurrentHookAndHeadersKeepBothChanges(t *testing.T) {
	for round := range 20 {
		f := newEnv(t)
		f.Sleep = func(time.Duration) {}
		f.Now = func() time.Time { return time.Unix(0, 0) }
		var wg sync.WaitGroup
		wg.Add(2)
		var id Identity
		go func() {
			defer wg.Done()
			id, _ = Resolve(f.Env, pid)
		}()
		go func() {
			defer wg.Done()
			_ = Hook(f.Env, pid, hookInput("sess", "startup"))
		}()
		wg.Wait()
		final := f.read(pid)
		if final.Session != "sess" || final.Agent == "" || final.Agent != id.Agent {
			t.Fatalf("round %d: state %+v, resolved %+v", round, final, id)
		}
	}
}

func TestBadHookInputIsRefused(t *testing.T) {
	f := newEnv(t)
	if err := Hook(f.Env, pid, strings.NewReader("{}")); err == nil {
		t.Fatal("no error for a hook input without session_id")
	}
	if err := Hook(f.Env, pid, strings.NewReader("nope")); err == nil {
		t.Fatal("no error for bad JSON")
	}
}

func TestHeaderLabelsArePercentEncoded(t *testing.T) {
	cwd := "/home/me/my dir/100%41/日本"
	headers := Headers("tok", Identity{Agent: "a", Session: "s ä"}, cwd)
	for _, name := range []string{"X-A2A-Cwd", "X-A2A-Session"} {
		for _, r := range headers[name] {
			if r < 0x21 || r > 0x7e {
				t.Fatalf("%s = %q has a byte that needs encoding", name, headers[name])
			}
		}
	}
	if got, err := url.PathUnescape(headers["X-A2A-Cwd"]); err != nil || got != cwd {
		t.Fatalf("decoded cwd = %q, %v", got, err)
	}
	if got, err := url.PathUnescape(headers["X-A2A-Session"]); err != nil || got != "s ä" {
		t.Fatalf("decoded session = %q, %v", got, err)
	}
}
