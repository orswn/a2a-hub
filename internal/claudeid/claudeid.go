// Package claudeid gives each Claude Code process one hub agent id. The SessionStart hook and the MCP
// headersHelper share one state file per Claude pid under $XDG_RUNTIME_DIR/a2a/claude.
package claudeid

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// HookWait bounds the wait of the first headers call for the session id of the SessionStart hook. Claude
// starts the MCP connect without waiting for the hook.
const HookWait = 3 * time.Second

const pollEvery = 50 * time.Millisecond

// State is the state file of one Claude process.
type State struct {
	// Start is the start time of the process in clock ticks, field 22 of /proc/<pid>/stat. A file with another
	// start belongs to an older process with the same pid.
	Start   string `json:"start,omitempty"`
	Session string `json:"session,omitempty"`
	Source  string `json:"source,omitempty"`
	Cwd     string `json:"cwd,omitempty"`
	Agent   string `json:"agent,omitempty"`
}

// Env holds the process facts that the helpers read, so tests can fake them.
type Env struct {
	// RuntimeDir is $XDG_RUNTIME_DIR.
	RuntimeDir string
	// ProcRoot is /proc.
	ProcRoot string
	Hostname string
	// Sleep waits between polls for the hook. Tests make it advance a fake clock.
	Sleep func(time.Duration)
	Now   func() time.Time
}

func (env Env) dir() string { return filepath.Join(env.RuntimeDir, "a2a", "claude") }

func (env Env) file(pid int) string { return filepath.Join(env.dir(), strconv.Itoa(pid)+".json") }

// StartTime reads field 22 of /proc/<pid>/stat. The command name in field 2 may hold spaces and parentheses,
// so the fields are counted after its last ")".
func (env Env) StartTime(pid int) (string, error) {
	raw, err := os.ReadFile(filepath.Join(env.ProcRoot, strconv.Itoa(pid), "stat"))
	if err != nil {
		return "", err
	}
	text := string(raw)
	end := strings.LastIndexByte(text, ')')
	if end < 0 {
		return "", fmt.Errorf("bad stat of pid %d", pid)
	}
	fields := strings.Fields(text[end+1:])
	// fields[0] is field 3.
	if len(fields) < 20 {
		return "", fmt.Errorf("bad stat of pid %d", pid)
	}
	return fields[19], nil
}

// update runs fn on the state of pid under an exclusive lock and writes the result to a temp file, then
// renames it. The lock keeps a hook and a headers call from losing each other's change.
func (env Env) update(pid int, fn func(*State) error) (State, error) {
	if err := os.MkdirAll(env.dir(), 0o700); err != nil {
		return State{}, err
	}
	lock, err := os.OpenFile(filepath.Join(env.dir(), strconv.Itoa(pid)+".lock"), os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return State{}, err
	}
	defer func() { _ = lock.Close() }()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return State{}, err
	}
	var state State
	if raw, err := os.ReadFile(env.file(pid)); err == nil {
		// A corrupt file counts as empty.
		_ = json.Unmarshal(raw, &state)
	} else if !errors.Is(err, os.ErrNotExist) {
		return State{}, err
	}
	if err := fn(&state); err != nil {
		return State{}, err
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return State{}, err
	}
	temp, err := os.CreateTemp(env.dir(), strconv.Itoa(pid)+".*.tmp")
	if err != nil {
		return State{}, err
	}
	_, werr := temp.Write(raw)
	cerr := temp.Close()
	if werr != nil || cerr != nil {
		_ = os.Remove(temp.Name())
		return State{}, errors.Join(werr, cerr)
	}
	if err := os.Rename(temp.Name(), env.file(pid)); err != nil {
		_ = os.Remove(temp.Name())
		return State{}, err
	}
	return state, nil
}

func (env Env) read(pid int) State {
	var state State
	if raw, err := os.ReadFile(env.file(pid)); err == nil {
		_ = json.Unmarshal(raw, &state)
	}
	return state
}

// HookInput is the part of the SessionStart hook stdin that the hook reads.
type HookInput struct {
	SessionID string `json:"session_id"`
	Source    string `json:"source"`
	Cwd       string `json:"cwd"`
}

// Hook records the session id of the Claude process pid. It keeps the agent id of the process, so a /clear
// does not change the agent. It also removes the files of processes that ended.
func Hook(env Env, pid int, stdin io.Reader) error {
	var input HookInput
	if err := json.NewDecoder(io.LimitReader(stdin, 1<<20)).Decode(&input); err != nil {
		return fmt.Errorf("bad hook input: %w", err)
	}
	if input.SessionID == "" {
		return errors.New("bad hook input: no session_id")
	}
	start, err := env.StartTime(pid)
	if err != nil {
		return fmt.Errorf("claude pid %d: %w", pid, err)
	}
	_, err = env.update(pid, func(state *State) error {
		if state.Start != start {
			*state = State{Start: start}
		}
		state.Session = input.SessionID
		state.Source = input.Source
		if input.Cwd != "" {
			state.Cwd = input.Cwd
		}
		return nil
	})
	if err != nil {
		return err
	}
	env.prune(pid)
	return nil
}

// prune removes the state of pids that no longer run. Errors do not matter: the next hook tries again.
func (env Env) prune(keep int) {
	entries, err := os.ReadDir(env.dir())
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		base, _, _ := strings.Cut(name, ".")
		pid, err := strconv.Atoi(base)
		if err != nil || pid == keep {
			continue
		}
		if _, err := os.Stat(filepath.Join(env.ProcRoot, base)); errors.Is(err, os.ErrNotExist) {
			_ = os.Remove(filepath.Join(env.dir(), name))
		}
	}
}

// Identity is what the headers helper sends.
type Identity struct {
	Agent   string
	Session string
	Cwd     string
}

// Resolve gives the agent id of the Claude process pid. The first call picks it: the session id of the hook
// when the hook writes it within HookWait, else claude-<host>-<pid>-<start>. Later calls, such as reconnects,
// reuse it.
func Resolve(env Env, pid int) (Identity, error) {
	start, err := env.StartTime(pid)
	if err != nil {
		return Identity{}, fmt.Errorf("claude pid %d: %w", pid, err)
	}
	deadline := env.Now().Add(HookWait)
	for {
		current := env.read(pid)
		if current.Start == start && (current.Agent != "" || current.Session != "") {
			break
		}
		if !env.Now().Before(deadline) {
			break
		}
		env.Sleep(pollEvery)
	}
	state, err := env.update(pid, func(state *State) error {
		if state.Start != start {
			*state = State{Start: start}
		}
		if state.Agent == "" {
			state.Agent = state.Session
		}
		if state.Agent == "" {
			state.Agent = fmt.Sprintf("claude-%s-%d-%s", env.Hostname, pid, start)
		}
		return nil
	})
	if err != nil {
		return Identity{}, err
	}
	return Identity{Agent: state.Agent, Session: state.Session, Cwd: state.Cwd}, nil
}

// Headers is the headersHelper output. Empty values are left out. The session and cwd labels are
// percent-encoded, because a header value cannot carry every byte of a path; the hub decodes them.
func Headers(token string, id Identity, cwd string) map[string]string {
	headers := map[string]string{
		"Authorization": "Bearer " + token,
		"X-A2A-Harness": "claude-code",
		"X-A2A-Agent":   id.Agent,
	}
	if id.Session != "" {
		headers["X-A2A-Session"] = url.PathEscape(id.Session)
	}
	if id.Cwd != "" {
		cwd = id.Cwd
	}
	if cwd != "" {
		headers["X-A2A-Cwd"] = url.PathEscape(cwd)
	}
	return headers
}
