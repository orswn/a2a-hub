// Command a2a connects Claude Code and Codex sessions on a host to the a2a hub.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/orswn/a2a-hub/internal/claudeid"
	"github.com/orswn/a2a-hub/internal/codexadapter"
	"github.com/orswn/a2a-hub/internal/hostauth"
	"github.com/orswn/a2a-hub/internal/linkclient"
	"github.com/orswn/a2a-hub/internal/mcpshim"
)

// version is set at build time with -ldflags "-X main.version=<version>".
var version = "0.0.0-dev"

const usage = `usage: a2a <command> [flags]

commands:
  claude-hook      SessionStart hook of Claude Code: records the session id
  claude-headers   headersHelper of the Claude Code MCP server: prints the hub headers
  mcp              stdio MCP server that forwards to the hub /mcp endpoint
  codex            starts the Codex units if needed, then runs codex --remote
  codex-adapter    links the Codex threads of this host to the hub
  codex-server     runs the Codex app-server with the hub MCP server
  version          prints the version
`

// sys holds the process facts and side effects, so tests can fake them.
type sys struct {
	getenv  func(string) string
	stdin   io.Reader
	stdout  io.Writer
	stderr  io.Writer
	pid     int
	ppid    int
	home    string
	cwd     string
	host    string
	procDir string
	// systemctl runs systemctl --user with args and returns its exit error.
	systemctl func(args ...string) error
	// execve replaces the process.
	execve   func(path string, argv []string, env []string) error
	environ  func() []string
	lookPath func(string) (string, error)
	signals  func() (context.Context, context.CancelFunc)
	// socketWait bounds the wait of the launcher for the app-server.
	socketWait time.Duration
	// self is the path of this binary.
	self string
}

func main() {
	home, _ := os.UserHomeDir()
	cwd, _ := os.Getwd()
	host, _ := os.Hostname()
	host, _, _ = strings.Cut(host, ".")
	self, err := os.Executable()
	if err != nil {
		self = "a2a"
	}
	s := sys{
		getenv: os.Getenv, stdin: os.Stdin, stdout: os.Stdout, stderr: os.Stderr,
		pid: os.Getpid(), ppid: os.Getppid(), home: home, cwd: cwd, host: host, procDir: "/proc",
		systemctl: func(args ...string) error {
			cmd := exec.Command("systemctl", append([]string{"--user"}, args...)...) //nolint:gosec // G204: fixed unit names and verbs.
			cmd.Stderr = os.Stderr
			return cmd.Run()
		},
		execve:   syscall.Exec,
		environ:  os.Environ,
		lookPath: exec.LookPath,
		signals: func() (context.Context, context.CancelFunc) {
			return signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
		},
		socketWait: 15 * time.Second,
		self:       self,
	}
	os.Exit(run(os.Args[1:], s))
}

func run(args []string, s sys) int {
	if len(args) == 0 {
		_, _ = io.WriteString(s.stderr, usage)
		return 2
	}
	var err error
	switch args[0] {
	case "claude-hook":
		err = claudeHook(args[1:], s)
	case "claude-headers":
		err = claudeHeaders(args[1:], s)
	case "mcp":
		err = mcpCommand(args[1:], s)
	case "codex":
		err = codexLauncher(args[1:], s)
	case "codex-adapter":
		err = codexAdapter(args[1:], s)
	case "codex-server":
		err = codexServer(args[1:], s)
	case "version", "--version":
		_, _ = fmt.Fprintln(s.stdout, version)
	case "help", "--help", "-h":
		_, _ = io.WriteString(s.stdout, usage)
	default:
		_, _ = fmt.Fprintf(s.stderr, "a2a: unknown command %q\n%s", args[0], usage)
		return 2
	}
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	var usageErr usageError
	if errors.As(err, &usageErr) {
		_, _ = fmt.Fprintf(s.stderr, "a2a %s: %s\n", args[0], usageErr.text)
		return 2
	}
	if err != nil {
		_, _ = fmt.Fprintf(s.stderr, "a2a %s: %v\n", args[0], err)
		return 1
	}
	return 0
}

type usageError struct{ text string }

func (e usageError) Error() string { return e.text }

func flags(name string, s sys) *flag.FlagSet {
	set := flag.NewFlagSet("a2a "+name, flag.ContinueOnError)
	set.SetOutput(s.stderr)
	return set
}

func parse(set *flag.FlagSet, args []string) error {
	if err := set.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return usageError{err.Error()}
	}
	if set.NArg() > 0 {
		return usageError{"unexpected argument " + strconv.Quote(set.Arg(0))}
	}
	return nil
}

func (s sys) runtimeDir() (string, error) {
	dir := s.getenv("XDG_RUNTIME_DIR")
	if dir == "" {
		return "", errors.New("XDG_RUNTIME_DIR is not set")
	}
	return dir, nil
}

func (s sys) claudeEnv() (claudeid.Env, error) {
	dir, err := s.runtimeDir()
	if err != nil {
		return claudeid.Env{}, err
	}
	return claudeid.Env{RuntimeDir: dir, ProcRoot: s.procDir, Hostname: s.host, Sleep: time.Sleep, Now: time.Now}, nil
}

func (s sys) token() (string, error) {
	if s.home == "" {
		return "", errors.New("no home directory")
	}
	return hostauth.ReadToken(hostauth.TokenFile(s.home))
}

// hookPid is the Claude process of a hook: env CLAUDE_PID, else the parent. Only a hook may read
// CLAUDE_PID: Claude sets it for hooks, but in a helper it can only come from another process.
func (s sys) hookPid() (int, error) {
	if text := s.getenv("CLAUDE_PID"); text != "" {
		pid, err := strconv.Atoi(text)
		if err != nil || pid <= 0 {
			return 0, fmt.Errorf("bad CLAUDE_PID %q", text)
		}
		return pid, nil
	}
	return s.ppid, nil
}

func claudeHook(args []string, s sys) error {
	if err := parse(flags("claude-hook", s), args); err != nil {
		return err
	}
	env, err := s.claudeEnv()
	if err != nil {
		return err
	}
	pid, err := s.hookPid()
	if err != nil {
		return err
	}
	return claudeid.Hook(env, pid, s.stdin)
}

func claudeIdentity(s sys) (claudeid.Identity, error) {
	env, err := s.claudeEnv()
	if err != nil {
		return claudeid.Identity{}, err
	}
	return claudeid.Resolve(env, s.ppid)
}

func claudeHeaders(args []string, s sys) error {
	if err := parse(flags("claude-headers", s), args); err != nil {
		return err
	}
	token, err := s.token()
	if err != nil {
		return err
	}
	id, err := claudeIdentity(s)
	if err != nil {
		return err
	}
	return json.NewEncoder(s.stdout).Encode(claudeid.Headers(token, id, s.cwd))
}

func mcpCommand(args []string, s sys) error {
	set := flags("mcp", s)
	hub := set.String("hub", "", "hub URL (default: env A2A_HUB_URL, else "+hostauth.DefaultHubURL+")")
	harness := set.String("harness", "codex", "harness of the client: codex or claude-code")
	if err := parse(set, args); err != nil {
		return err
	}
	if *harness != "codex" && *harness != "claude-code" {
		return usageError{"--harness must be codex or claude-code"}
	}
	shim := &mcpshim.Shim{
		URL: hostauth.HubURL(*hub, s.getenv) + "/mcp",
		Log: func(line string) { _, _ = fmt.Fprintln(s.stderr, "a2a mcp: "+line) },
		Headers: func() (http.Header, error) {
			token, err := s.token()
			if err != nil {
				return nil, err
			}
			if *harness == "codex" {
				return http.Header{"Authorization": {"Bearer " + token}, "X-A2A-Harness": {"codex"}}, nil
			}
			id, err := claudeIdentity(s)
			if err != nil {
				return nil, err
			}
			headers := http.Header{}
			for key, value := range claudeid.Headers(token, id, s.cwd) {
				headers.Set(key, value)
			}
			return headers, nil
		},
	}
	ctx, cancel := s.signals()
	defer cancel()
	return shim.Run(ctx, s.stdin, s.stdout)
}

const (
	serverUnit  = "a2a-codex-server.service"
	adapterUnit = "a2a-codex.service"
)

func codexSocket(s sys) (string, error) {
	dir, err := s.runtimeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "a2a", "codex.sock"), nil
}

// codexLauncher starts the units that do not run, waits for the app-server socket, and runs the Codex TUI
// against it. Its arguments go to codex; "resume" and "fork" take --remote after the subcommand.
func codexLauncher(args []string, s sys) error {
	socket, err := codexSocket(s)
	if err != nil {
		return err
	}
	var start []string
	for _, unit := range []string{serverUnit, adapterUnit} {
		if s.systemctl("is-active", "--quiet", unit) != nil {
			start = append(start, unit)
		}
	}
	if len(start) > 0 {
		if err := s.systemctl(append([]string{"start"}, start...)...); err != nil {
			return fmt.Errorf("systemctl --user start %s: %w", strings.Join(start, " "), err)
		}
	}
	if err := waitSocket(socket, s.socketWait); err != nil {
		return err
	}
	codex, err := s.lookPath("codex")
	if err != nil {
		return err
	}
	return s.execve(codex, codexArgv(args, "unix://"+socket, s.cwd), s.environ())
}

// codexArgv adds --remote, and -C with the terminal cwd unless the user gave one: with --remote, a new
// thread otherwise starts in the cwd of the app-server. A resumed thread keeps its own cwd.
func codexArgv(args []string, remote, cwd string) []string {
	argv := []string{"codex"}
	if len(args) > 0 && (args[0] == "resume" || args[0] == "fork") {
		argv = append(argv, args[0])
		args = args[1:]
	}
	argv = append(argv, "--remote", remote)
	if cwd != "" && !hasCd(args) {
		argv = append(argv, "-C", cwd)
	}
	return append(argv, args...)
}

func hasCd(args []string) bool {
	for _, arg := range args {
		if arg == "--" {
			return false
		}
		if arg == "--cd" || strings.HasPrefix(arg, "--cd=") || strings.HasPrefix(arg, "-C") {
			return true
		}
	}
	return false
}

// waitSocket waits until the app-server accepts connections. A socket file alone can be left by a crash.
func waitSocket(path string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for {
		if conn, err := net.DialTimeout("unix", path, time.Second); err == nil {
			_ = conn.Close()
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no app-server answers on %s after %v; see journalctl --user -u %s", path, limit, serverUnit)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// codexServer runs the app-server on the socket with the hub as MCP server a2a. Over HTTP, the token goes
// to codex only in its environment, never on the command line. Over stdio, "a2a mcp" reads the token file.
func codexServer(args []string, s sys) error {
	set := flags("codex-server", s)
	hub := set.String("hub", "", "hub URL (default: env A2A_HUB_URL, else "+hostauth.DefaultHubURL+")")
	transport := set.String("mcp-transport", "http", "how codex reaches the hub MCP server: http, or stdio through a2a mcp")
	if err := parse(set, args); err != nil {
		return err
	}
	if *transport != "http" && *transport != "stdio" {
		return usageError{"--mcp-transport must be http or stdio"}
	}
	socket, err := codexSocket(s)
	if err != nil {
		return err
	}
	token, err := s.token()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		return err
	}
	if err := removeStaleSocket(socket); err != nil {
		return err
	}
	codex, err := s.lookPath("codex")
	if err != nil {
		return err
	}
	hubURL := hostauth.HubURL(*hub, s.getenv)
	argv := []string{"codex", "app-server", "--listen", "unix://" + socket}
	env := slices.DeleteFunc(s.environ(), func(kv string) bool { return strings.HasPrefix(kv, "A2A_HUB_TOKEN=") })
	if *transport == "stdio" {
		shimArgs, _ := json.Marshal([]string{"mcp", "--harness", "codex", "--hub", hubURL})
		argv = append(argv,
			"-c", "mcp_servers.a2a.command="+strconv.Quote(s.self),
			"-c", "mcp_servers.a2a.args="+string(shimArgs),
		)
		return s.execve(codex, argv, env)
	}
	argv = append(argv,
		"-c", "mcp_servers.a2a.url="+strconv.Quote(hubURL+"/mcp"),
		"-c", `mcp_servers.a2a.bearer_token_env_var="A2A_HUB_TOKEN"`,
		"-c", `mcp_servers.a2a.http_headers={"X-A2A-Harness"="codex"}`,
	)
	return s.execve(codex, argv, append(env, "A2A_HUB_TOKEN="+token))
}

// removeStaleSocket removes a socket file that no process listens on, such as one left by a crash.
func removeStaleSocket(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("%s exists and is not a socket", path)
	}
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err == nil {
		_ = conn.Close()
		return fmt.Errorf("an app-server already listens on %s", path)
	}
	return os.Remove(path)
}

func codexAdapter(args []string, s sys) error {
	set := flags("codex-adapter", s)
	hub := set.String("hub", "", "hub URL (default: env A2A_HUB_URL, else "+hostauth.DefaultHubURL+")")
	socket := set.String("socket", "", "app-server socket (default: $XDG_RUNTIME_DIR/a2a/codex.sock)")
	state := set.String("state", "", "state file (default: $XDG_STATE_HOME/a2a/codex-threads.json)")
	if err := parse(set, args); err != nil {
		return err
	}
	if *socket == "" {
		path, err := codexSocket(s)
		if err != nil {
			return err
		}
		*socket = path
	}
	if *state == "" {
		base := s.getenv("XDG_STATE_HOME")
		if base == "" {
			base = filepath.Join(s.home, ".local", "state")
		}
		*state = filepath.Join(base, "a2a", "codex-threads.json")
	}
	token, err := s.token()
	if err != nil {
		return err
	}
	ctx, cancel := s.signals()
	defer cancel()
	adapter := codexadapter.New(codexadapter.Options{
		Socket:    *socket,
		Link:      &linkclient.Client{BaseURL: hostauth.HubURL(*hub, s.getenv), Token: token},
		StateFile: *state,
		Version:   version,
		Log:       func(line string) { _, _ = fmt.Fprintln(s.stderr, line) },
	})
	return adapter.Run(ctx)
}
