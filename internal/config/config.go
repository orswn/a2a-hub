// Package config reads the hub settings from command line flags and HUB_* environment variables.
package config

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Config holds the hub settings. An empty file or URL means not set.
type Config struct {
	Host string
	Port int
	// Data is a data directory, or ":memory:".
	Data       string
	TokensFile string
	// ExternalFile lists external A2A agents, such as Hermes, that the hub sends tasks to.
	ExternalFile  string
	TestAgents    bool
	PublicURL     string
	PresenceGrace time.Duration
	Heartbeat     time.Duration
	// PushAllow lists host names, addresses, and CIDRs that push webhooks may reach although they are private.
	PushAllow []string
}

// Env looks up an environment variable, like os.LookupEnv.
type Env func(name string) (string, bool)

// Error is a bad setting value. The hub exits with status 2 for it.
type Error struct{ msg string }

func (e *Error) Error() string { return e.msg }

func errorf(format string, args ...any) error { return &Error{msg: fmt.Sprintf(format, args...)} }

// ArgError is a command line that does not parse, such as an unknown flag. The hub exits with status 1 for it.
type ArgError struct{ msg string }

func (e *ArgError) Error() string { return e.msg }

// TestAgentAllow is added to the push allow list in test mode: the TCK and test webhooks listen on loopback.
var TestAgentAllow = []string{"127.0.0.0/8", "::1"}

type option struct {
	boolean bool
}

var options = map[string]option{
	"listen":      {},
	"data":        {},
	"tokens":      {},
	"external":    {},
	"test-agents": {boolean: true},
	"public-url":  {},
	"push-allow":  {},
}

// parseArgs accepts the same command lines as node:util parseArgs in strict mode with no positionals.
func parseArgs(args []string) (map[string]string, error) {
	values := map[string]string{}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			if i+1 < len(args) {
				return nil, &ArgError{fmt.Sprintf("unexpected argument %q: this command takes no positional arguments", args[i+1])}
			}
			break
		}
		if !strings.HasPrefix(arg, "-") || arg == "-" {
			return nil, &ArgError{fmt.Sprintf("unexpected argument %q: this command takes no positional arguments", arg)}
		}
		if !strings.HasPrefix(arg, "--") {
			return nil, &ArgError{fmt.Sprintf("unknown option %q", arg)}
		}
		name, inline, hasInline := strings.Cut(arg[2:], "=")
		opt, known := options[name]
		if !known {
			return nil, &ArgError{fmt.Sprintf("unknown option %q", "--"+name)}
		}
		if opt.boolean {
			if hasInline {
				return nil, &ArgError{fmt.Sprintf("option --%s does not take an argument", name)}
			}
			values[name] = "true"
			continue
		}
		if hasInline {
			values[name] = inline
			continue
		}
		if i+1 >= len(args) {
			return nil, &ArgError{fmt.Sprintf("option --%s <value> argument missing", name)}
		}
		next := args[i+1]
		if len(next) > 1 && next[0] == '-' {
			return nil, &ArgError{fmt.Sprintf("option --%s argument is ambiguous: %q looks like an option", name, next)}
		}
		values[name] = next
		i++
	}
	return values, nil
}

// Load reads the flags in args and the HUB_* variables in env. A flag wins over its variable.
func Load(args []string, env Env) (Config, error) {
	values, err := parseArgs(args)
	if err != nil {
		return Config{}, err
	}
	pick := func(flag, variable, fallback string) string {
		if value, ok := values[flag]; ok {
			return value
		}
		if value, ok := env(variable); ok {
			return value
		}
		return fallback
	}
	pickFile := func(flag, variable string) string {
		if value, ok := values[flag]; ok {
			return value
		}
		value, _ := env(variable)
		return value
	}

	var cfg Config
	cfg.Host, cfg.Port, err = ParseListen(pick("listen", "HUB_LISTEN", "0.0.0.0:8080"))
	if err != nil {
		return Config{}, err
	}
	publicURL := pick("public-url", "HUB_PUBLIC_URL", "")
	if publicURL != "" && !strings.HasPrefix(publicURL, "http://") && !strings.HasPrefix(publicURL, "https://") {
		return Config{}, errorf("--public-url must start with http:// or https://")
	}
	testAgentsEnv, _ := env("HUB_TEST_AGENTS")
	cfg.TestAgents = values["test-agents"] == "true" || testAgentsEnv == "1"
	pushAllowText, _ := env("HUB_PUSH_ALLOW")
	if value, ok := values["push-allow"]; ok {
		pushAllowText = value
	}
	cfg.PushAllow, err = ParsePushAllow(pushAllowText)
	if err != nil {
		return Config{}, &Error{msg: err.Error()}
	}
	if cfg.TestAgents {
		cfg.PushAllow = append(cfg.PushAllow, TestAgentAllow...)
	}
	cfg.Data = pick("data", "HUB_DATA", "/data")
	cfg.TokensFile = pickFile("tokens", "HUB_TOKENS_FILE")
	cfg.ExternalFile = pickFile("external", "HUB_EXTERNAL_FILE")
	cfg.PublicURL = strings.TrimRight(publicURL, "/")
	if cfg.PresenceGrace, err = positiveMillis(env, "HUB_PRESENCE_GRACE_MS", 20_000); err != nil {
		return Config{}, err
	}
	if cfg.Heartbeat, err = positiveMillis(env, "HUB_HEARTBEAT_MS", 15_000); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func positiveMillis(env Env, name string, fallback int) (time.Duration, error) {
	value, _ := env(name)
	if value == "" {
		return time.Duration(fallback) * time.Millisecond, nil
	}
	n, ok := jsNumber(value)
	if !ok || n != math.Trunc(n) || n <= 0 || n > float64(math.MaxInt64/int64(time.Millisecond)) {
		return 0, errorf("%s must be a positive integer", name)
	}
	return time.Duration(n) * time.Millisecond, nil
}

var bracketed = regexp.MustCompile(`^\[(.*)\]$`)

// ParseListen splits host:port. An IPv6 host may be in brackets.
func ParseListen(value string) (string, int, error) {
	colon := strings.LastIndex(value, ":")
	if colon < 0 {
		return "", 0, errorf("--listen must be host:port, got %s", value)
	}
	host := bracketed.ReplaceAllString(value[:colon], "$1")
	port, ok := jsNumber(value[colon+1:])
	if host == "" || !ok || port != math.Trunc(port) || port < 0 || port > 65535 {
		return "", 0, errorf("--listen must be host:port, got %s", value)
	}
	return host, int(port), nil
}

var (
	jsDecimal = regexp.MustCompile(`^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$`)
	jsRadix   = regexp.MustCompile(`^0([xXoObB])([0-9a-fA-F]+)$`)
)

// jsNumber converts a string like the JavaScript Number function. false means NaN.
func jsNumber(text string) (float64, bool) {
	text = strings.TrimFunc(text, isJSSpace)
	switch text {
	case "":
		return 0, true
	case "Infinity", "+Infinity":
		return math.Inf(1), true
	case "-Infinity":
		return math.Inf(-1), true
	}
	if match := jsRadix.FindStringSubmatch(text); match != nil {
		base := map[byte]int{'x': 16, 'o': 8, 'b': 2}[strings.ToLower(match[1])[0]]
		n, err := strconv.ParseUint(match[2], base, 64)
		return float64(n), err == nil
	}
	if !jsDecimal.MatchString(text) {
		return 0, false
	}
	n, err := strconv.ParseFloat(text, 64)
	return n, err == nil || errors.Is(err, strconv.ErrRange)
}

// isJSSpace matches the characters that JavaScript String.prototype.trim removes.
func isJSSpace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0xa0, 0x1680, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000, 0xfeff:
		return true
	}
	return r >= 0x2000 && r <= 0x200a
}
