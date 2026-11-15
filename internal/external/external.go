// Package external reads the file of external A2A agents, such as Hermes, that the hub sends tasks to.
package external

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/orswn/a2a-hub/internal/jsonx"
	"github.com/orswn/a2a-hub/internal/pushguard"
)

// Agent is one external agent. The hub sends it tasks and maps its answers back.
type Agent struct {
	// ID is the hub agent id, /a2a/<id>, and the key in the file.
	ID string
	// Name is what @<name> resolves to before any session agent.
	Name string
	// URL is the JSON-RPC URL of the agent. Its card is at <url>/.well-known/agent-card.json.
	URL         string
	Token       string
	Description string
	// Poll is the GetTask interval after a dropped stream and the card retry interval after a failed fetch.
	Poll    time.Duration
	CardTTL time.Duration
	// ReplyTimeout is the longest time from dispatch to a final answer.
	ReplyTimeout time.Duration
	// Idle is how long a stream may stay without a byte before it counts as dropped.
	Idle time.Duration
	// Retry is the wait after a rate-limit answer.
	Retry time.Duration
	// Offline is how long the hub retries an agent it cannot connect to before the task fails.
	Offline time.Duration
	// Webhook is where the hub posts the replies to the MCP sends of the agent. nil means inbox only.
	Webhook *Webhook
}

// Webhook is a signed webhook route of an external agent.
type Webhook struct {
	URL string
	// Secret is the HMAC key, used as its UTF-8 text.
	Secret string
}

// Defaults of the optional fields, in milliseconds as in the file.
var Defaults = map[string]int64{
	"pollMs":         10_000,
	"cardTtlMs":      600_000,
	"replyTimeoutMs": 960_000,
	"idleMs":         30_000,
	"retryMs":        60_000,
	"offlineMs":      600_000,
}

var (
	agentID      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	visibleASCII = regexp.MustCompile(`^[\x21-\x7E]+$`)
)

const agentIDSource = `^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`

func known(field string) bool {
	switch field {
	case "name", "url", "token", "description", "webhook":
		return true
	}
	_, ok := Defaults[field]
	return ok
}

func positive(entry *jsonx.Object, id, field string) (time.Duration, error) {
	value, ok := entry.Get(field)
	if !ok {
		return time.Duration(Defaults[field]) * time.Millisecond, nil
	}
	n, isNumber := value.(float64)
	if !isNumber || n != math.Trunc(n) || math.IsInf(n, 0) || n <= 0 {
		return 0, fmt.Errorf("external agent %s: %s must be a positive integer", id, field)
	}
	if n >= float64(math.MaxInt64/int64(time.Millisecond)) {
		return time.Duration(math.MaxInt64), nil
	}
	return time.Duration(n) * time.Millisecond, nil
}

// text gives the trimmed string of a field. ok is false when the field is optional and missing.
func text(entry *jsonx.Object, id, field string, required bool) (string, bool, error) {
	value, present := entry.Get(field)
	if !present && !required {
		return "", false, nil
	}
	s, isString := value.(string)
	if trimmed := strings.TrimFunc(s, isJSSpace); isString && trimmed != "" {
		return trimmed, true, nil
	}
	return "", false, fmt.Errorf("external agent %s: %s must be a non-empty string", id, field)
}

// Parse reads the text of an external agents file. No error text quotes the file, which holds plain tokens.
func Parse(source []byte) ([]Agent, error) {
	raw, err := jsonx.Parse(source)
	if err != nil {
		return nil, errors.New("external file is not valid JSON")
	}
	file, ok := raw.(*jsonx.Object)
	if !ok {
		return nil, errors.New("external file must be a JSON object")
	}
	agents := []Agent{}
	names := map[string]bool{}
	for _, id := range file.Keys() {
		if !agentID.MatchString(id) {
			return nil, fmt.Errorf("external agent id %s has invalid characters", id)
		}
		entry, ok := file.Value(id).(*jsonx.Object)
		if !ok {
			return nil, fmt.Errorf("external agent %s: entry must be an object", id)
		}
		for _, field := range entry.Keys() {
			if !known(field) {
				return nil, fmt.Errorf("external agent %s: unknown field %s", id, field)
			}
		}
		rawURL, _, err := text(entry, id, "url", true)
		if err != nil {
			return nil, err
		}
		parsed, err := pushguard.ParseURL(rawURL)
		if err != nil {
			return nil, fmt.Errorf("external agent %s: url is not a valid URL", id)
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return nil, fmt.Errorf("external agent %s: url must use http or https", id)
		}
		name, set, err := text(entry, id, "name", false)
		if err != nil {
			return nil, err
		}
		if !set {
			name = id
		}
		if !agentID.MatchString(name) {
			return nil, fmt.Errorf("external agent %s: name must match %s", id, agentIDSource)
		}
		token, isString := entry.Value("token").(string)
		if !isString || !visibleASCII.MatchString(token) {
			return nil, fmt.Errorf("external agent %s: token must be visible ASCII", id)
		}
		if names[name] {
			return nil, fmt.Errorf("external agent %s: name %s is used twice", id, name)
		}
		names[name] = true
		description, set, err := text(entry, id, "description", false)
		if err != nil {
			return nil, err
		}
		if !set {
			description = name + ": an external A2A agent"
		}
		agent := Agent{ID: id, Name: name, URL: rawURL, Token: token, Description: description}
		for _, field := range []struct {
			name string
			to   *time.Duration
		}{
			{"pollMs", &agent.Poll}, {"cardTtlMs", &agent.CardTTL}, {"replyTimeoutMs", &agent.ReplyTimeout},
			{"idleMs", &agent.Idle}, {"retryMs", &agent.Retry}, {"offlineMs", &agent.Offline},
		} {
			if *field.to, err = positive(entry, id, field.name); err != nil {
				return nil, err
			}
		}
		if raw, present := entry.Get("webhook"); present {
			if agent.Webhook, err = webhook(raw, id); err != nil {
				return nil, err
			}
		}
		agents = append(agents, agent)
	}
	return agents, nil
}

func webhook(raw any, id string) (*Webhook, error) {
	spec, ok := raw.(*jsonx.Object)
	if !ok {
		return nil, fmt.Errorf("external agent %s: webhook must be an object", id)
	}
	for _, field := range spec.Keys() {
		if field != "url" && field != "secret" {
			return nil, fmt.Errorf("external agent %s: unknown webhook field %s", id, field)
		}
	}
	rawURL, _, err := text(spec, id, "url", true)
	if err != nil {
		return nil, fmt.Errorf("external agent %s: webhook url must be a non-empty string", id)
	}
	parsed, err := pushguard.ParseURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("external agent %s: webhook url is not a valid URL", id)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("external agent %s: webhook url must use http or https", id)
	}
	secret, isString := spec.Value("secret").(string)
	if !isString || strings.TrimFunc(secret, isJSSpace) == "" {
		return nil, fmt.Errorf("external agent %s: webhook secret must be a non-empty string", id)
	}
	return &Webhook{URL: rawURL, Secret: secret}, nil
}

// Load reads an external agents file. An empty path gives no agents.
func Load(path string) ([]Agent, error) {
	if path == "" {
		return []Agent{}, nil
	}
	source, err := os.ReadFile(path) //nolint:gosec // G304: the operator gives the external file path.
	if err != nil {
		return nil, err
	}
	return Parse(source)
}

// CardURL is <url>/.well-known/agent-card.json, also when the URL has no trailing slash.
func CardURL(raw string) string {
	if !strings.HasSuffix(raw, "/") {
		raw += "/"
	}
	base, err := url.Parse(raw)
	if err != nil {
		return raw + ".well-known/agent-card.json"
	}
	return base.ResolveReference(&url.URL{Path: ".well-known/agent-card.json"}).String()
}

func isJSSpace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0xa0, 0x1680, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000, 0xfeff:
		return true
	}
	return r >= 0x2000 && r <= 0x200a || unicode.Is(unicode.Zs, r)
}
