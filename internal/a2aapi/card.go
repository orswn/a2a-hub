package a2aapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/jsonx"
)

// Builtin is an agent that the hub runs itself, such as the test agents.
type Builtin interface {
	ID() string
	Name() string
	Description() string
	// Reply gives a direct message answer to a message without a task. nil makes a task.
	Reply(message *jsonx.Object, contextID string) *jsonx.Object
	// Start drives a task that was made or continued. It must not block.
	Start(e *engine.Engine, taskID string, message *jsonx.Object)
}

// ExternalCard is the card content of an external agent: the configured description and the skills of the
// last card that the agent served.
type ExternalCard struct {
	Description string
	Skills      []*jsonx.Object
}

// CardInput is what a card is made of.
type CardInput struct {
	Agent    *engine.AgentRow
	BaseURL  string
	Version  string
	Builtin  Builtin
	External *ExternalCard
}

func skillsFor(agent *engine.AgentRow, description string, builtin Builtin, external *ExternalCard) []any {
	if builtin != nil {
		return []any{jsonx.NewObject("id", builtin.ID(), "name", builtin.Name(), "description", description, "tags", []any{"test"})}
	}
	if agent.Kind != engine.KindExternal {
		// The skill id stays as protocol 2 named it, so a client that keys on it keeps working.
		return []any{jsonx.NewObject("id", "pi-session", "name", sessionName(agent), "description", description, "tags", []any{"coding"})}
	}
	if external != nil && len(external.Skills) > 0 {
		skills := make([]any, len(external.Skills))
		for i, skill := range external.Skills {
			skills[i] = skill
		}
		return skills
	}
	return []any{jsonx.NewObject("id", "external", "name", agent.Name, "description", description, "tags", []any{"external"})}
}

// sessionName is "session", after the harness label when the agent has one, such as "codex session".
func sessionName(agent *engine.AgentRow) string {
	var labels map[string]any
	_ = json.Unmarshal([]byte(agent.Labels), &labels)
	if harness, _ := labels[engine.HarnessLabel].(string); harness != "" {
		return harness + " session"
	}
	return "session"
}

// BuildCard makes the card in A2A 1.0 JSON form. It has static content only: no cwd, status, or label other
// than the harness.
func BuildCard(in CardInput) *jsonx.Object {
	agent := in.Agent
	var description string
	switch {
	case in.Builtin != nil:
		description = in.Builtin.Description()
	case agent.Kind == engine.KindExternal && in.External != nil:
		description = in.External.Description
	case agent.Kind == engine.KindExternal:
		description = agent.Name + ": an external A2A agent"
	default:
		host := "unknown host"
		if agent.Host != nil {
			host = *agent.Host
		}
		description = sessionName(agent) + " on " + host
	}
	card := jsonx.NewObject(
		"name", agent.Name,
		"description", description,
		"supportedInterfaces", []any{jsonx.NewObject(
			"url", in.BaseURL+"/a2a/"+agent.ID, "protocolBinding", "JSONRPC", "protocolVersion", "1.0")},
		"version", in.Version,
		"capabilities", jsonx.NewObject(
			"streaming", true,
			"pushNotifications", true,
			"extendedAgentCard", false,
			"extensions", cardExtensions(),
		),
		"defaultInputModes", []any{"text/plain"},
		"defaultOutputModes", []any{"text/plain"},
		"skills", skillsFor(agent, description, in.Builtin, in.External),
	)
	if in.Builtin == nil {
		card.Set("securitySchemes", jsonx.NewObject("bearer",
			jsonx.NewObject("httpAuthSecurityScheme", jsonx.NewObject("scheme", "bearer"))))
		card.Set("securityRequirements", []any{jsonx.NewObject("schemes",
			jsonx.NewObject("bearer", jsonx.NewObject("list", []any{})))})
	}
	return card
}

// cardExtensions lists both bus extension URIs, because the hub activates the one that a request names.
func cardExtensions() []any {
	out := make([]any, len(engine.BusExtensionURIs))
	for i, uri := range engine.BusExtensionURIs {
		out[i] = jsonx.NewObject("uri", uri, "required", false)
	}
	return out
}

// CardEtag is the ETag of the card JSON: the first 32 hex digits of its sha256, quoted.
func CardEtag(cardJSON []byte) string {
	sum := sha256.Sum256(cardJSON)
	return `"` + hex.EncodeToString(sum[:])[:32] + `"`
}
