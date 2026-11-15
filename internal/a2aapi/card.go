package a2aapi

import (
	"crypto/sha256"
	"encoding/hex"

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

// CardInput is what a card is made of.
type CardInput struct {
	Agent   *engine.AgentRow
	BaseURL string
	Version string
	Builtin Builtin
}

func skillsFor(agent *engine.AgentRow, description string, builtin Builtin) []any {
	if builtin != nil {
		return []any{jsonx.NewObject("id", builtin.ID(), "name", builtin.Name(), "description", description, "tags", []any{"test"})}
	}
	if agent.Kind != "external" {
		return []any{jsonx.NewObject("id", "pi-session", "name", "pi session", "description", description, "tags", []any{"coding"})}
	}
	// FIXME(okka): use the skills of the external agent's own card once the gateway reads it.
	return []any{jsonx.NewObject("id", "external", "name", agent.Name, "description", description, "tags", []any{"external"})}
}

// BuildCard makes the card in A2A 1.0 JSON form. It has static content only: no cwd, labels, or status.
func BuildCard(in CardInput) *jsonx.Object {
	agent := in.Agent
	var description string
	switch {
	case in.Builtin != nil:
		description = in.Builtin.Description()
	case agent.Kind == "external":
		// FIXME(okka): use the configured description of the external agent once the gateway exists.
		description = agent.Name + ": an external A2A agent"
	default:
		host := "unknown host"
		if agent.Host != nil {
			host = *agent.Host
		}
		description = "pi session on " + host
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
			"extensions", []any{jsonx.NewObject("uri", engine.BusExtensionURI, "required", false)},
		),
		"defaultInputModes", []any{"text/plain"},
		"defaultOutputModes", []any{"text/plain"},
		"skills", skillsFor(agent, description, in.Builtin),
	)
	if in.Builtin == nil {
		card.Set("securitySchemes", jsonx.NewObject("bearer",
			jsonx.NewObject("httpAuthSecurityScheme", jsonx.NewObject("scheme", "bearer"))))
		card.Set("securityRequirements", []any{jsonx.NewObject("schemes",
			jsonx.NewObject("bearer", jsonx.NewObject("list", []any{})))})
	}
	return card
}

// CardEtag is the ETag of the card JSON: the first 32 hex digits of its sha256, quoted.
func CardEtag(cardJSON []byte) string {
	sum := sha256.Sum256(cardJSON)
	return `"` + hex.EncodeToString(sum[:])[:32] + `"`
}
