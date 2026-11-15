package a2aapi

import (
	"testing"

	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/jsonx"
)

func TestAnExternalCardTakesTheConfiguredDescriptionAndTheAgentSkills(t *testing.T) {
	agent := &engine.AgentRow{ID: "hermes", Name: "hermes", Kind: "external"}
	skill := jsonx.NewObject("id", "s", "name", "S", "description", "", "tags", []any{})
	cases := []struct {
		external    *ExternalCard
		description string
		skills      string
	}{
		{nil, "hermes: an external A2A agent",
			`[{"id":"external","name":"hermes","description":"hermes: an external A2A agent","tags":["external"]}]`},
		{&ExternalCard{Description: "notes"}, "notes",
			`[{"id":"external","name":"hermes","description":"notes","tags":["external"]}]`},
		{&ExternalCard{Description: "notes", Skills: []*jsonx.Object{skill}}, "notes",
			`[{"id":"s","name":"S","description":"","tags":[]}]`},
	}
	for _, c := range cases {
		card := BuildCard(CardInput{Agent: agent, BaseURL: "http://hub", Version: "1", External: c.external})
		if got, _ := card.String("description"); got != c.description {
			t.Errorf("description %q, want %q", got, c.description)
		}
		if got := string(jsonx.Stringify(card.Value("skills"))); got != c.skills {
			t.Errorf("skills %s, want %s", got, c.skills)
		}
	}
}
