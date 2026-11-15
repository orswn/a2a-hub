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

func TestASessionCardNamesTheHarnessAndKeepsTheSkillID(t *testing.T) {
	host := "mars"
	cases := []struct{ labels, description, skill string }{
		{`{"harness":"codex","team":"core"}`, "codex session on mars",
			`[{"id":"pi-session","name":"codex session","description":"codex session on mars","tags":["coding"]}]`},
		{`{"team":"core"}`, "session on mars",
			`[{"id":"pi-session","name":"session","description":"session on mars","tags":["coding"]}]`},
	}
	for _, c := range cases {
		agent := &engine.AgentRow{ID: "s1", Name: "bee", Kind: engine.KindSession, Host: &host, Labels: c.labels}
		card := BuildCard(CardInput{Agent: agent, BaseURL: "http://hub", Version: "1"})
		if got, _ := card.String("description"); got != c.description {
			t.Errorf("description %q, want %q", got, c.description)
		}
		if got := string(jsonx.Stringify(card.Value("skills"))); got != c.skill {
			t.Errorf("skills %s, want %s", got, c.skill)
		}
	}
}

func TestACardListsBothBusExtensionURIs(t *testing.T) {
	card := BuildCard(CardInput{Agent: &engine.AgentRow{ID: "hermes", Name: "hermes", Kind: engine.KindExternal}, BaseURL: "http://hub", Version: "1"})
	capabilities, _ := card.Object("capabilities")
	want := `[{"uri":"https://github.com/orswn/a2a-hub/bus/v1","required":false},{"uri":"https://github.com/orswn/pimon/a2a/bus/v1","required":false}]`
	if got := string(jsonx.Stringify(capabilities.Value("extensions"))); got != want {
		t.Errorf("extensions %s, want %s", got, want)
	}
}
