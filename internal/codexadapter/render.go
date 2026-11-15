package codexadapter

import (
	"encoding/json"
	"fmt"
	"strings"
)

// taskAnswerText ends the render of a message that carries a task.
const taskAnswerText = "Answer with the a2a reply tool and this messageId."

// wakingUpdateStates are the task states of an update that wake the sender, as in the pi client.
var wakingUpdateStates = map[string]bool{
	"completed": true, "failed": true, "canceled": true, "rejected": true, "input-required": true, "auth-required": true,
}

type messageEvent struct {
	MessageID string `json:"messageId"`
	TaskID    string `json:"taskId"`
	From      struct {
		Agent string `json:"agent"`
		Name  string `json:"name"`
		Kind  string `json:"kind"`
	} `json:"from"`
	Kind      string `json:"kind"`
	Wake      string `json:"wake"`
	Text      string `json:"text"`
	TaskState string `json:"taskState"`
}

type updateEvent struct {
	TaskID    string `json:"taskId"`
	State     string `json:"state"`
	MessageID string `json:"messageId"`
	Kind      string `json:"kind"`
	Text      string `json:"text"`
	From      struct {
		Agent string `json:"agent"`
		Name  string `json:"name"`
	} `json:"from"`
}

// sender describes who sent a message: its name, harness label, and agent or principal kind.
type sender struct {
	Name    string
	Harness string
	Kind    string
}

// Render is the user input of a wake turn: "Hub message from <name> (<harness>) kind=<kind> messageId=<id>:
// <text>". The sender kind shows only when it is not session, such as "(external)", or "(unknown)" after a
// failed sender lookup. A message that carries a task adds taskAnswerText. The reply tool needs only the
// messageId, so the task id stays out.
func Render(from sender, kind, messageID, text string, task bool) string {
	var who []string
	if from.Harness != "" {
		who = append(who, from.Harness)
	}
	switch from.Kind {
	case "session":
	case "":
		who = append(who, "unknown")
	default:
		who = append(who, from.Kind)
	}
	out := "Hub message from " + from.Name
	if len(who) > 0 {
		out += " (" + strings.Join(who, ", ") + ")"
	}
	out += fmt.Sprintf(" kind=%s messageId=%s: %s", kind, messageID, text)
	if task {
		out += "\n\n" + taskAnswerText
	}
	return out
}

func decode[T any](raw json.RawMessage) (T, bool) {
	var v T
	err := json.Unmarshal(raw, &v)
	return v, err == nil
}
