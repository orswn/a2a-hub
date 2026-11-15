package mcp

import (
	"strings"

	"github.com/orswn/a2a-hub/internal/a2aapi"
	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/jsonx"
)

// The wording of the delivery states. The MCP snapshot test pins it.
const (
	unconfirmedText = "possibly delivered; do not resend"
	noReplyText     = "No reply yet; it will arrive in your inbox."
)

func formatSend(headline, messageID, contextID string, lines []line) string {
	out := []string{headline + " on thread " + contextID + " (messageId " + messageID + ")."}
	for _, state := range []string{a2aapi.Delivered, a2aapi.Queued, a2aapi.Unconfirmed, a2aapi.Failed} {
		var group []string
		for _, l := range lines {
			if l.state != state {
				continue
			}
			note := l.note
			if note == "" && state == a2aapi.Unconfirmed {
				note = unconfirmedText
			}
			entry := l.name + " (id:" + l.id
			if note != "" {
				entry += ", " + note
			}
			group = append(group, entry+")")
		}
		if len(group) > 0 {
			out = append(out, state+": "+strings.Join(group, ", "))
		}
	}
	return strings.Join(out, "\n")
}

func senderText(from engine.InboxFrom) string {
	if from.Agent == "" {
		return from.Name + " (" + from.Kind + ")"
	}
	return from.Name + " (" + from.Kind + ", id:" + from.Agent + ")"
}

func formatItem(item engine.InboxItem) string {
	head := "messageId=" + item.MessageID + " threadId=" + item.ContextID
	if item.ReplyTo != nil {
		head += " replyTo=" + *item.ReplyTo
	}
	if item.TaskID != nil {
		head += " taskId=" + *item.TaskID
		if item.TaskState != "" {
			head += " " + item.TaskState
		}
	}
	lines := []string{head}
	if item.Direction == "in" {
		lines = append(lines, "from="+senderText(item.From))
	} else {
		lines = append(lines, "to="+item.To.Name+" (id:"+item.To.Agent+")")
	}
	meta := "kind=" + item.Kind
	if item.ExpectReply {
		meta += " expectReply"
	}
	lines = append(lines, meta+" at="+item.CreatedAt)
	if subject, ok := item.Subject.(string); ok && subject != "" {
		lines = append(lines, "subject: "+subject)
	}
	lines = append(lines, item.Text)
	if item.Data != nil {
		lines = append(lines, "data: "+string(jsonx.Stringify(item.Data)))
	}
	return strings.Join(lines, "\n")
}

func formatItems(items []engine.InboxItem) string {
	if len(items) == 0 {
		return "Nothing found."
	}
	out := make([]string, len(items))
	for i, item := range items {
		out[i] = formatItem(item)
	}
	return strings.Join(out, "\n\n---\n\n")
}

func harnessOf(agent engine.AgentInfo) string {
	harness, _ := agent.Labels.String(engine.HarnessLabel)
	return harness
}

func searchText(agent engine.AgentInfo) string {
	parts := []string{agent.Name, agent.ID, harnessOf(agent)}
	if agent.Host != nil {
		parts = append(parts, *agent.Host)
	}
	if topics, ok := agent.Topics.([]any); ok {
		for _, topic := range topics {
			if text, ok := topic.(string); ok {
				parts = append(parts, text)
			}
		}
	}
	return strings.ToLower(strings.Join(parts, "\n"))
}

func describeAgent(agent engine.AgentInfo) string {
	parts := []string{agent.Name, "id:" + agent.ID, "kind=" + agent.Kind}
	if harness := harnessOf(agent); harness != "" {
		parts = append(parts, "harness="+harness)
	}
	parts = append(parts, agent.State)
	if agent.Host != nil {
		parts = append(parts, "host="+*agent.Host)
	}
	if agent.Activity != nil && *agent.Activity != "" {
		parts = append(parts, `doing="`+jsonx.Slice(*agent.Activity, 60)+`"`)
	}
	if agent.Status.Text != "" {
		parts = append(parts, `status="`+agent.Status.Text+`"`)
	}
	if agent.Cwd != nil && *agent.Cwd != "" {
		parts = append(parts, "cwd="+*agent.Cwd)
	}
	return strings.Join(parts, " ")
}
