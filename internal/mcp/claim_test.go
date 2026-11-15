package mcp

import (
	"strings"
	"testing"
	"time"

	"github.com/orswn/a2a-hub/internal/jsonx"
)

// waitingSend starts a Hermes send with wait and gives the message that p1 got from it.
func waitingSend(t *testing.T, h *env, c *client, wait int) (messageID, contextID string, done chan string) {
	t.Helper()
	done = make(chan string, 1)
	go func() {
		done <- c.ok("send", args("target", "p1", "text", "quick?", "wait", wait, "origin", telegram()))
	}()
	eventually(t, "sent", func() bool {
		inbox, _ := h.e.Inbox("p1", false, 1)
		if len(inbox) == 1 {
			messageID, contextID = inbox[0].MessageID, inbox[0].ContextID
			return true
		}
		return false
	})
	return messageID, contextID, done
}

func TestAWaitingSendTakesOnlyTheFirstReplyAndTheNextOnesArePosted(t *testing.T) {
	hook := newReceiver(t)
	h, c := hermesSetup(t, hook)
	messageID, _, done := waitingSend(t, h, c, 10)
	for _, id := range []string{"m-a", "m-b", "m-c"} {
		h.sendFrom("p1", "hermes", id, "kind", "answer", "replyTo", messageID)
	}
	mustContain(t, "wait", <-done, "messageId=m-a")
	hook.wait(t, 2)
	all := strings.Join(hook.bodies, "\n")
	if hook.count() != 2 || !strings.Contains(all, `"message_id":"m-b"`) || !strings.Contains(all, `"message_id":"m-c"`) {
		t.Errorf("posts = %q", hook.bodies)
	}
}

func TestAWaiterHoldsBackOnlyOneMessage(t *testing.T) {
	hook := newReceiver(t)
	h, c := hermesSetup(t, hook)
	messageID, contextID := sentIDs(t, c.ok("send", args("target", "p1", "text", "hello")))
	w := h.srv.addWaiter("hermes", messageID, contextID)
	for _, id := range []string{"m-a", "m-b", "m-c"} {
		h.sendFrom("p1", "hermes", id, "kind", "answer", "replyTo", messageID)
	}
	hook.wait(t, 2)
	h.srv.removeWaiter(w)
	hook.wait(t, 3)
	var ids []string
	for _, body := range hook.bodies {
		ids = append(ids, body[strings.Index(body, `"message_id":"`)+14:][:3])
	}
	if strings.Join(ids, " ") != "m-b m-c m-a" {
		t.Errorf("posted %q, want m-b and m-c at once and the held m-a after the wait", ids)
	}
}

func TestAThreadNoteDoesNotKeepAWaitingSendFromItsReply(t *testing.T) {
	hook := newReceiver(t)
	h, c := hermesSetup(t, hook)
	messageID, contextID, done := waitingSend(t, h, c, 5)
	h.sendFrom("p1", "hermes", "m-note", "kind", "note", "contextId", contextID)
	hook.wait(t, 1)
	h.sendFrom("p1", "hermes", "m-a", "kind", "answer", "replyTo", messageID)
	mustContain(t, "wait", <-done, "First reply:", "messageId=m-a")
	if hook.count() != 1 || !strings.Contains(hook.bodies[0], `"message_id":"m-note"`) {
		t.Errorf("posts = %q", hook.bodies)
	}
}

// A task reply can come before the waiter knows the task. The waiter holds it by its thread and hands it to the
// webhook if the wait ends without reading it.
func TestATaskMessageThatAWaiterHeldIsPostedWhenTheWaitEnds(t *testing.T) {
	hook := newReceiver(t)
	h, c := hermesSetup(t, hook)
	sent := c.ok("send", args("target", "p1", "text", "build", "kind", "task"))
	messageID, contextID := sentIDs(t, sent)
	owed, _ := h.e.Owed("p1")
	w := h.srv.addWaiter("hermes", messageID, contextID)
	if _, err := h.e.LinkUpdate("p1", owed.Tasks[0].TaskID, jsonx.NewObject("state", "completed", "text", "built")); err != nil {
		t.Fatal(err)
	}
	h.srv.waitMu.Lock()
	held := w.claimed
	h.srv.waitMu.Unlock()
	if held == nil || held.TaskID != owed.Tasks[0].TaskID {
		t.Fatalf("held = %+v", held)
	}
	ended := time.Now()
	h.srv.removeWaiter(w)
	hook.wait(t, 1)
	if !strings.Contains(hook.bodies[0], `"text":"built"`) || hook.times[0].Before(ended) {
		t.Errorf("posts = %q at %v, wait ended at %v", hook.bodies, hook.times[0], ended)
	}
}

func TestAMessageThatIsReadBeforeItsPostIsNotPosted(t *testing.T) {
	hook := newReceiver(t)
	hook.hold = make(chan struct{})
	h, c := hermesSetup(t, hook)
	messageID, _ := sentIDs(t, c.ok("send", args("target", "p1", "text", "hello")))
	h.sendFrom("p1", "hermes", "m-1", "kind", "answer", "replyTo", messageID)
	hook.wait(t, 1)
	h.sendFrom("p1", "hermes", "m-2", "kind", "answer", "replyTo", messageID)
	mustContain(t, "inbox", c.ok("inbox", args("messageId", "m-2")), "messageId=m-2")
	close(hook.hold)
	h.sendFrom("p1", "hermes", "m-3", "kind", "answer", "replyTo", messageID)
	hook.wait(t, 2)
	if hook.count() != 2 || !strings.Contains(hook.bodies[1], `"message_id":"m-3"`) {
		t.Errorf("posts = %q", hook.bodies)
	}
}
