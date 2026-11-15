package codexadapter

import "testing"

func TestRender(t *testing.T) {
	cases := []struct {
		from sender
		kind string
		task bool
		want string
	}{
		{sender{Name: "pi-1", Harness: "pi", Kind: "session"}, "task", true,
			"Hub message from pi-1 (pi) kind=task messageId=m1: hi\n\nAnswer with the a2a reply tool and this messageId."},
		{sender{Name: "hermes", Kind: "external"}, "task", false,
			"Hub message from hermes (external) kind=task messageId=m1: hi"},
		{sender{Name: "a", Harness: "codex", Kind: "session"}, "note", false,
			"Hub message from a (codex) kind=note messageId=m1: hi"},
		{sender{Name: "tck", Kind: "builtin"}, "result", false,
			"Hub message from tck (builtin) kind=result messageId=m1: hi"},
		// A failed sender lookup shows that the kind is unknown.
		{sender{Name: "gone"}, "result", false,
			"Hub message from gone (unknown) kind=result messageId=m1: hi"},
	}
	for _, c := range cases {
		if got := Render(c.from, c.kind, "m1", "hi", c.task); got != c.want {
			t.Fatalf("Render = %q, want %q", got, c.want)
		}
	}
}
