package jsonx

import (
	"encoding/json"
	"testing"
)

// The wanted texts come from Node 24: JSON.stringify(JSON.parse(input)).
func TestRoundTripMatchesJavaScript(t *testing.T) {
	cases := []struct{ in, want string }{
		{
			`{"b":1,"2":2,"a":3,"10":4,"01":5,"4294967295":6,"4294967294":7}`,
			`{"2":2,"10":4,"4294967294":7,"b":1,"a":3,"01":5,"4294967295":6}`,
		},
		{`{"a":1,"b":2,"a":3}`, `{"a":3,"b":2}`},
		{
			`[1.0,-0,1e21,1e-7,0.000001,123456789012345678901,1.5e-7,1e400,-1e400,0.1,100,2.5e+3]`,
			`[1,0,1e+21,1e-7,0.000001,123456789012345680000,1.5e-7,null,null,0.1,100,2500]`,
		},
		{
			`"\u0001\u001f\b\f\n\r\t\"\\/<>&\u2028\u2029é\ud83d\ude00\u007f"`,
			"\"\\u0001\\u001f\\b\\f\\n\\r\\t\\\"\\\\/<>&\u2028\u2029é😀\u007f\"",
		},
		{`{"x":[null,true,false,{}],"y":{"z":[]}}`, `{"x":[null,true,false,{}],"y":{"z":[]}}`},
		{" \n[ ]\t", `[]`},
	}
	for _, c := range cases {
		v, err := ParseString(c.in)
		if err != nil {
			t.Fatalf("Parse(%s): %v", c.in, err)
		}
		if got := string(Stringify(v)); got != c.want {
			t.Errorf("Stringify(Parse(%s))\n got %s\nwant %s", c.in, got, c.want)
		}
	}
}

func TestParseRefusesWhatJSONParseRefuses(t *testing.T) {
	for _, in := range []string{``, `{`, `[1,]`, `{"a":1,}`, `01`, `NaN`, `'x'`, `{} {}`, `1 2`, `"\x"`, "\ufeff{}"} {
		if _, err := ParseString(in); err == nil {
			t.Errorf("Parse(%q) did not fail", in)
		}
	}
}

func TestObjectKeepsPlaceOnSetAndDropsUndefined(t *testing.T) {
	o := NewObject("messageId", "m1", "role", "ROLE_USER", "contextId", "c0")
	o.Set("taskId", "t1").Set("contextId", "c1").Set("role", nil)
	if got := string(Stringify(o)); got != `{"messageId":"m1","contextId":"c1","taskId":"t1"}` {
		t.Errorf("got %s", got)
	}
	c := o.Clone().Set("extra", Null)
	if o.Len() != 3 || string(Stringify(c)) != `{"messageId":"m1","contextId":"c1","taskId":"t1","extra":null}` {
		t.Errorf("clone changed the original or lost null: %s %s", Stringify(o), Stringify(c))
	}
	if string(Stringify([]any{nil, Null})) != `[null,null]` {
		t.Error("an undefined array item must print null")
	}
}

type shape struct {
	Text  string  `json:"text"`
	Data  any     `json:"data,omitempty"`
	Inner *Object `json:"inner,omitempty"`
	List  []any   `json:"list,omitzero"`
}

func TestMarshalStructsLikeJavaScript(t *testing.T) {
	got, err := MarshalString(shape{
		Text:  "<a & b>\u2028",
		Data:  Null,
		Inner: NewObject("z", 1.0, "a", "é"),
		List:  []any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := "{\"text\":\"<a & b>\u2028\",\"data\":null,\"inner\":{\"z\":1,\"a\":\"é\"},\"list\":[]}"; got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
	var o Object
	if err := json.Unmarshal([]byte(`{"b":1,"a":2}`), &o); err != nil || string(Stringify(&o)) != `{"b":1,"a":2}` {
		t.Errorf("UnmarshalJSON: %v %s", err, Stringify(&o))
	}
}

func TestStringLengthInUTF16Units(t *testing.T) {
	if Len("a😀é") != 4 {
		t.Errorf("Len = %d", Len("a😀é"))
	}
	for _, c := range []struct {
		in   string
		n    int
		want string
	}{{"a😀é", 1, "a"}, {"a😀é", 2, "a"}, {"a😀é", 3, "a😀"}, {"abc", 9, "abc"}} {
		if got := Slice(c.in, c.n); got != c.want {
			t.Errorf("Slice(%q, %d) = %q", c.in, c.n, got)
		}
	}
}
