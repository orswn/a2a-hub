package engine

import (
	"encoding/base64"
	"regexp"
	"strings"
	"testing"
	"time"
)

// The expected values come from the TypeScript hub functions on Node 24.

func TestCleanStatusTextMatchesTheTypeScriptHub(t *testing.T) {
	for in, want := range map[string]string{
		"  a\n b  ":                     "a b",
		"a \t b":                        "a \t b",
		"a\r\n\r\n  b":                  "a b",
		"a\u00a0\nb":                    "a b",
		"\u2028a\u2029":                 "a",
		" \n ":                          "",
		"a\u3000\nb\u3000":              "a b",
		strings.Repeat("x", 170):        strings.Repeat("x", 160),
		strings.Repeat("😀", 100):        strings.Repeat("😀", 80),
		strings.Repeat("x", 159) + " y": strings.Repeat("x", 159),
	} {
		if got := CleanStatusText(in); got != want {
			t.Errorf("CleanStatusText(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMessageTextJoinsTheTextParts(t *testing.T) {
	msg := parse(t, `{"parts":[{"text":"a"},{"data":{}},{"text":"b"},{"text":1}]}`)
	if got := MessageText(msg); got != "a\nb" {
		t.Errorf("MessageText = %q", got)
	}
	if got := MessageText(nil); got != "" {
		t.Errorf("MessageText(nil) = %q", got)
	}
}

func TestUUIDv7CarriesTheTimeAndTheVersion(t *testing.T) {
	id := UUIDv7(startTime)
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(id) {
		t.Errorf("id = %s", id)
	}
	// Buffer.writeUIntBE(Date.parse("2026-01-01T00:00:00Z"), 0, 6) gives 019b76daa800.
	if prefix := strings.ReplaceAll(id, "-", "")[:12]; prefix != "019b76daa800" {
		t.Errorf("time prefix = %s", prefix)
	}
	if UUIDv7(startTime) == id {
		t.Error("two ids are equal")
	}
}

func TestISOWritesToISOStringForm(t *testing.T) {
	for ms, want := range map[int64]string{
		startTime:     "2026-01-01T00:00:00.000Z",
		startTime + 1: "2026-01-01T00:00:00.001Z",
		startTime - 1: "2025-12-31T23:59:59.999Z",
		0:             "1970-01-01T00:00:00.000Z",
		-1:            "1969-12-31T23:59:59.999Z",
	} {
		if got := ISO(ms); got != want {
			t.Errorf("ISO(%d) = %s, want %s", ms, got, want)
		}
	}
}

func TestParseTimestampReadsTheISOFormsOfDateParse(t *testing.T) {
	local := func(y, mo, d, h, mi int) int64 {
		return time.Date(y, time.Month(mo), d, h, mi, 0, 0, time.Local).UnixMilli()
	}
	utc := func(text string) int64 {
		at, err := time.Parse(time.RFC3339Nano, text)
		if err != nil {
			t.Fatal(err)
		}
		return at.UnixMilli()
	}
	for in, want := range map[string]int64{
		"2026-01-01T00:00:00Z":        startTime,
		"2026-01-01":                  startTime,
		"2026-01":                     startTime,
		"2026":                        startTime,
		"2026-01-01T00:00":            local(2026, 1, 1, 0, 0),
		"2026-01-01T00:00:00":         local(2026, 1, 1, 0, 0),
		"2026-01-01T00:00:00.5Z":      startTime + 500,
		"2026-01-01T00:00:00.123456Z": startTime + 123,
		"2026-01-01T00:00:00+02:00":   utc("2025-12-31T22:00:00Z"),
		"2026-01-01T00:00:00+0200":    utc("2025-12-31T22:00:00Z"),
		"2026-01-01 00:00:00Z":        startTime,
		"2026-01-01t00:00:00z":        startTime,
		"+002026-01-01T00:00:00Z":     startTime,
		"2026-01-01T24:00:00Z":        startTime + day,
	} {
		got, ok := ParseTimestamp(in)
		if !ok || got != want {
			t.Errorf("ParseTimestamp(%q) = %d %v, want %d", in, got, ok, want)
		}
	}
	// Date.parse also refuses these. It takes "2026-1-1", "2026-02-30", "Jan 1 2026", and "1" with its
	// legacy parser; this hub refuses them.
	for _, in := range []string{"2026-01-01T00:00:00.Z", "x", "", "2026-01-01T00Z", "2026-01-01T00:00:60Z", "2026-1-1",
		"2026-02-30T00:00:00Z", "Jan 1 2026", "1"} {
		if got, ok := ParseTimestamp(in); ok {
			t.Errorf("ParseTimestamp(%q) = %d", in, got)
		}
	}
}

func TestPageTokensDecodeLikeBufferFromBase64url(t *testing.T) {
	token := encodePageToken("2026-01-01T00:00:00.000Z", "id-0001")
	if token != "WyIyMDI2LTAxLTAxVDAwOjAwOjAwLjAwMFoiLCJpZC0wMDAxIl0" {
		t.Errorf("token = %s", token)
	}
	decode := func(token string) (string, error) {
		var out string
		err := catch(func() {
			statusAt, id := decodePageToken(token)
			out = statusAt + " " + id
		})
		return out, err
	}
	for _, in := range []string{token, token + "==", token[:4] + "!!" + token[4:], token[:4] + " \n" + token[4:]} {
		if got, err := decode(in); err != nil || got != "2026-01-01T00:00:00.000Z id-0001" {
			t.Errorf("decode(%q) = %q, %v", in, got, err)
		}
	}
	for _, in := range []string{"WyJhIiwiYiJd", "WyJhIiwiYiJd=x", "WyJh*IiwiYiJd"} {
		if got, err := decode(in); err != nil || got != "a b" {
			t.Errorf("decode(%q) = %q, %v", in, got, err)
		}
	}
	for _, in := range []string{"bad", "W+yJhIiwiYiJd", encodeRaw(`["a"]`), encodeRaw(`["a",1]`), encodeRaw(`{}`)} {
		if _, err := decode(in); err == nil || err.Error() != "pageToken is not valid" {
			t.Errorf("decode(%q) = %v", in, err)
		}
	}
}

func encodeRaw(text string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(text))
}
