package auth

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func hexOf(token string) string {
	digest := Sha256(token)
	return hex.EncodeToString(digest[:])
}

func testTable(t *testing.T) *TokenTable {
	t.Helper()
	table, err := ParseTokens([]byte(`{
		"mars": {"kind": "host", "sha256": "` + hexOf("mars-secret") + `"},
		"hermes": {"kind": "peer", "sha256": "` + strings.ToUpper(hexOf("hermes-secret")) + `"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	return table
}

func TestBearerTokenMapsToPrincipalAndKind(t *testing.T) {
	tokens := testTable(t)
	cases := map[string]Principal{
		"Bearer mars-secret":     {Name: "mars", Kind: KindHost},
		"bearer hermes-secret":   {Name: "hermes", Kind: KindPeer},
		"BEARER\tmars-secret \t": {Name: "mars", Kind: KindHost},
	}
	for header, want := range cases {
		got, ok := Authenticate(header, tokens, false)
		if !ok || got != want {
			t.Errorf("Authenticate(%q) = %+v, %v; want %+v", header, got, ok, want)
		}
	}
}

func TestMissingMalformedOrWrongTokenFails(t *testing.T) {
	tokens := testTable(t)
	for _, header := range []string{"", "Basic bWFyczp4", "Bearer wrong", "Bearer mars-secret extra", "Bearer", "Bearermars-secret"} {
		if got, ok := Authenticate(header, tokens, false); ok {
			t.Errorf("Authenticate(%q) = %+v, want 401", header, got)
		}
	}
}

func TestAnonymousOnlyWhereAllowedAndSentTokenStillChecked(t *testing.T) {
	tokens := testTable(t)
	if got, ok := Authenticate("", tokens, true); !ok || got != Anonymous {
		t.Errorf("Authenticate(\"\", allow) = %+v, %v; want anonymous", got, ok)
	}
	if _, ok := Authenticate("Bearer wrong", tokens, true); ok {
		t.Error("a wrong token passed on an anonymous route")
	}
}

func TestRefusesTokenFileWithBadKindOrDigest(t *testing.T) {
	cases := map[string]string{
		`{"x": {"kind": "admin", "sha256": "` + hexOf("a") + `"}}`: "kind",
		`{"x": {"kind": "host", "sha256": "abc"}}`:                 "sha256",
		`{"x": {"kind": "host", "sha256": 5}}`:                     "sha256",
		`{"x": null}`:                                              "kind",
		`[]`:                                                       "object",
		`null`:                                                     "object",
		`{`:                                                        "",
	}
	for text, want := range cases {
		_, err := ParseTokens([]byte(text))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("ParseTokens(%s) = %v, want an error with %q", text, err, want)
		}
	}
	table, err := ParseTokens([]byte(`{}`))
	if err != nil || table.Size() != 0 {
		t.Errorf("ParseTokens({}) = %v, %v", table, err)
	}
}

func TestLaterDuplicateKeyWins(t *testing.T) {
	table, err := ParseTokens([]byte(`{"a": {"kind": "host", "sha256": "` + hexOf("one") + `"}, "a": {"kind": "peer", "sha256": "` + hexOf("two") + `"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if table.Size() != 1 {
		t.Fatalf("size = %d, want 1", table.Size())
	}
	if got, ok := table.Match("two"); !ok || got.Kind != KindPeer {
		t.Errorf("Match(two) = %+v, %v", got, ok)
	}
	if _, ok := table.Match("one"); ok {
		t.Error("the replaced entry still matches")
	}
}

func TestLoadTokens(t *testing.T) {
	empty, err := LoadTokens("")
	if err != nil || empty.Size() != 0 {
		t.Fatalf("LoadTokens(\"\") = %v, %v", empty, err)
	}
	file := filepath.Join(t.TempDir(), "tokens.json")
	if err := os.WriteFile(file, []byte(`{"mars": {"kind": "host", "sha256": "`+hexOf("s")+`"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	table, err := LoadTokens(file)
	if err != nil || table.Size() != 1 {
		t.Fatalf("LoadTokens = %v, %v", table, err)
	}
	if _, err := LoadTokens(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Error("a missing token file loaded")
	}
}
