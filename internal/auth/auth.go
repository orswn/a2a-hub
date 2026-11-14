// Package auth maps bearer tokens to principals through a token file of sha256 digests.
package auth

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
)

// Kind is the kind of a principal.
type Kind string

const (
	KindHost Kind = "host"
	KindPeer Kind = "peer"
)

// Principal is the caller that a token names.
type Principal struct {
	Name string
	Kind Kind
}

// Anonymous is the principal of a request without an Authorization header on a route that allows it.
var Anonymous = Principal{Name: "anonymous", Kind: KindPeer}

type entry struct {
	principal Principal
	digest    [sha256.Size]byte
}

// TokenTable holds the digests of the known tokens.
type TokenTable struct {
	entries []entry
}

// Size is the number of tokens.
func (t *TokenTable) Size() int { return len(t.entries) }

// Match compares the token against every entry, so the time does not depend on which entry matches.
func (t *TokenTable) Match(token string) (Principal, bool) {
	digest := Sha256(token)
	var found Principal
	ok := false
	for _, e := range t.entries {
		if subtle.ConstantTimeCompare(e.digest[:], digest[:]) == 1 && !ok {
			found, ok = e.principal, true
		}
	}
	return found, ok
}

// Sha256 is the digest that the token file stores for a token.
func Sha256(token string) [sha256.Size]byte {
	return sha256.Sum256([]byte(token))
}

var digestHex = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

// ParseTokens reads a token file: a JSON object of name to {"kind": "host"|"peer", "sha256": "<64 hex>"}.
func ParseTokens(text []byte) (*TokenTable, error) {
	names, values, err := objectEntries(text)
	if err != nil {
		return nil, err
	}
	table := &TokenTable{}
	for i, name := range names {
		// A struct would match field names without regard to case. JSON.parse keys are exact.
		var spec map[string]json.RawMessage
		var kind string
		if json.Unmarshal(values[i], &spec) != nil || json.Unmarshal(spec["kind"], &kind) != nil ||
			(kind != string(KindHost) && kind != string(KindPeer)) {
			return nil, fmt.Errorf(`token %s: kind must be "host" or "peer"`, name)
		}
		var text string
		if json.Unmarshal(spec["sha256"], &text) != nil || !digestHex.MatchString(text) {
			return nil, fmt.Errorf("token %s: sha256 must be 64 hex chars", name)
		}
		e := entry{principal: Principal{Name: name, Kind: Kind(kind)}}
		if _, err := hex.Decode(e.digest[:], []byte(text)); err != nil {
			return nil, fmt.Errorf("token %s: %w", name, err)
		}
		table.entries = append(table.entries, e)
	}
	return table, nil
}

// objectEntries decodes a JSON object in key order. A repeated key keeps its first place and its last value, like JSON.parse.
func objectEntries(text []byte) ([]string, []json.RawMessage, error) {
	var raw json.RawMessage
	if err := json.Unmarshal(text, &raw); err != nil {
		return nil, nil, fmt.Errorf("token file is not valid JSON: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	start, err := decoder.Token()
	if err != nil {
		return nil, nil, err
	}
	if delim, ok := start.(json.Delim); !ok || delim != '{' {
		return nil, nil, errors.New("token file must be a JSON object")
	}
	var names []string
	var values []json.RawMessage
	index := map[string]int{}
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, nil, err
		}
		name := key.(string)
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, nil, err
		}
		if at, seen := index[name]; seen {
			values[at] = value
			continue
		}
		index[name] = len(names)
		names = append(names, name)
		values = append(values, value)
	}
	return names, values, nil
}

// LoadTokens reads the token file. An empty path gives an empty table.
func LoadTokens(file string) (*TokenTable, error) {
	if file == "" {
		return &TokenTable{}, nil
	}
	text, err := os.ReadFile(file) //nolint:gosec // G304: the operator gives the token file path.
	if err != nil {
		return nil, err
	}
	return ParseTokens(text)
}

var bearer = regexp.MustCompile(`(?i)^Bearer` + jsSpace + `+(` + jsNonSpace + `+)` + jsSpace + `*$`)

const (
	jsSpaceChars = `\t\n\v\f\r \x{a0}\x{1680}\x{2000}-\x{200a}\x{2028}\x{2029}\x{202f}\x{205f}\x{3000}\x{feff}`
	jsSpace      = `[` + jsSpaceChars + `]`
	jsNonSpace   = `[^` + jsSpaceChars + `]`
)

// Authenticate checks an Authorization header. An empty header gives Anonymous only when allowAnonymous is set.
// false means 401.
func Authenticate(header string, tokens *TokenTable, allowAnonymous bool) (Principal, bool) {
	if header == "" {
		if allowAnonymous {
			return Anonymous, true
		}
		return Principal{}, false
	}
	match := bearer.FindStringSubmatch(header)
	if match == nil {
		return Principal{}, false
	}
	return tokens.Match(match[1])
}
