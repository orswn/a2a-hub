// Package jsonx reads and writes JSON with the semantics of JavaScript JSON.parse and JSON.stringify.
//
// The hub shares its sqlite file and its wire with a TypeScript implementation, so JSON text must come out
// byte for byte the same: keys keep their order (array index keys first, as in a JavaScript object),
// numbers print like JavaScript numbers, and strings escape only what JSON.stringify escapes.
//
// A parsed value is one of: Null, bool, float64, string, []any, or *Object. A Go nil is JavaScript
// undefined: an object drops the key and an array prints null.
package jsonx

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"unicode/utf16"
	"unicode/utf8"
)

type null struct{}

func (null) MarshalJSON() ([]byte, error) { return []byte("null"), nil }

// Null is the JSON null value. It differs from a Go nil, which means the value is absent.
var Null any = null{}

// IsNull reports whether v is the JSON null value.
func IsNull(v any) bool {
	_, ok := v.(null)
	return ok
}

// Object is a JSON object that keeps the key order of a JavaScript object.
type Object struct {
	keys []string
	vals map[string]any
}

// NewObject makes an object from key and value pairs. A nil value leaves its key out.
func NewObject(pairs ...any) *Object {
	o := &Object{}
	for i := 0; i+1 < len(pairs); i += 2 {
		o.Set(pairs[i].(string), pairs[i+1])
	}
	return o
}

// Len is the number of keys.
func (o *Object) Len() int {
	if o == nil {
		return 0
	}
	return len(o.keys)
}

// Get returns the value of a key. A missing key gives nil, false.
func (o *Object) Get(key string) (any, bool) {
	if o == nil {
		return nil, false
	}
	v, ok := o.vals[key]
	return v, ok
}

// Value returns the value of a key, or nil when the key is missing.
func (o *Object) Value(key string) any {
	v, _ := o.Get(key)
	return v
}

// String returns the value of a key when it is a string.
func (o *Object) String(key string) (string, bool) {
	s, ok := o.Value(key).(string)
	return s, ok
}

// Object returns the value of a key when it is an object.
func (o *Object) Object(key string) (*Object, bool) {
	obj, ok := o.Value(key).(*Object)
	return obj, ok && obj != nil
}

// Set sets a key. A new key goes last; an existing key keeps its place. A nil value deletes the key, like
// an assignment of undefined followed by JSON.stringify.
func (o *Object) Set(key string, v any) *Object {
	if v == nil {
		o.Delete(key)
		return o
	}
	if o.vals == nil {
		o.vals = map[string]any{}
	}
	if _, ok := o.vals[key]; !ok {
		o.keys = append(o.keys, key)
	}
	o.vals[key] = v
	return o
}

// Delete removes a key.
func (o *Object) Delete(key string) {
	if o == nil {
		return
	}
	if _, ok := o.vals[key]; !ok {
		return
	}
	delete(o.vals, key)
	o.keys = slices.DeleteFunc(o.keys, func(k string) bool { return k == key })
}

// Keys lists the keys in JavaScript property order: array index keys ascending, then the rest in insertion order.
func (o *Object) Keys() []string {
	if o == nil {
		return nil
	}
	var index, rest []string
	for _, k := range o.keys {
		if _, ok := arrayIndex(k); ok {
			index = append(index, k)
		} else {
			rest = append(rest, k)
		}
	}
	if len(index) == 0 {
		return rest
	}
	slices.SortFunc(index, func(a, b string) int {
		x, _ := arrayIndex(a)
		y, _ := arrayIndex(b)
		return int(x) - int(y)
	})
	return append(index, rest...)
}

// Clone copies the key list. The values are shared, so treat parsed values as read-only.
func (o *Object) Clone() *Object {
	if o == nil {
		return &Object{}
	}
	c := &Object{keys: slices.Clone(o.keys), vals: make(map[string]any, len(o.vals))}
	for k, v := range o.vals {
		c.vals[k] = v
	}
	return c
}

// MarshalJSON writes the object like JSON.stringify.
func (o *Object) MarshalJSON() ([]byte, error) { return Stringify(o), nil }

// UnmarshalJSON reads a JSON object like JSON.parse.
func (o *Object) UnmarshalJSON(data []byte) error {
	v, err := Parse(data)
	if err != nil {
		return err
	}
	obj, ok := v.(*Object)
	if !ok {
		return errors.New("jsonx: not a JSON object")
	}
	*o = *obj
	return nil
}

// arrayIndex reports whether k is a canonical array index, the keys that a JavaScript object lists first.
func arrayIndex(k string) (uint32, bool) {
	if k == "" || len(k) > 10 || (len(k) > 1 && k[0] == '0') {
		return 0, false
	}
	n, err := strconv.ParseUint(k, 10, 32)
	if err != nil || n == math.MaxUint32 {
		return 0, false
	}
	return uint32(n), true
}

// Parse reads one JSON value like JSON.parse: a duplicate key keeps its first place and its last value, and a
// number is a float64, so an integer beyond 2^53 loses precision and 1e400 becomes +Inf.
func Parse(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	v, err := parseValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("jsonx: unexpected data after the JSON value")
	}
	return v, nil
}

// ParseString is Parse for a string.
func ParseString(text string) (any, error) { return Parse([]byte(text)) }

func parseValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			obj := &Object{}
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key, _ := keyTok.(string)
				v, err := parseValue(dec)
				if err != nil {
					return nil, err
				}
				obj.Set(key, v)
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			return obj, nil
		case '[':
			list := []any{}
			for dec.More() {
				v, err := parseValue(dec)
				if err != nil {
					return nil, err
				}
				list = append(list, v)
			}
			if _, err := dec.Token(); err != nil {
				return nil, err
			}
			return list, nil
		}
		return nil, fmt.Errorf("jsonx: unexpected %v", t)
	case nil:
		return Null, nil
	case bool, string:
		return t, nil
	case json.Number:
		f, err := strconv.ParseFloat(string(t), 64)
		if err != nil && !errors.Is(err, strconv.ErrRange) {
			return nil, err
		}
		return f, nil
	}
	return nil, fmt.Errorf("jsonx: unexpected token %v", tok)
}

// Stringify writes v like JSON.stringify. Go values that are not parsed values (structs, maps, ints) go through
// encoding/json first, so their field order and omitempty rules apply.
func Stringify(v any) []byte {
	var buf bytes.Buffer
	write(&buf, v)
	return buf.Bytes()
}

// Marshal is Stringify with an error for a value that encoding/json cannot write.
func Marshal(v any) ([]byte, error) {
	if !parsed(v) {
		data, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		if v, err = Parse(data); err != nil {
			return nil, err
		}
	}
	return Stringify(v), nil
}

// MarshalString is Marshal into a string.
func MarshalString(v any) (string, error) {
	data, err := Marshal(v)
	return string(data), err
}

// ToValue turns any value that encoding/json can write into a parsed value.
func ToValue(v any) (any, error) {
	if parsed(v) {
		return v, nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return Parse(data)
}

func parsed(v any) bool {
	switch v.(type) {
	case nil, null, bool, float64, string, []any, *Object:
		return true
	}
	return false
}

func write(buf *bytes.Buffer, v any) {
	switch t := v.(type) {
	case nil, null:
		buf.WriteString("null")
	case bool:
		buf.WriteString(strconv.FormatBool(t))
	case float64:
		buf.WriteString(FormatNumber(t))
	case int:
		buf.WriteString(strconv.Itoa(t))
	case int64:
		buf.WriteString(strconv.FormatInt(t, 10))
	case string:
		writeString(buf, t)
	case []any:
		buf.WriteByte('[')
		for i, item := range t {
			if i > 0 {
				buf.WriteByte(',')
			}
			write(buf, item)
		}
		buf.WriteByte(']')
	case *Object:
		buf.WriteByte('{')
		first := true
		for _, k := range t.Keys() {
			if !first {
				buf.WriteByte(',')
			}
			first = false
			writeString(buf, k)
			buf.WriteByte(':')
			write(buf, t.vals[k])
		}
		buf.WriteByte('}')
	default:
		value, err := ToValue(t)
		if err != nil {
			buf.WriteString("null")
			return
		}
		write(buf, value)
	}
}

// FormatNumber prints a number like JavaScript: shortest round trip digits, exponent only below 1e-6 or from
// 1e21 on, and null for NaN and the infinities as JSON.stringify does.
func FormatNumber(f float64) string {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "null"
	}
	if f == 0 {
		return "0"
	}
	format := byte('f')
	if abs := math.Abs(f); abs < 1e-6 || abs >= 1e21 {
		format = 'e'
	}
	b := strconv.AppendFloat(nil, f, format, -1, 64)
	if format == 'e' {
		if n := len(b); n >= 4 && b[n-4] == 'e' && b[n-3] == '-' && b[n-2] == '0' {
			b[n-2] = b[n-1]
			b = b[:n-1]
		}
	}
	return string(b)
}

const hex = "0123456789abcdef"

func writeString(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			switch c {
			case '"':
				buf.WriteString(`\"`)
			case '\\':
				buf.WriteString(`\\`)
			case '\b':
				buf.WriteString(`\b`)
			case '\f':
				buf.WriteString(`\f`)
			case '\n':
				buf.WriteString(`\n`)
			case '\r':
				buf.WriteString(`\r`)
			case '\t':
				buf.WriteString(`\t`)
			default:
				if c < 0x20 {
					buf.WriteString(`\u00`)
					buf.WriteByte(hex[c>>4])
					buf.WriteByte(hex[c&0xf])
				} else {
					buf.WriteByte(c)
				}
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			buf.WriteRune(utf8.RuneError)
		} else {
			buf.WriteString(s[i : i+size])
		}
		i += size
	}
	buf.WriteByte('"')
}

// Len is the length of s in UTF-16 code units, the JavaScript string length.
func Len(s string) int {
	n := 0
	for _, r := range s {
		n += utf16.RuneLen(r)
	}
	return n
}

// Slice returns the first n UTF-16 code units of s, like s.slice(0, n) in JavaScript. A surrogate pair that
// n would split is left out, because Go strings cannot hold a lone surrogate.
func Slice(s string, n int) string {
	units := 0
	for i, r := range s {
		size := utf16.RuneLen(r)
		if units+size > n {
			return s[:i]
		}
		units += size
	}
	return s
}
