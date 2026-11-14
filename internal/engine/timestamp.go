package engine

import (
	"regexp"
	"strconv"
	"time"
)

var isoTimestamp = regexp.MustCompile(`^([+-]\d{6}|\d{4})(?:-(\d{2})(?:-(\d{2}))?)?` +
	`(?:[Tt ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?([Zz]|[+-]\d{2}:?\d{2})?)?$`)

// ParseTimestamp reads the ISO 8601 forms of the JavaScript Date Time String Format, in milliseconds since the
// epoch. A date alone is UTC, and a date and time without an offset is local time, as in Date.parse.
// Extra digits of a fraction are cut. The non-ISO forms that V8 also takes, such as "Jan 1 2026", are refused.
func ParseTimestamp(text string) (int64, bool) {
	m := isoTimestamp.FindStringSubmatch(text)
	if m == nil || m[1] == "-000000" {
		return 0, false
	}
	num := func(s string, fallback int) int {
		if s == "" {
			return fallback
		}
		n, _ := strconv.Atoi(s)
		return n
	}
	year := num(m[1], 0)
	month, day := num(m[2], 1), num(m[3], 1)
	hour, minute, second := num(m[4], 0), num(m[5], 0), num(m[6], 0)
	ms := 0
	if fraction := m[7]; fraction != "" {
		ms = num((fraction + "00")[:3], 0)
	}
	if month < 1 || month > 12 || day < 1 || day > time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day() ||
		minute > 59 || second > 59 || hour > 24 || (hour == 24 && (minute != 0 || second != 0 || ms != 0)) {
		return 0, false
	}
	location := time.UTC
	if m[4] != "" && m[8] == "" {
		location = time.Local
	}
	t := time.Date(year, time.Month(month), day, hour, minute, second, ms*int(time.Millisecond), location)
	if offset := m[8]; offset != "" && offset != "Z" && offset != "z" {
		digits := offset[1:]
		if len(digits) == 5 {
			digits = digits[:2] + digits[3:]
		}
		oh, om := num(digits[:2], 0), num(digits[2:], 0)
		if oh > 23 || om > 59 {
			return 0, false
		}
		shift := time.Duration(oh)*time.Hour + time.Duration(om)*time.Minute
		if offset[0] == '+' {
			shift = -shift
		}
		t = t.Add(shift)
	}
	return t.UnixMilli(), true
}
