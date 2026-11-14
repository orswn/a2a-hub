package engine

import (
	"testing"

	"github.com/orswn/a2a-hub/internal/pushguard"
)

// The expected texts follow new URL() in Node 24 and validatePushUrl of the TypeScript hub.
func TestValidatePushURLRefusesBadURLsWithTheSameTexts(t *testing.T) {
	guard, err := pushguard.New(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for url, want := range map[string]string{
		"https://hooks.example/x": "",
		"HTTP://x":                "",
		"":                        "push config url is required",
		"foo":                     "push config url is not a valid URL",
		"http://":                 "push config url is not a valid URL",
		"http://a b/":             "push config url is not a valid URL",
		"https://[::1":            "push config url is not a valid URL",
		"mailto:x":                "push config url must use http or https",
		"ftp://x/":                "push config url must use http or https",
		"javascript:alert(1)":     "push config url must use http or https",
		"file:///etc/passwd":      "push config url must use http or https",
		"ws://x":                  "push config url must use http or https",
		"http://127.0.0.1:9/hook": "push url address 127.0.0.1 is not allowed",
	} {
		got := ""
		if err := ValidatePushURL(url, guard); err != nil {
			got = err.Error()
			if reason(t, err) != ReasonInvalid {
				t.Errorf("%q: reason %q", url, reason(t, err))
			}
		}
		if got != want {
			t.Errorf("%q = %q, want %q", url, got, want)
		}
	}
	if err := ValidatePushURL(float64(1), guard); err == nil || err.Error() != "push config url is required" {
		t.Errorf("number = %v", err)
	}
}
