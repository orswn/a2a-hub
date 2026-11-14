package pushguard

import (
	"context"
	"errors"
	"net/netip"
	"regexp"
	"testing"

	"github.com/orswn/a2a-hub/internal/config"
)

func table(entries map[string][]string) Resolver {
	return func(_ context.Context, host string) ([]string, error) {
		addresses, ok := entries[host]
		if !ok {
			return nil, errors.New("ENOTFOUND " + host)
		}
		return addresses, nil
	}
}

func mustGuard(t *testing.T, allow []string, resolve Resolver) *Guard {
	t.Helper()
	g, err := New(allow, resolve)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func checkURL(g *Guard, raw string) string {
	u, err := ParseURL(raw)
	if err != nil {
		return err.Error()
	}
	return g.CheckHost(u.Host)
}

func match(t *testing.T, label, got, pattern string) {
	t.Helper()
	if pattern == "" {
		if got != "" {
			t.Errorf("%s = %q, want no refusal", label, got)
		}
		return
	}
	if !regexp.MustCompile(pattern).MatchString(got) {
		t.Errorf("%s = %q, want a match for %s", label, got, pattern)
	}
}

func TestDeniesPrivateLoopbackLinkLocalCGNATUnspecifiedAndMulticastByDefault(t *testing.T) {
	g := mustGuard(t, nil, nil)
	for _, ip := range []string{
		"127.0.0.1", "10.1.2.3", "172.20.0.5", "192.168.1.1", "169.254.169.254", "100.100.1.1", "0.0.0.0",
		"224.0.0.1", "::1", "::", "fd12::1", "fe80::1", "ff02::1", "::ffff:127.0.0.1",
	} {
		if g.AddressAllowed(netip.MustParseAddr(ip)) {
			t.Errorf("%s allowed", ip)
		}
	}
	for _, ip := range []string{"93.184.216.34", "100.128.0.1", "172.32.0.1", "2606:4700::1111"} {
		if !g.AddressAllowed(netip.MustParseAddr(ip)) {
			t.Errorf("%s denied", ip)
		}
	}
}

func TestRefusesAnIPLiteralInADeniedRangeAtConfigTimeAndLeavesHostNamesToSendTime(t *testing.T) {
	g := mustGuard(t, nil, nil)
	match(t, "metadata", checkURL(g, "http://169.254.169.254/latest"), `169\.254\.169\.254`)
	match(t, "v6 loopback", checkURL(g, "http://[::1]:8080/x"), `::1`)
	match(t, "one number", checkURL(g, "http://2130706433/"), `127\.0\.0\.1`)
	match(t, "host name", checkURL(g, "https://hooks.example/a"), "")
	match(t, "public", checkURL(g, "https://93.184.216.34/a"), "")
}

func TestChecksEveryResolvedAddressAtSendTime(t *testing.T) {
	g := mustGuard(t, nil, table(map[string][]string{
		"public.example": {"93.184.216.34"},
		"rebind.example": {"93.184.216.34", "10.0.0.7"},
		"inside.example": {"192.168.0.10"},
	}))
	ctx := context.Background()
	match(t, "public", g.CheckSend(ctx, "https://public.example/hook"), "")
	match(t, "rebind", g.CheckSend(ctx, "https://rebind.example/hook"), `10\.0\.0\.7`)
	match(t, "inside", g.CheckSend(ctx, "https://inside.example/hook"), `192\.168\.0\.10`)
	match(t, "missing", g.CheckSend(ctx, "https://missing.example/hook"), `ENOTFOUND`)
	match(t, "literal", g.CheckSend(ctx, "http://127.0.0.1:9/hook"), `127\.0\.0\.1`)
}

func TestAllowsHostNamesAndCIDRsFromTheAllowList(t *testing.T) {
	g := mustGuard(t, []string{"hooks.internal", "10.20.0.0/16", "fd00::5"}, table(map[string][]string{
		"hooks.internal": {"10.9.9.9"},
		"svc.example":    {"10.20.3.4"},
		"other.example":  {"10.21.0.1"},
	}))
	ctx := context.Background()
	match(t, "allowed host", g.CheckSend(ctx, "http://hooks.internal/x"), "")
	match(t, "upper case and trailing dot", g.CheckSend(ctx, "http://HOOKS.internal./x"), "")
	match(t, "inside CIDR", g.CheckSend(ctx, "http://svc.example/x"), "")
	match(t, "outside CIDR", g.CheckSend(ctx, "http://other.example/x"), `10\.21\.0\.1`)
	match(t, "CIDR literal", checkURL(g, "http://10.20.1.1/x"), "")
	match(t, "allowed v6", checkURL(g, "http://[fd00::5]/x"), "")
	match(t, "other v6", checkURL(g, "http://[fd00::6]/x"), `fd00::6`)
}

func TestParsesTheAllowListAndRefusesABadEntry(t *testing.T) {
	got, err := config.ParsePushAllow(" Hooks.Example , 10.0.0.0/8,::1,, ")
	if err != nil || len(got) != 3 || got[0] != "hooks.example" || got[1] != "10.0.0.0/8" || got[2] != "::1" {
		t.Errorf("ParsePushAllow = %q, %v", got, err)
	}
	if got, err := config.ParsePushAllow(""); err != nil || len(got) != 0 {
		t.Errorf("empty = %q, %v", got, err)
	}
	for _, bad := range []string{"10.0.0.0/33", "fd00::/129", "10.0.0.0/x", "http://x", "a b", "-bad.example"} {
		if _, err := New([]string{bad}, nil); err == nil || !regexp.MustCompile(`push allow`).MatchString(err.Error()) {
			t.Errorf("New(%q) = %v", bad, err)
		}
	}
}

// The hosts come from Node 24: new URL(raw).hostname, without brackets.
func TestParseURLGivesTheWHATWGHost(t *testing.T) {
	cases := map[string]string{
		"http://[::ffff:127.0.0.1]/":           "::ffff:7f00:1",
		"http://[0:0:0:0:0:ffff:0a00:0001]/":   "::ffff:a00:1",
		"http://0x7f.1/":                       "127.0.0.1",
		"http://1.2.3.4./":                     "1.2.3.4",
		"http://0x/":                           "0.0.0.0",
		"http://1.2.0x/":                       "1.2.0.0",
		"http://1.2.3.04/":                     "1.2.3.4",
		"HTTP://EXAMPLE.com/":                  "example.com",
		"https://hooks.example:8443/a?b#c":     "hooks.example",
		"http://4294967296/":                   "",
		"http://09/":                           "",
		"http://a.09/":                         "",
		"http://1.256.0.0/":                    "",
		"http:///x":                            "x",
		"https:hooks.example":                  "hooks.example",
		"http://":                              "",
		"not a url":                            "",
		"https://user:pass@hooks.example/path": "hooks.example",
	}
	for raw, want := range cases {
		u, err := ParseURL(raw)
		if want == "" {
			if !errors.Is(err, ErrInvalidURL) {
				t.Errorf("ParseURL(%q) = %+v, %v, want invalid", raw, u, err)
			}
			continue
		}
		if err != nil || u.Host != want {
			t.Errorf("ParseURL(%q) = %+v, %v, want host %s", raw, u, err, want)
		}
	}
	if u, err := ParseURL("ftp://files.example/x"); err != nil || u.Scheme != "ftp" {
		t.Errorf("ftp = %+v, %v", u, err)
	}
}
