package hostauth

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHubURLPrefersTheFlagThenTheEnvThenTheDefault(t *testing.T) {
	env := func(value string) func(string) string {
		return func(key string) string {
			if key == "A2A_HUB_URL" {
				return value
			}
			return ""
		}
	}
	cases := []struct{ flag, env, want string }{
		{"http://flag:1/", "http://env:2", "http://flag:1"},
		{"", "http://env:2/", "http://env:2"},
		{"", "", DefaultHubURL},
	}
	for _, c := range cases {
		if got := HubURL(c.flag, env(c.env)); got != c.want {
			t.Errorf("HubURL(%q, %q) = %q, want %q", c.flag, c.env, got, c.want)
		}
	}
}

func TestTokenFileIsUnderConfigA2A(t *testing.T) {
	if got := TokenFile("/home/u"); got != "/home/u/.config/a2a/host.token" {
		t.Fatalf("TokenFile = %q", got)
	}
}

func TestReadTokenTrimsAPrivateFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.token")
	if err := os.WriteFile(path, []byte("  secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	token, err := ReadToken(path)
	if err != nil || token != "secret" {
		t.Fatalf("ReadToken = %q, %v", token, err)
	}
}

func TestReadTokenRefusesAFileThatOthersCanRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.token")
	if err := os.WriteFile(path, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadToken(path); err == nil || !strings.Contains(err.Error(), "mode 0600") {
		t.Fatalf("ReadToken error = %v", err)
	}
}

func TestReadTokenRefusesASymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "real")
	if err := os.WriteFile(target, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "host.token")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadToken(link); err == nil || !strings.Contains(err.Error(), "mode 0600") {
		t.Fatalf("ReadToken error = %v", err)
	}
}

func TestReadTokenNamesTheMissingAndTheEmptyFile(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "host.token")
	if _, err := ReadToken(missing); err == nil || !strings.Contains(err.Error(), "create "+missing) {
		t.Fatalf("missing: %v", err)
	}
	if err := os.WriteFile(missing, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadToken(missing); err == nil || !strings.Contains(err.Error(), "is empty") {
		t.Fatalf("empty: %v", err)
	}
}
