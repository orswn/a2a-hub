// Package hostauth finds the hub URL and reads the host token that the a2a CLI sends to the hub.
package hostauth

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// DefaultHubURL is the hub that a host uses when neither --hub nor A2A_HUB_URL names one.
const DefaultHubURL = "https://a2a-hub.rswn.dev"

// HubURL picks the hub base URL: the flag value, else env A2A_HUB_URL, else DefaultHubURL. It has no
// trailing slash.
func HubURL(flag string, getenv func(string) string) string {
	url := flag
	if url == "" {
		url = getenv("A2A_HUB_URL")
	}
	if url == "" {
		url = DefaultHubURL
	}
	return strings.TrimRight(url, "/")
}

// TokenFile is the host token file under home.
func TokenFile(home string) string {
	return filepath.Join(home, ".config", "a2a", "host.token")
}

// ReadToken reads a bearer token. The file must be a regular file of this user that no other user can read,
// and must not be a symlink.
func ReadToken(path string) (string, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0) //nolint:gosec // G304: the token path is fixed under the home directory.
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("no hub token: create %s with mode 0600", path)
		}
		if errors.Is(err, syscall.ELOOP) {
			return "", modeError(path)
		}
		return "", fmt.Errorf("cannot read hub token file %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("cannot read hub token file %s: %w", path, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !info.Mode().IsRegular() || !ok || int(stat.Uid) != os.Getuid() || info.Mode().Perm()&0o077 != 0 {
		return "", modeError(path)
	}
	raw, err := io.ReadAll(io.LimitReader(file, 64<<10))
	if err != nil {
		return "", fmt.Errorf("cannot read hub token file %s: %w", path, err)
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return "", fmt.Errorf("hub token file %s is empty", path)
	}
	return token, nil
}

func modeError(path string) error {
	return fmt.Errorf("hub token file %s must be a regular file owned by this user with mode 0600", path)
}
