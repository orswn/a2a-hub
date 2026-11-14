package config

import (
	"fmt"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
)

var (
	hostLabel = `[a-z0-9_](?:[a-z0-9_-]{0,61}[a-z0-9_])?`
	hostName  = regexp.MustCompile(`^` + hostLabel + `(?:\.` + hostLabel + `)*$`)
	cidrBits  = regexp.MustCompile(`^\d{1,3}$`)
)

func isIP(text string) bool {
	_, err := netip.ParseAddr(text)
	return err == nil
}

func isIPv6(text string) bool {
	addr, err := netip.ParseAddr(text)
	return err == nil && addr.Is6()
}

// ParsePushAllow parses a comma list of host names, IP addresses, and CIDRs into lower case entries.
func ParsePushAllow(value string) ([]string, error) {
	entries := []string{}
	for _, part := range strings.Split(value, ",") {
		entry := strings.ToLower(strings.TrimFunc(part, isJSSpace))
		if entry == "" {
			continue
		}
		if ip, bits, isCIDR := strings.Cut(entry, "/"); isCIDR {
			maxBits := 32
			if isIPv6(ip) {
				maxBits = 128
			}
			n, _ := strconv.Atoi(bits)
			if !isIP(ip) || !cidrBits.MatchString(bits) || n > maxBits {
				return nil, fmt.Errorf("push allow entry %s is not a valid CIDR", entry)
			}
		} else if name := strings.TrimSuffix(entry, "."); !isIP(entry) && (len(name) > 253 || !hostName.MatchString(name)) {
			return nil, fmt.Errorf("push allow entry %s is not a host name, address, or CIDR", entry)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
