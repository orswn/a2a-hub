// Package pushguard decides which push webhook addresses the hub may reach.
package pushguard

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"syscall"

	"github.com/orswn/a2a-hub/internal/config"
)

// Resolver returns the addresses of a host name.
type Resolver func(ctx context.Context, host string) ([]string, error)

// DefaultResolver asks the system resolver.
func DefaultResolver(ctx context.Context, host string) ([]string, error) {
	return net.DefaultResolver.LookupHost(ctx, host)
}

// denied lists loopback, RFC 1918, link-local, ULA, CGNAT, unspecified, and multicast.
var denied = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("::/128"),
	netip.MustParsePrefix("::1/128"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("ff00::/8"),
}

// Guard holds the allow list. The zero value denies every private address and allows the rest.
type Guard struct {
	allowed []netip.Prefix
	hosts   map[string]bool
	resolve Resolver
}

// TestAgentAllow is the loopback that test mode adds to the allow list, so the TCK can reach its own webhook.
var TestAgentAllow = []string{"127.0.0.0/8", "::1"}

// New makes a guard from allow list entries: host names, addresses, and CIDRs. A nil resolver uses the system.
func New(allow []string, resolve Resolver) (*Guard, error) {
	entries, err := config.ParsePushAllow(strings.Join(allow, ","))
	if err != nil {
		return nil, err
	}
	g := &Guard{hosts: map[string]bool{}, resolve: resolve}
	for _, entry := range entries {
		if prefix, err := netip.ParsePrefix(entry); err == nil {
			g.allowed = append(g.allowed, unmapPrefix(prefix).Masked())
		} else if addr, err := netip.ParseAddr(entry); err == nil {
			addr = addr.Unmap()
			g.allowed = append(g.allowed, netip.PrefixFrom(addr, addr.BitLen()))
		} else {
			g.hosts[strings.TrimSuffix(entry, ".")] = true
		}
	}
	return g, nil
}

func unmapPrefix(p netip.Prefix) netip.Prefix {
	if p.Addr().Is4In6() && p.Bits() >= 96 {
		return netip.PrefixFrom(p.Addr().Unmap(), p.Bits()-96)
	}
	return p
}

// AddressAllowed reports whether the hub may send to ip. An IPv4-mapped IPv6 address counts as its IPv4 address,
// and a zoned address counts as the address without its zone.
func (g *Guard) AddressAllowed(ip netip.Addr) bool {
	ip = ip.WithZone("").Unmap()
	if g != nil {
		for _, p := range g.allowed {
			if p.Contains(ip) {
				return true
			}
		}
	}
	for _, p := range denied {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}

// CheckHost is the config time check of a URL host: it refuses an IP literal in a denied range and leaves
// host names to send time. It returns the reason for a refusal, or "".
func (g *Guard) CheckHost(host string) string {
	ip, ok := hostAddr(host)
	if !ok || g.AddressAllowed(ip) {
		return ""
	}
	return fmt.Sprintf("push url address %s is not allowed", host)
}

// CheckSend is the send time check: every resolved address must be allowed. It returns the reason for a
// refusal, or "".
func (g *Guard) CheckSend(ctx context.Context, rawURL string) string {
	u, err := ParseURL(rawURL)
	if err != nil {
		return err.Error()
	}
	host := u.Host
	if g != nil && g.hosts[host] {
		return ""
	}
	if ip, ok := hostAddr(host); ok {
		if g.AddressAllowed(ip) {
			return ""
		}
		return fmt.Sprintf("push url address %s is not allowed", host)
	}
	resolve := DefaultResolver
	if g != nil && g.resolve != nil {
		resolve = g.resolve
	}
	addresses, err := resolve(ctx, host)
	if err != nil {
		return fmt.Sprintf("cannot resolve %s: %v", host, err)
	}
	if len(addresses) == 0 {
		return "cannot resolve " + host
	}
	for _, address := range addresses {
		ip, err := netip.ParseAddr(address)
		if err != nil || !g.AddressAllowed(ip) {
			return fmt.Sprintf("push host %s resolves to %s, which is not allowed", host, address)
		}
	}
	return ""
}

// DialContext dials like base, but refuses a connection to an address that the guard does not allow. The check
// runs on the address of the socket, so a DNS change between CheckSend and the dial cannot reach a denied
// address. A host name on the allow list skips the check, as in CheckSend.
func (g *Guard) DialContext(base *net.Dialer) func(ctx context.Context, network, address string) (net.Conn, error) {
	guarded := *base
	guarded.Control = func(_, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return err
		}
		ip, err := netip.ParseAddr(host)
		if err != nil || !g.AddressAllowed(ip) {
			return fmt.Errorf("push address %s is not allowed", host)
		}
		return nil
	}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		if g != nil && g.hosts[strings.TrimSuffix(strings.ToLower(host), ".")] {
			return base.DialContext(ctx, network, address)
		}
		return guarded.DialContext(ctx, network, address)
	}
}

func hostAddr(host string) (netip.Addr, bool) {
	ip, err := netip.ParseAddr(host)
	return ip, err == nil
}

// URL is a push URL with its host in the form a WHATWG URL parser gives: lower case, no brackets or trailing
// dot, and an IPv4 address in dotted decimal even when the URL writes it as one number or in hex.
type URL struct {
	Scheme string
	Host   string
}

// ErrInvalidURL is the error of ParseURL for a string that is not a URL with a host.
var ErrInvalidURL = errors.New("push config url is not a valid URL")

// ParseURL reads an absolute URL. The scheme is lower case. A URL of the http or https scheme must have a host.
func ParseURL(raw string) (URL, error) {
	raw = strings.Trim(raw, "\x00\t\n\f\r ")
	if scheme, rest, ok := strings.Cut(raw, ":"); ok && (strings.EqualFold(scheme, "http") || strings.EqualFold(scheme, "https")) {
		// The WHATWG parser takes any run of slashes and backslashes after a special scheme as "//".
		raw = scheme + "://" + strings.TrimLeft(rest, `/\`)
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" {
		return URL{}, ErrInvalidURL
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return URL{Scheme: scheme}, nil
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return URL{}, ErrInvalidURL
	}
	if strings.HasPrefix(u.Host, "[") {
		ip, err := netip.ParseAddr(host)
		if err != nil || !ip.Is6() || ip.Zone() != "" {
			return URL{}, ErrInvalidURL
		}
		return URL{Scheme: scheme, Host: formatIPv6(ip)}, nil
	}
	host = strings.TrimSuffix(host, ".")
	if ip, numeric, ok := whatwgIPv4(host); numeric {
		if !ok {
			return URL{}, ErrInvalidURL
		}
		host = ip.String()
	}
	return URL{Scheme: scheme, Host: host}, nil
}

// whatwgIPv4 parses a host the way the WHATWG URL parser does when its last label is a number: one to four
// parts in decimal, hex (0x), or octal (leading 0). numeric is false for a host name.
func whatwgIPv4(host string) (ip netip.Addr, numeric, ok bool) {
	parts := strings.Split(host, ".")
	if !ipv4Number(parts[len(parts)-1]) {
		return netip.Addr{}, false, false
	}
	if len(parts) > 4 {
		return netip.Addr{}, true, false
	}
	values := make([]uint64, len(parts))
	for i, part := range parts {
		n, good := parseIPv4Part(part)
		if !good {
			return netip.Addr{}, true, false
		}
		values[i] = n
	}
	for _, n := range values[:len(values)-1] {
		if n > 255 {
			return netip.Addr{}, true, false
		}
	}
	last := values[len(values)-1]
	if last >= 1<<(8*(5-len(values))) {
		return netip.Addr{}, true, false
	}
	addr := last
	for i, n := range values[:len(values)-1] {
		addr += n << (8 * (3 - i))
	}
	return netip.AddrFrom4([4]byte(binary.BigEndian.AppendUint64(nil, addr)[4:])), true, true
}

func ipv4Number(part string) bool {
	if part == "" {
		return false
	}
	if strings.HasPrefix(part, "0x") {
		for _, c := range part[2:] {
			if !strings.ContainsRune("0123456789abcdef", c) {
				return false
			}
		}
		return true
	}
	for _, c := range part {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func parseIPv4Part(part string) (uint64, bool) {
	if part == "" {
		return 0, false
	}
	base := 10
	switch {
	case strings.HasPrefix(part, "0x"):
		part, base = part[2:], 16
		if part == "" {
			return 0, true
		}
	case len(part) > 1 && part[0] == '0':
		part, base = part[1:], 8
	}
	n, err := strconv.ParseUint(part, base, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return 1 << 40, true
		}
		return 0, false
	}
	return n, true
}

// formatIPv6 writes an IPv6 address like the WHATWG URL serializer: RFC 5952 compression and hex pieces, also
// for an IPv4-mapped address.
func formatIPv6(ip netip.Addr) string {
	if !ip.Is4In6() {
		return ip.String()
	}
	b := ip.As16()
	return fmt.Sprintf("::ffff:%x:%x", uint16(b[12])<<8|uint16(b[13]), uint16(b[14])<<8|uint16(b[15]))
}
