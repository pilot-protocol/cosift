// Package netguard refuses outbound connections to any address outside the
// public unicast internet, so an attacker-supplied URL cannot reach cloud
// metadata, loopback, or RFC1918 space.
package netguard

import (
	"errors"
	"net/netip"
)

// ErrBlocked wraps every refusal so callers can errors.Is it.
var ErrBlocked = errors.New("netguard: refused to dial non-public address")

var blockedV4 = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
}

var (
	globalUnicastV6 = netip.MustParsePrefix("2000::/3")
	sixToFourV6     = netip.MustParsePrefix("2002::/16")
	teredoV6        = netip.MustParsePrefix("2001::/32")

	blockedV6 = []netip.Prefix{
		netip.MustParsePrefix("::ffff:0:0/96"),
		netip.MustParsePrefix("64:ff9b::/96"),
		netip.MustParsePrefix("2001:2::/48"),
		netip.MustParsePrefix("2001:10::/28"),
		netip.MustParsePrefix("2001:db8::/32"),
		netip.MustParsePrefix("3fff::/20"),
	}
)

// Allowed reports whether addr is a globally routable unicast address.
func Allowed(addr netip.Addr) bool {
	if !addr.IsValid() || addr.Zone() != "" {
		return false
	}
	if addr.Is4() {
		return addr.IsGlobalUnicast() && !inAny(blockedV4, addr)
	}
	if inAny(blockedV6, addr) {
		return false
	}
	if !addr.IsGlobalUnicast() || !globalUnicastV6.Contains(addr) {
		return false
	}
	if sixToFourV6.Contains(addr) {
		return Allowed(embeddedV4(addr, 2, false))
	}
	if teredoV6.Contains(addr) {
		return Allowed(embeddedV4(addr, 4, false)) && Allowed(embeddedV4(addr, 12, true))
	}
	return true
}

func inAny(prefixes []netip.Prefix, addr netip.Addr) bool {
	for _, p := range prefixes {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}

// embeddedV4 lifts the 4 bytes at off out of a v6 address; complement undoes
// the bitwise inversion Teredo applies to the client address.
func embeddedV4(addr netip.Addr, off int, complement bool) netip.Addr {
	b := addr.As16()
	var v4 [4]byte
	for i := range v4 {
		v4[i] = b[off+i]
		if complement {
			v4[i] = ^v4[i]
		}
	}
	return netip.AddrFrom4(v4)
}
