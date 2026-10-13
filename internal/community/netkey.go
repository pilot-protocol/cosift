package community

import "net/netip"

// netKey is the rate-limit identity for a client address: the address for IPv4, the /64 for IPv6.
func netKey(ip string) string {
	a, err := netip.ParseAddr(ip)
	if err != nil {
		return ip
	}
	if a = a.Unmap(); a.Is4() {
		return a.String()
	}
	return netip.PrefixFrom(a.WithZone(""), 64).Masked().String()
}
