package heroshi

import (
	"strings"
)

// Given a string of the form "host", "host:port", or "[ipv6::address]:port",
// return true if the string includes a port.
func HasPort(s string) bool { return strings.LastIndex(s, ":") > strings.LastIndex(s, "]") }

// Splits host(address) and port.
// "google.com:80" -> "google.com", "80"
// "[::1]" -> "[::1]", ""
func SplitPort(s string) (string, string) {
	v6End := strings.LastIndex(s, "]")
	colonPos := strings.LastIndex(s, ":")
	if colonPos > v6End {
		// "google.com:80" or "[::1]:53"
		return s[0:colonPos], s[colonPos+1 : len(s)]
	}
	// "example.com" or "[::1]"
	return s, ""
}
