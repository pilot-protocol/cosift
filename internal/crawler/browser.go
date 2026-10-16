package crawler

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// BrowserMajor is the Chrome major version BrowserUserAgent and the Sec-Ch-Ua header claim.
const BrowserMajor = 155

const BrowserUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36"

// BrowserHeaders returns a fresh copy of the headers a desktop Chrome sends for a top-level navigation.
func BrowserHeaders() http.Header {
	h := http.Header{}
	h.Set("User-Agent", BrowserUserAgent)
	h.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7")
	h.Set("Accept-Language", "en-US,en;q=0.9")
	h.Set("Sec-Ch-Ua", fmt.Sprintf(`"Google Chrome";v="%d", "Chromium";v="%d", "Not?A_Brand";v="24"`, BrowserMajor, BrowserMajor))
	h.Set("Sec-Ch-Ua-Mobile", "?0")
	h.Set("Sec-Ch-Ua-Platform", `"Windows"`)
	h.Set("Sec-Fetch-Dest", "document")
	h.Set("Sec-Fetch-Mode", "navigate")
	h.Set("Sec-Fetch-Site", "none")
	h.Set("Sec-Fetch-User", "?1")
	h.Set("Upgrade-Insecure-Requests", "1")
	return h
}

var privateHostSuffixes = []string{".localhost", ".local", ".internal", ".home", ".lan", ".test", ".invalid"}

// CheckPublicTarget refuses a URL that is not an http(s) page on a public hostname and a web port.
func CheckPublicTarget(u *url.URL) error {
	host := strings.ToLower(u.Hostname())
	if (u.Scheme != "http" && u.Scheme != "https") || host == "" || u.User != nil || u.Opaque != "" {
		return fmt.Errorf("%w: not a plain http(s) URL", errNonPublicTarget)
	}
	if port := u.Port(); port != "" && port != "80" && port != "443" {
		return fmt.Errorf("%w: non-web port denied", errNonPublicTarget)
	}
	if net.ParseIP(host) != nil || !strings.Contains(host, ".") || strings.HasSuffix(host, ".") {
		return fmt.Errorf("%w: not a public hostname", errNonPublicTarget)
	}
	for _, suffix := range privateHostSuffixes {
		if strings.HasSuffix(host, suffix) {
			return fmt.Errorf("%w: not a public hostname", errNonPublicTarget)
		}
	}
	return nil
}

// PublicRedirects returns a copy of c that follows at most five redirects and refuses any hop CheckPublicTarget refuses.
func PublicRedirects(c *http.Client) *http.Client {
	out := *c
	out.CheckRedirect = func(r *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return fmt.Errorf("too many redirects")
		}
		return CheckPublicTarget(r.URL)
	}
	return &out
}

// BrowserClient is PublicHTTPClient with every redirect hop re-checked.
func BrowserClient(timeout time.Duration) *http.Client {
	return PublicRedirects(PublicHTTPClient(timeout))
}
