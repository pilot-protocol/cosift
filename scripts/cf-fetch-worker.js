// cosift CF-Worker fetcher add-on
//
// Run this on a Cloudflare Worker (one per region if you want, or just one).
// cosift's crawler will POST URLs to it; the Worker fetches the target site
// from CF's edge and returns the body to cosift. Lets a single cosift box
// effectively crawl from CF's 300+ POPs without per-IP rate limiting on the
// target side.
//
// Deploy with wrangler:
//   wrangler init cosift-fetch
//   # paste this file in src/index.js
//   wrangler deploy
//
// Set the shared bearer in your Worker env (via dashboard or wrangler secret):
//   wrangler secret put COSIFT_TOKEN
//   (paste a long random string; same value goes in cosift.json
//   crawler.remote_fetcher_token)
//
// Free tier: 100 K requests / day per worker. Paid: $5 / 10 M.
//
// Request shape (POST application/json):
//   {
//     "url":     "https://example.com/page",
//     "user_agent": "Mozilla/5.0 ...",   // optional
//     "headers": { "Accept": "text/html" } // optional extras
//   }
//
// Response: raw body bytes, headers:
//   Content-Type:    upstream's content-type
//   X-Final-URL:     fully-resolved URL after redirects
//   X-Origin-Status: upstream HTTP status (always 200 from the Worker
//                    itself unless the Worker errored; cosift consults
//                    this header to detect upstream 4xx/5xx)
//   X-Origin-IP:     CF colo that egressed
//
// On Worker error (network failure, CF block) → HTTP 502 with JSON
// {"error": "..."}.

export default {
  async fetch(request, env, ctx) {
    if (request.method !== "POST") {
      return new Response("POST only", { status: 405 });
    }
    const auth = request.headers.get("authorization") || "";
    if (env.COSIFT_TOKEN && auth !== `Bearer ${env.COSIFT_TOKEN}`) {
      return new Response("unauthorized", { status: 401 });
    }
    let req;
    try {
      req = await request.json();
    } catch {
      return new Response("bad json", { status: 400 });
    }
    if (!req.url) return new Response("missing url", { status: 400 });

    const headers = new Headers();
    headers.set("User-Agent", req.user_agent || "Mozilla/5.0 (cosift-cf-fetcher)");
    for (const [k, v] of Object.entries(req.headers || {})) {
      headers.set(k, String(v));
    }

    let upstream;
    try {
      upstream = await fetch(req.url, {
        method: "GET",
        headers,
        redirect: "follow",
        cf: { cacheTtl: 0, cacheEverything: false, scrapeShield: false },
      });
    } catch (e) {
      return new Response(JSON.stringify({ error: String(e) }), {
        status: 502,
        headers: { "Content-Type": "application/json" },
      });
    }

    const body = await upstream.arrayBuffer();
    return new Response(body, {
      status: 200, // we ALWAYS return 200 from the worker; X-Origin-Status carries the real status
      headers: {
        "Content-Type":    upstream.headers.get("content-type") || "application/octet-stream",
        "X-Final-URL":     upstream.url,
        "X-Origin-Status": String(upstream.status),
        "X-Origin-IP":     request.cf?.colo || "unknown",
      },
    });
  }
};
