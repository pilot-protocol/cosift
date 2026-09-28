package wiki_test

import (
	_ "embed"
	"strings"
	"testing"
)

//go:embed testdata/rust-async-runtimes.md
var rustBody string

// Synthetic, publicly safe fixtures: example domains only.

type cite struct{ url, title, quote string }

type fixture struct {
	kind, id, slug, title, vertical, tier, lead, body string
	held                                              bool
	cites                                             []cite
}

// promoted is published in the pages' own environment at the default promotion_min_tier, ok.
func (f fixture) promoted() bool { return f.kind == "promoted" || f.kind == "ok" }

func stackID(n int) string {
	const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	b := []byte("01J9W7A0000000000000000000")
	for i := 25; n > 0; i, n = i-1, n/32 {
		b[i] = crockford[n%32]
	}
	return string(b)
}

// generated builds an AR §5.1 body from five cited facts about title.
func generated(title string, facts [5]string) string {
	return "## Overview\n\n" + facts[0] + " [1] " + facts[1] + " [2]\n\n" +
		"## Details\n\n" + facts[2] + " [1] " + facts[3] + " [2] " + facts[4] + " [1]\n\n" +
		"## Scope\n\n" + "This article summarises what the cited sources report about " + strings.ToLower(title) +
		". It is a synthetic fixture written for the page tests, so its facts are illustrative and its sources are placeholder addresses on example domains. " +
		"A real article follows the same shape: a lead that stands on its own, sections of dense factual prose, a list of key facts, and numbered markers that point at the sources below. " +
		"The page renders the lead and the body as one text, keeps every quote verbatim, and links out only to the cited addresses. " +
		"Readers who need more depth should follow the sources, which carry the full context that a short summary leaves out, including definitions, edge cases and the dates when each statement was true. " +
		"Nothing in this fixture refers to a person, an account or a request, and none of it should be read as advice.\n\n" +
		"## Background\n\n" + "Summaries like this one exist so that an agent can answer a question about " + strings.ToLower(title) + " in one step, with the evidence attached. " +
		"Each statement above carries a marker, each marker names a source, and each source is listed with the quote that supports it. " +
		"When the sources disagree or change, the article is rebuilt from the corpus rather than edited by hand, and the page shows the date of the last change.\n\n" +
		"## Key facts\n\n- " + facts[0] + " [1]\n- " + facts[1] + " [2]\n- " + facts[3] + " [2]\n"
}

func fixtures(t testing.TB) []fixture {
	two := func(slug string) []cite {
		return []cite{
			{"https://docs.example.org/" + slug + "/overview", "Overview of " + strings.ReplaceAll(slug, "-", " "), "The overview describes the main mechanisms and their trade-offs."},
			{"https://papers.example.net/" + slug + "/survey", "A survey of " + strings.ReplaceAll(slug, "-", " "), "The survey compares the approaches and reports where each one fails."},
		}
	}
	gen := func(kind string, n int, slug, title, vertical, tier string, facts [5]string) fixture {
		return fixture{kind: kind, id: stackID(n), slug: slug, title: title, vertical: vertical, tier: tier,
			lead: title + " is summarised here from two placeholder sources. The lead stands on its own for an agent that reads nothing else.",
			body: generated(title, facts), cites: two(slug)}
	}
	return []fixture{
		{kind: "promoted", id: stackID(1), slug: "rust-async-runtimes", title: "Rust async runtimes", vertical: "dev-docs", tier: "strong",
			lead: "Rust's async functions compile to state machines that do nothing until an executor polls them. Runtimes supply that executor together with I/O reactors, timers and task scheduling [1].",
			body: rustBody,
			cites: []cite{
				{"https://docs.example.org/async/runtime-tutorial", "Async runtime tutorial", "A runtime drives futures to completion by polling them when they can make progress."},
				{"https://docs.example.org/async/executors", "Executors and wakers", "A waker tells the executor that a task is ready to be polled again."},
				{"https://blog.example.net/work-stealing", "Work-stealing schedulers", "Idle worker threads steal queued tasks from busy ones."},
			}},
		gen("promoted", 2, "solid-state-battery-production", "Solid-state battery production", "technology-news", "strong", [5]string{
			"Solid-state cells replace the liquid electrolyte with a solid layer that conducts ions.",
			"Pilot lines report that stacking thin solid layers without defects is the main yield problem.",
			"Sulfide electrolytes conduct ions well but react with moisture, so production needs dry rooms.",
			"Oxide electrolytes are more stable in air but need high sintering temperatures.",
			"Cell makers describe pressure during cycling as a design constraint for pack housings.",
		}),
		gen("promoted", 3, "bloom-filter-variants", "Bloom filter variants", "research", "strong", [5]string{
			"A Bloom filter answers set membership with no false negatives and a tunable false-positive rate.",
			"Counting Bloom filters replace bits with small counters so that elements can be removed.",
			"Cuckoo filters store short fingerprints in buckets and support deletion with less space at low error rates.",
			"The false-positive rate of a classic filter depends on the bits per element and the number of hash functions.",
			"Blocked Bloom filters keep each element's bits in one cache line to reduce memory traffic.",
		}),
		gen("promoted", 4, "open-data-licences", "Open data licences", "other", "strong", [5]string{
			"Open data licences grant permission to use, share and adapt a dataset under stated conditions.",
			"Attribution licences require that reuse credits the source in the way the licence describes.",
			"Share-alike terms require that adapted datasets are released under the same licence.",
			"Public-domain dedications waive the rights a publisher could otherwise assert over the data.",
			"Database rights in some jurisdictions protect a collection separately from its individual records.",
		}),
		gen("ok", 5, "raft-consensus-algorithm", "Raft consensus algorithm", "research", "ok", [5]string{
			"Raft elects a leader that appends client commands to a replicated log.",
			"A command is committed once a majority of servers has stored it in their logs.",
			"Followers that do not hear from a leader within a randomised timeout start an election.",
			"Terms number each election, and a server rejects messages from an older term.",
			"Log matching ensures that two logs with an entry of the same index and term agree on all earlier entries.",
		}),
		{kind: "stub", id: stackID(6), slug: "webassembly-component-model", title: "WebAssembly component model", vertical: "dev-docs"},
		gen("tombstoned", 7, "deprecated-hash-functions", "Deprecated hash functions", "technology-news", "strong", [5]string{
			"Collision attacks make some older hash functions unsuitable for signatures.",
			"Standards bodies publish transition timelines for retiring weak algorithms.",
			"Legacy protocols often negotiate the hash function, which allows downgrade attacks.",
			"Password storage needs slow, salted functions rather than fast general-purpose hashes.",
			"Checksums for accidental corruption have weaker requirements than cryptographic hashes.",
		}),
		func() fixture {
			f := gen("held", 8, "container-image-signing", "Container image signing", "dev-docs", "strong", [5]string{
				"Image signing binds a digest of the image to an identity that verifiers trust.",
				"Signatures can be stored next to the image in the same registry.",
				"Admission policies reject images whose signatures do not verify.",
				"Transparency logs record signatures so that misuse of a key can be detected.",
				"Keyless signing issues short-lived certificates tied to a workload identity.",
			})
			f.held = true
			return f
		}(),
		gen("rejected", 9, "browser-fingerprinting-techniques", "Browser fingerprinting techniques", "technology-news", "strong", [5]string{
			"Fingerprinting combines many small browser properties into a stable identifier.",
			"Canvas and font measurements vary slightly across devices and drivers.",
			"Browsers reduce entropy by standardising or randomising exposed values.",
			"Privacy budgets limit how much identifying information a site can query.",
			"Fingerprints change when users update software, which limits their lifetime.",
		}),
		gen("other-env", 10, "service-mesh-sidecars", "Service mesh sidecars", "dev-docs", "strong", [5]string{
			"A sidecar proxy runs next to each service instance and handles its network traffic.",
			"Meshes use sidecars to add mutual TLS without changing application code.",
			"Each sidecar adds latency and memory use to every request path.",
			"Sidecar-less designs move proxy functions into a per-node agent.",
			"Traffic policies are pushed to sidecars from a central control plane.",
		}),
	}
}
