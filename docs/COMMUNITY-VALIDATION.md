# Community web and CLI validation — 2026-09-17

All execution described here used temporary local databases and processes.
No production deployment, merge, release, real Stripe key, or charge was made.
The candidate remains in PR #58. This evidence establishes working client and
server flows; it is not a production load or model-quality certification.

## Findings fixed in this sweep

- The CLI silently truncated responses at 1 MiB, even though the portal allows
  4 MiB. It now returns complete bounded JSON and rejects oversized or malformed
  responses instead of reporting success with broken output.
- Incompatible CLI flags were checked after login and, in one case, after local
  indexing. Intent and query validation now run before network, stdin or index
  side effects. Credits cannot silently ignore a supplied contribution.
- CLI cancellation prevented its deferred logout. Session revocation now uses
  an independent five-second cleanup context.
- Logout during an active web request could leave Search disabled. Delayed
  saved requests, credit responses, checkout redirects and unauthorized responses
  could affect the next account. Account changes now abort outstanding requests,
  invalidate stale responses, clear private UI state, and restore controls. A
  late startup session check cannot overwrite a newer login.
- A lost index response or failed reward write could lose contribution credit
  because a recrawl reported the page as no longer novel. The backend now journals
  submission intent and its receipt in Pebble before acknowledging success.
  Retries retain eligibility across restart; a submission ID is bound to its
  payload, existing canonical URLs remain ineligible, and the ledger still
  deduplicates rewards globally by content hash.
- Broken or unrelated backend acknowledgements could silently remove a job from
  the retry queue. Malformed JSON, missing acknowledgements, wrong queued URLs,
  and invalid indexed-content hashes now leave the job pending for retry.

Regression tests reproduced the original CLI, account-state, lost-receipt and
malformed-acknowledgement failures before their fixes.

## Executed flows

| Flow | Evidence |
| --- | --- |
| Signup, email/password login, logout, interests | Actual browser against the built local community service; interests and saved requests survived login/logout. |
| Search | Actual browser and built `cosift request` queried a real local Pebble index and returned contributed Go documentation. |
| Answer and Research | Actual browser and built CLI traversed community → native Cosift endpoints → a local deterministic chat fixture. Verified answer text, planning, source links and citation rendering. |
| Saved requests | Saved the same query separately as Search, Answer and Research; reran saved Answer in the browser. Account-isolation and deletion checks also run in the Go suite. |
| URL list contribution | Actual browser submitted a list containing one duplicate and one new URL; only the new page was accepted. Private-network URLs were rejected. |
| CSV | Actual file chooser uploaded a two-row CSV; duplicate/new counts were correct. Sample link emitted a download; HTTP check verified CSV content and attachment headers. |
| Local indexing | Built `cosift contribute -index-locally` fetched a public Go documentation page, persisted text/passages locally, and uploaded metadata and embeddings to the portal. The guarded backend validated and indexed it. |
| Earned credits | The CLI contribution appeared as Indexed in the browser with a 10-credit balance. Web URL contributions also earned credits. Unit tests cover content deduplication, concurrent spending and failure refunds. |
| Guest access | Actual browser searched successfully, then received a cooldown on an immediate repeat. Automated tests cover shared aliases, different mode caps, concurrent access, restart persistence and failure refunds. |
| Mid-request logout | Started a delayed Answer and logged out; login restored an empty working search form and the correct account's saved requests. Seven JavaScript regressions cover stale-response and cancellation cases. |
| Safety and quality | Automated fixtures cover adult URLs/metadata, unsafe redirects, private egress, phishing verdicts, uncertain/invalid moderation, spam, placeholder/bot pages, forged vectors and content/model mismatches. Rejected material never receives credit. |
| Stripe | Automated HTTP fixtures and signed SDK events cover checkout prices, signatures, paid/unpaid states, retries, concurrent fulfillment, refunds and test/live isolation. Missing keys hid payments in the actual browser and CLI response. |

The live local integration used `pebble-serve` on port 17785, `community` on
17786, and a deterministic OpenAI-compatible model fixture on 17787. The model
fixture returned two-dimensional vectors and fixed chat/moderation responses;
no inference-quality claim follows from those responses. Public webpage fetch,
local persistence, artifact comparison, native indexing/retrieval, account
storage and browser/CLI HTTP paths were real.

## Repeatable checks

```sh
node --test internal/community/webtests/*.test.cjs
GOWORK=off go test -race ./cmd/cosift -run TestCommunity -count=1
GOWORK=off go test -race ./internal/community -count=1
GOWORK=off go vet ./...
GOWORK=off go test -race -timeout 10m ./...
GOWORK=off make smoke
GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o /tmp/cosift-candidate ./cmd/cosift
```

Full race tests, vet, Linux ARM64 compilation, JavaScript regressions, and the
real crawl/serve smoke passed locally. The additional binary integration test
runs actual CLI subprocesses against the real community HTTP handler and a
controlled retrieval backend; it does not start the contribution worker or
require internet access. JavaScript regressions are now included in PR CI.

## Remaining activation checks

- Complete a real Stripe test-mode Checkout/webhook/refund exercise with an
  isolated account database before adding live keys; synthetic signed events
  do not prove an externally configured webhook or merchant account works.
- Compare real-model Search/Answer/Research quality and latency against the
  retained production baseline. Verify moderation with the intended model.
  Text/metadata checks are not an image/video safety classifier.
- Confirm the quota policy before selling extra capacity. Credits buy requests
  after the shared free allowance, and never bypass mode caps. With the default
  60 free shared requests and Search capped at 60/minute, credits help mixed-mode
  workloads; they do not increase a search-only user's 60/minute ceiling.
- Follow the existing operator handoff, including corpus readiness, backups,
  matching portal/engine versions and public routing compatibility. Durable
  receipt keys are included with the engine's Pebble data and must be retained
  alongside community ledger backups. Upgrade the backend before the portal;
  an older backend cannot provide durable reward receipts.
