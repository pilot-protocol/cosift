# Contributing to cosift

Thanks for considering a contribution. The patterns below are the working
conventions of the codebase — following them keeps the project consistent and
reviewable.

## Getting started

```bash
go build -o cosift ./cmd/cosift
go test ./...
./cosift init && ./cosift serve
```

The full unit-test suite runs in well under a minute. Run it before every PR.
No external services are required to develop locally — tests use `httptest`
plus stub embedders / chat clients to keep the suite hermetic.

## Dependency policy

Cosift has **three deps**, deliberately:

| Dep                            | Why                                              |
|--------------------------------|--------------------------------------------------|
| `modernc.org/sqlite`           | Pure-Go SQLite — no cgo, single binary           |
| `golang.org/x/net/html`        | HTML parsing for the crawler                     |
| `ledongthuc/pdf`               | PDF text extraction                              |

A fourth dep requires explicit justification. "Saves writing 50 lines" is not
enough — the standard library is usually adequate for cosift's domain (HTTP
servers, JSON, text processing, channel-based concurrency). See
[`DEPS.md`](DEPS.md) for the rule of thumb and rejected alternatives.

## No cgo, no GPU

Cosift runs on VPS-class hardware. cgo means platform-specific build pain,
larger binaries, and harder-to-debug crashes. GPU access means non-portability.
Both are out unless there's an extraordinary reason.

LLM / embedding / rerank calls go out over HTTP to OpenAI-compatible endpoints.
That's where the heavy compute happens — not in-process.

## Test discipline

### Every PR ships tests

Each feature, bug fix, or behavior-changing PR introduces at least one test
that locks in the new behavior. The bar is **a test that fails before the
change and passes after**, not coverage percentage.

### Helper extraction at N=3

The codebase tolerates one or two copies of similar logic. Once a *third* call
site appears that needs the same logic, extract a shared helper at that
moment. Premature abstraction (extracting at N=2) leaves abstractions whose
shape gets revealed wrong by the third use; deferred abstraction (waiting
until N=4+) leaves drift between near-identical sites.

### Stub embedders / chat clients

Tests of `/search`, `/answer`, `/research` etc. that need an embedder use the
in-tree stubs (see `internal/server/*_test.go`). The stubs return
deterministic vectors / responses and let the test assert on every call's
inputs. Don't hit real OpenAI / TEI endpoints in unit tests.

### E2E tests for cross-package contracts

Some bugs only surface at the seam between subcommands (e.g. crawl writes,
serve reads; init writes config, crawl reads it). These get covered with
`cmd/cosift/*_e2e_test.go` tests that build the binary and run subprocesses
against an `httptest`-served fake site. Slower (~3 s each) but they catch
real-binary regressions that unit tests miss.

### Real-runner smoke test

`scripts/smoke-test.sh` (invoked via `make smoke`) builds the binary, crawls a
real public seed, exercises the full operator surface, and tears down. Run it
before PRs that touch the crawler, the store, or the HTTP layer — it catches
the class of bug that unit tests against in-memory shortcuts can miss (WAL
multi-writer races, real-HTML edge cases, real-content fetches).

The smoke test does NOT exercise `/answer`, `/research`, `/find_similar`, or
`?hyde=` — those need an LLM key. A separate `make smoke-test-llm` track is
planned for keyed coverage.

## Destructive operation guards

The `cosift admin recrawl`, `recrawl-by-domain`, and `reembed` subcommands
mutate the index in ways that can be expensive (LLM credits) or destructive
(dropping passages). Their patterns:

- **`-y` for confirmation.** Destructive operations refuse to proceed without
  explicit `-y`. The flag exists to keep an operator from accidentally pasting
  a copy-pasted command.
- **`-dry-run` for previews.** Each destructive subcommand has a `-dry-run`
  that runs the same query path and prints what *would* change — including a
  count and a sample list — without writing.
- **Safer-on-conflict.** Where two flag combinations could conflict, the
  safer one wins. `-dry-run` overrides `-y` (preview rather than execute);
  `-since DATE` narrows scope rather than expanding it.

When adding a new operator surface that mutates state, follow the same
pattern: dry-run first, then `-y`-gated execute.

## Eval gate

Retrieval-affecting changes must be measured before merging.

```bash
# Local baseline
./cosift eval -retriever bm25 -save mine.json
./cosift eval -retriever dense -rerank -save mine-dense.json

# Diff against a saved baseline
./cosift eval -baseline mine.json -retriever bm25
```

For answer-quality changes:

```bash
./cosift answer-eval -corpus my-corpus.json -queries my-queries.json -save run.json
./cosift answer-eval-compare /tmp/before.json /tmp/after.json
```

Use a smaller `-synth-model` than `-judge-model` to avoid self-preference
bias. Cost is bounded by the corpus size and the per-query LLM calls.

PRs that touch retrieval should include the eval-diff numbers in the
description.

## Commit and PR style

- One topic per PR. Mixed bug fixes + features make review harder.
- Clear commit messages: imperative mood, present tense ("add X" not
  "added X"). The first line summarizes; the body explains *why*.
- Update relevant docs in the same PR. README and DEPS.md drift fast if not
  touched alongside code.
- If you add or remove a HTTP query parameter, update the README's
  "Retrieval pipeline" table.

## Operator-facing surface

When in doubt about whether a feature is operator-visible:

- If it changes the HTTP wire format, it's user-visible — document in README.
- If it changes the on-disk schema, run the migration with `IF NOT EXISTS`
  guards. The schema migrates forward automatically on startup; old data dirs
  must work with new binaries.
- If it adds a CLI subcommand, add it to the CLI reference table in README.
- If it adds an `/admin/*` endpoint, document it in the admin section and
  exercise auth-on / auth-off paths in tests.

That's it. Open a PR and we'll iterate.
