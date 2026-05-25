# CLI flag design — why `-json`, `-format`, and `-text` aren't unified

**Status:** design closed. Output-mode rationalization was considered through
iters 102-114 (14 priorities lists in a row) and formally closed in iter 115.
This document explains why the current design is correct, so future contributors
don't re-open the same question.

## Current state

Across the cosift CLI, three different flags affect output:

| Flag         | Commands using it                                    | What it does                                                |
|--------------|------------------------------------------------------|-------------------------------------------------------------|
| `-json`      | `query`, `search`, `research`, `find-similar`, `answer`, `contents`, `admin stats/config/recrawl/recrawl-domain` | Pass through raw API JSON response (no rendering)           |
| `-format`    | `search`, `research`, `find-similar`, `answer` (text\|md), `export`, `ingest` (json\|jsonl\|text\|md) | Select rendering format for the response                    |
| `-text`      | `contents`                                            | Strip metadata, output only document body text              |

`admin reembed` is special: `-json` on it passes through the **raw SSE event stream**
rather than a terminal JSON blob, because reembed's response IS an event stream
(no terminal blob to render).

## Why three flags instead of one

The tempting unification is `-format text|md|json` everywhere. It would be wrong
for one specific reason: **`-format` controls RENDERING, `-json` controls
TRANSPORT, and they are orthogonal concerns on the synth CLIs.**

For `cosift search "..." -format md`:
- Transport: server returns JSON via HTTP
- Rendering: CLI parses JSON, emits markdown-rendered hits to stdout

For `cosift search "..." -json`:
- Transport: server returns JSON via HTTP
- Rendering: CLI passes the JSON bytes straight to stdout (no parse, no render)

If `-format json` meant "raw passthrough" on synth CLIs (like `-json` does today),
operators would have two ways to spell the same thing — confusable code paths
without clear precedence.

If `-format json` meant something different from `-json`, we'd have two flags
that look related but don't compose:
- `-format text` (default) + `-json` → raw JSON output
- `-format json` (new) + `-json` → ??? (both already-set bools? error? precedence?)

Both unifications are footguns. iter-103 caught this when adding `-format json`
to export:

> "The synth CLIs already have `-json` as a separate flag, so accepting `json`
> in their `-format` would create two confusable code paths. Export doesn't
> have `-json` because it always writes to a file (the format IS the wire
> shape). Different validators reflect a real semantic difference; merging
> would just paper over it."

`export` is the exception that proves the rule: it writes to a file (the
`-output` PATH), so the wire format and the rendering are the same axis. One
flag (`-format`) is sufficient. Synth CLIs write to stdout, where the two
axes are genuinely independent.

## Why `contents -text` exists

`cosift contents` returns document text. The metadata vs body axis is:

| Mode              | Output                                                  |
|-------------------|---------------------------------------------------------|
| (default)         | URL: ..., Title: ..., Status: cached, Text: ...         |
| `-text`           | Body text only, no headers                              |
| `-json`           | Raw `ContentsResponse` JSON (with URL, Title, Text, ...) |
| `-text -json`     | Mutually exclusive (would be ambiguous); fails clearly  |

`-text` is its own dimension — strip metadata vs include metadata — orthogonal
to both rendering format and transport. Calling it `-format text` would have
been the wrong choice because:

- The default output already IS "text" (just with metadata).
- `cosift contents -text -file urls.txt` pairs `-text` with batch input. The
  batch separator (`---`) makes more sense as "concat docs as text" than
  "format as text format".

It's a one-off, not part of a system. Naming it specifically (`-text`) reflects
that.

## What would change the calculus

Three scenarios that would warrant re-opening this design:

1. **A real operator hits the confusion.** If two operators in succession
   report "I expected `cosift search ... -format json` to work like `-json`",
   that's signal. So far, every analysis pass has come from inside the project.

2. **A fourth output mode appears.** The current three flags carve a clean
   space: `-json` for transport, `-format` for rendering, `-text` for the
   contents-specific metadata-stripping axis. A fourth mode (XML? YAML?
   binary?) would force re-evaluation.

3. **A CLI generator framework gets adopted.** If cosift moves off `flag`
   to something like cobra/spf13 with subcommand grouping and flag inheritance,
   the cost of restructuring drops and the unification becomes cheaper.

None of these conditions are met as of iter 115. The current design has
shipped successfully across 16 commands and 14 iters of operator-facing
work without confusion in practice.

## Backward-compatibility cost of any future change

If output rationalization is revisited:
- `-json` is a positional bool flag in current scripts. Changing its meaning
  would break those scripts silently.
- `-format md` aliasing exists (md == markdown); precedent for accepting
  multiple spellings of a value.
- The right migration path is probably: ADD `-format json` to synth CLIs as
  an alias for `-json`, deprecate `-json` with a warning, drop it in a
  major version. ~3-iter arc, low individual risk per iter.

But the cost is paying for backward-compat with no offsetting user-facing
win. Operators get to type `-format json` instead of `-json` — saving exactly
zero confusion in practice.

## Conclusion

The current design has a clear semantic — `-format` is for rendering,
`-json` is for transport passthrough, `-text` is the contents-specific
metadata-stripping axis. Three flags carve three orthogonal axes. Unifying
into one flag would conflate axes and ship a footgun.

**Closed in iter 115. Reopen if any of the three conditions in "What would
change the calculus" comes true.**
