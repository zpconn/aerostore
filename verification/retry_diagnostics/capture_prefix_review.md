# Explicit diagnostic baseline review for prior-prefix capture

This is a reviewed source transition, not a renewed claim that the native
program is unchanged from its original instrumentation baseline. The old
[`default_sources_4da551b.json`](default_sources_4da551b.json) remains byte-for-byte
identical to the manifest in commit `94ad54bef275dd4db0ce382569bc239e931ada87`.
Its original reference is `4da551bbc2a17614ae11795cfee7764cb252d56a`. The prior
baseline rejects the new native capture code; that rejection is tested.

The [reference OCC source](occ_partitioned_94ad54b.rs) was copied directly from
immutable commit `94ad54bef275dd4db0ce382569bc239e931ada87`, with SHA-256
`03719920d5553b5edc1f240a8e2490c96d67b5f0d0145d7be2285d3ffe97eb1a`.
It contains the existing diagnostic observations and still projects exactly to
the original default token stream. Checks use these retained bytes and never
consult mutable HEAD.

The complete current OCC token expectation is that reference plus exactly:

1. `let prior_read_len = tx.index_reads.len();` immediately before the second
   bucket loop, which reads and captures publication stamps.
2. Assign `previous` from a branch evaluated for each bucket. If
   `tx.index_reads.len() == prior_read_len`, search the whole vector with its
   existing `iter().find(...)`; otherwise search
   `tx.index_reads[..prior_read_len]` with the same predicate. Then use
   `if let Some(previous) = previous` for the unchanged stamp validation.
3. `#[cfg(test)] mod capture_prefix_tests;` appended at EOF. The new child-module
   test bodies are separate source-bound inputs; they are not native capture
   tokens and are not hidden by diagnostic normalization.

The requested buckets are unique after canonicalization. New dependencies
appended during this query therefore cannot match a later requested bucket;
the existing prefix must remain unchanged. Before any append, the whole vector
is exactly that prefix; after an append the bounded arm skips only new entries.
The length test is performed for every bucket, so an earlier append switches
the next search to the bounded arm. These are semantic premises, not
conclusions of this lexical checker. They require the updated capture and
composition proofs and native regressions. The edit does not change stamp
validation, source/destination publication, index identity comparisons, guard
lifetimes, abort paths, raw lookup or materialization. It can change concurrent
interleavings and retry counts through shorter capture work.

All 18 observation statements retain the same exact guards, arguments and
rejecting branches. Two 16-token context windows change because the neighboring
search expression changes. Immediately after `LookupPostSnapshotStamp`, the
window's search prefix changes from `if let Some(previous) =` to
`let previous = if tx.index_reads`. Immediately before
`LookupChangedCapturedStamp`, the window starts with `= previous {` instead of
`bucket) {`; its `if previous.stamp != stamp` and sticky assignment stay exact.
The checker requires these two exact old/new windows and rejects changes to
every other window. It does not skip or shorten context validation.

The first OCC observation's default-token position advances by 11; the remaining
16 advance by 65. These are the length declaration and hybrid search syntax,
not relocated instrumentation. The sole
WAL observation retains its original position and token contexts. WAL's full
default token SHA-256 remains
`dbad99d7aee277fb7d0806cd26104ab9ee2e9b6b7d8a0084547f96f4c02283bf`.

`normalize.py` is unchanged, with SHA-256
`c741c113689c7afe0ca94473cf9c029abfae647cefef656b4d3e40787ee7cb8c`.
It still erases only the finite, reviewed diagnostic calls. The prefix length
and both guarded search arms remain in its output. Its existing shared-generator hash pin
and standalone receipt freshness checks continue to apply.

`check_default.py` validates the historical manifest and reference source hashes,
derives the exact reviewed token transition independently of the current native
file, checks the two explicit context transitions and unchanged WAL projection, and then checks
the live default tokens and site positions. Updating a current expected digest
alone cannot admit an additional native edit. The preserved manifest has SHA-256
`90b4952ea89a54740cc8666f15673ef4ddc9947a307a9da746b50bd0e48473b4`.
The checker output records all eight input-file hashes, including both preserved
artifacts, its lexer and normalizer, the checker and current manifest, and the
two native source files. It also records the canonical manifest value's hash.
The pilot's complete source fingerprint independently includes these files and
this review; the source-bound component generators still import only the
unchanged pinned normalizer, not this historical baseline checker.

The missing/null transition and legacy-spoof checks remain enforced: the current
baseline requires `prior_prefix_capture_hybrid_v2`; claiming the older revision
accepts only the complete exact archived original manifest. The prior simple
slice candidate and its failed repeated-query performance control remain in
the separate first-candidate evidence. The selected hybrid requires fresh
source-bound measurements and proofs; this lexical transition is not evidence
that its performance controls pass.

This review does not modify old proof receipts or make them current. Fresh
source-bound component checks are required after the semantic edit. Default
proof scope remains `retry-diagnostics` disabled; feature-enabled observations,
universal ordered mapping, whole-engine serializability and worker-failure
availability are not proved by this baseline update.
