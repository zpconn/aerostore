# Native indexed lookup and MVCC materialization

This campaign adapts the actual visibility test, version-chain walk, private-write
selection, row-lock check, read recording, row conflict validator, and indexed
lookup materialization tail from `aerostore_core/src/occ_partitioned.rs`.
The restricted adapter checks signatures, atomic orderings, chain bounds, and
the native acquire/capture/raw-candidate/release/materialize order. Unsupported
source changes fail extraction. The adapter is reviewed trusted code, not a
general Rust frontend.

The checked contracts establish:

- Exact snapshot visibility, including active creators/deleters and own writes.
- First-visible-version selection, or an explicit retry/error; exhausting the
  native chain limit cannot silently turn a present row into absence.
- Latest private-write precedence, key filtering, sorted unique row IDs, and
  complete results when candidate coverage holds.
- Exact read-record row ID, pointer and observed creator identity, pointer-based
  deduplication, preservation of existing records, and new-record provenance.
- Native validation rejects changed creator identity and relevant deletions.
- Appending an invisible version while retaining the old chain preserves the
  selected snapshot value under the stated image relation.

`history.rs` then **derives historical candidate coverage**. It proves exact
posting replay, shared-clock lifecycle chronology, and per-bucket stamp coverage
for arbitrary finite coherent event sequences. Stores on disjoint buckets may
reorder; the proof requires monotone labels only within each affected bucket.
Accepted query stamps imply that the relevant current postings contain the
snapshot matches. The final wrapper calls the native materialization tail and
retains its read-provenance guarantees. A concrete finite successful history
checks consistency of the abstract premises.

This history argument is independently proved in Verus. The corresponding
[Lean theorems](../lean/query_completeness.md) are not imported as axioms.
[The indexed slice](../indexed_slice/README.md) connects these contracts to
guard ownership, dependency capture and subsequent conflict validation.

## Explicit boundary

`Storage::image` is a retained version-chain projection for an operation; it is
not a claim that the physical heap stops changing. `history_maps_snapshot`
relates that image to the visible-event replay. Raw current-posting enumeration,
the image/history correspondence, valid offsets, retention and actual atomic
loads still need native refinement. Historical candidate completeness itself is
proved rather than assumed. The separate append lemma and deterministic native
tests support the retention boundary but do not establish arbitrary concurrent
raw-heap correspondence.

Rows model the extracted index-key projection of payloads, not every payload
byte. Key extraction, collection semantics, successful allocation, error
projection, and the finite no-wrap lifecycle policy remain explicit assumptions.
The query slice is not a full writer-commit, WAL, crash or serializability proof.
These proofs introduce no runtime locks or bookkeeping.

## Run

```sh
python3 verification/lookup/generate.py
python3 verification/lookup/test_generate.py
python3 verification/lookup/run.py
```

The receipt pins tool/source hashes and requires every named root and semantic
negative control. Mutants must fail an intended verification obligation;
parse errors and timeouts do not count. The composed pilot validates the receipt
against the frozen campaign manifest.
