# Native posting and row-publication data refinement

This campaign joins the existing native posting algorithms to row publication
for **one changed index key on one final write**. Row IDs, binding IDs, keys,
transaction IDs, version offsets and ordinary-write selection are symbolic.
Creation, key moves and key removal have inhabited examples. An update whose
index key is unchanged has no `IndexChange` in native code and is outside this
selected one-change slice.

`Storage<P, R>` contains disjoint posting and row projections. The embedded
posting module is the actual generated preparation, rollback and source-removal
code. The embedded prepared-publication module is unchanged. Both publishers
use the same `lookup::Image` and row fields; no copied visibility definition or
unchecked theorem import is introduced.

## Derived contracts

The incoming `planned` relation identifies the selected row, its current head
key, the prepared allocation's key and the actual change's before/after keys.
It requires the selected row's initial postings to match its current head and
the existing partition/allocation and posting-key authority. From these entry
conditions, the proof derives that destination insertion owns an absent posting;
absence is not supplied as an independent desired-outcome assumption.

The actual algorithms then establish:

- Preparation adds exactly the destination set and records exactly its successful
  additions. An error leaves data unchanged or poisons the index state.
- Rollback of those recorded additions restores the original posting set on
  success; a rollback failure poisons the state. This helper covers index
  rollback itself, not transaction abort or private-allocation recycling.
- Source removal deletes exactly the source set on success. Its failures retain
  the native partial-removal frame and poison the state.
- Row publication derives the exact deletion/link/head image from actual field
  operations. Successful publication makes the selected row's head key agree
  with its final posting set. Other rows and bindings' postings are unchanged.
- Successful posting helpers preserve the initial poison flag. This is a
  strengthening of their existing exported contracts, proved from unchanged
  primitive assumptions and the actual loops.

`publish_indexed_write` composes those steps for the prepared publisher.
`native_ordinary_data_segment` additionally derives its order from the actual
ordinary `commit_with_record_impl` branch. It checks the native ordinary wrapper
selects `WRITE_AHEAD=false`, checks the complete surrounding prefix and suffix,
and retains preparation, source removal, ordinary publication and failure
poisoning in source order. The unreachable prepared-callback branch is checked
before selection. Changing any omitted surrounding statement requires review.

The ordinary publisher is source-derived separately, including its selected
write iteration, immutable offset loads, deletion CAS, link store, head CAS and
returned write record. The result retains exact row ID, base/new offsets, both
projected key values and the dirty-column mask. Creation's `base_value` fallback
to the new value is also preserved. The composed result is either successful
row/posting agreement, unchanged data, or a poisoned partial state.

The constructors and `PostingMemory` demonstrate that the entry relation and
primitive contracts are inhabited. The latter uses explicit ghost posting
state and is a consistency model, not a native index implementation. Separate
create/move/delete witnesses inhabit the successful relation. These witnesses
do not assert that every fallible native invocation succeeds; no allocation or
resolution success assumption is added to the production theorem.

## Explicit boundary

This is conditional refinement of a selected publication interval, not a full
commit or concurrent-history proof. The actual prefix's validations and lock
acquisitions are checked as source context but are not proved here to establish
the incoming `planned` relation or native authority. The complete join from
private write history, validated snapshots and held physical guards remains a
caller obligation.

Borrow separation frames the two logical views in this proof. Instantiating
those views with one real arena requires the native posting primitives to
modify only the posting projection and row primitives to modify only the row
projection. Physical pointer validity, mmap/allocator ownership, immutable key
extraction, atomic observations and weak memory remain explicit assumptions.
The ordinary value-load primitive promises one exact field observation; it does
not assume final posting agreement or successful publication.

The segment omits only checked transaction read/write-set cleanup and lifecycle
finish calls from its error projection, and checks the remaining native suffix
as context. It therefore makes no statement about registration ownership,
deregistration, stamp publication, cleanup success, WAL or durability. Those
operations need separate composition. Publication errors can leave the base's
`xmax` set; they are not falsely characterized as rolled-back row state. The
native error branch poisons the table.

No production code, synchronization or bookkeeping changes in this campaign.
`full_commit_refinement_proved`, `whole_storage_history_refinement_proved`,
`transaction_history_refinement_proved`, allocator, multirow, WAL and weak-memory
refinement flags remain false.

## Reproduction and controls

```sh
python3 verification/commit_data/generate.py --check
python3 verification/commit_data/test_generate.py
python3 verification/commit_data/run.py --output target/verification/commit-data
```

The campaign checks the full artifact, 13 named roots and 17 semantic controls.
Controls remove key/row correspondence premises, skip actual data operations or
rollback, omit failure poisoning, reorder native source removal before
preparation, and corrupt ordinary publication records. Every negative must
reach a failed proof obligation; compiler errors and timeouts do not count.
Eleven adapter tests check source selection, exact omitted effects, ordinary policy,
field provenance and rejected unsupported changes. Receipts include exact tool,
source, generated mutant and raw-log fingerprints.
