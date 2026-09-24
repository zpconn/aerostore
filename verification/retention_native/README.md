# Native publication, retention and reuse schedules

These deterministic tests execute AeroStore's actual publication, snapshot
selection, vacuum and allocation paths. They are regression evidence, not a
formal refinement or Rust memory-safety proof. This audit found a public API
correctness bug: a caller-supplied vacuum horizon above the retained horizon
could erase a reader's first visible row and allow that missing-row read to
commit. The public API now clamps its argument to the same arena's retained
horizon. The normal collector calls the unchanged internal kernel after its
existing horizon calculation, so this fix adds no extra scan to that path.

Two new unit tests use narrowly scoped `cfg(test)` callbacks:

* `retention_native_partial_publication_retains_snapshot_until_reuse` runs both
  ordinary and prepared commits. A reader begins after the first row's base is
  marked deleted, or after its replacement head is published, while the second
  row remains unpublished. Its snapshot includes the older active writer and
  returns both old values. After the writer finishes, vacuum must retain both
  bases until the reader ends; subsequent writes reuse the original addresses.
* `retention_native_loaded_cursor_survives_pruned_tail_reuse` pauses the actual
  native chain walk after loading its next pointer from an invisible version.
  Vacuum prunes an older tail below the reader's retained visible version, and a
  real commit reuses that address. The already loaded cursor still reaches the
  historical value. The test checks unlinking before reuse and cleared deletion
  metadata before continuing. After the reader ends, the formerly traversed
  versions become reclaimable. The schedule covers both an older creator active
  in the snapshot and a creator that begins after that snapshot.

A public-API regression starts a reader, commits a replacement, then requests
vacuum with `u64::MAX`. The reader must still obtain its historical value and
its commit must reject the later writer. A conservative request of zero must
delay reclamation; releasing the reader must permit eventual reclamation.
An isolated negative control removes exactly the public clamp and must fail
before any reclaimed address is reused.

The campaign also runs the existing native regressions for a row-lock guard
outliving its transaction and a newer overlapping reader not inheriting an
obsolete retention horizon. These avoid duplicating existing tests while
covering the guard pin and eventual release obligations of the same protocol.

`ROW_PUBLICATION_STEP_HOOK` runs after the base-xmax operation (`false`) and
after a successful head CAS (`true`) in each publication path.
`ROW_TRAVERSAL_STEP_HOOK` runs immediately after the native `row.next` load and
receives the current and loaded-next offsets. All declarations and calls are
test-only. The lookup adapter strips only that exact traversal block at its
reviewed position. Publication adapters similarly review their precise cuts.
Callbacks establish interleavings directly; sleeps and probabilistic races do
not determine results.

```sh
python3 -m unittest discover -s verification/retention_native -p 'test_*.py'
python3 verification/retention_native/run.py --output target/verification/retention-native
```

Choose a fresh output directory. The runner archives the committed crate tree,
overlays current native sources and tests, and builds each mutation in its own
workspace and Cargo target directory with the pinned compiler. The collector
source is overlaid and fingerprinted as well as the row and lifecycle sources.
Eight negative
controls ignore the horizon, substitute registration IDs for retained horizons,
recycle without unlinking, preserve stale deletion metadata, reclaim a locked
version, inherit an obsolete horizon, or ignore an active creator during partial
publication; the eighth omits the public horizon clamp. Each must fail its named assertion. Compiler failures, signals,
timeouts and zero-test runs do not count. The destructive retention variants
assert failure before reinitializing prematurely reclaimed storage.

The proof obligation exposed by this audit is stronger than preservation of a
fresh traversal's visible result: **a cursor already loaded by a live reader
must not be recycled before that reader stops using it**. Coherent publication
history must establish that the traversed prefix is retained and a visible
anchor precedes every reclaimable tail. The new test exercises that boundary;
it does not establish the unbounded concurrent or weak-memory theorem. Native
allocator ownership, physical pointer mapping and reclamation remain explicit
obligations of the formal campaigns.

The [runtime-delta audit](check_runtime_delta.py) permits exactly the reviewed
public wrapper, internal kernel rename and collector call rename relative to
`f3d8ec49`. It also requires the exact public regression and the previously
approved test-only hooks/modules. All other Rust/Cargo tokens must match. Its
receipt explicitly records `production_equivalent = false`: this is a real
correctness fix, and lexical delta detection is not a semantic proof. The
ordinary collector still performs one horizon calculation and the kernel body
is token-identical. No new throughput or latency result is claimed.

```sh
python3 verification/retention_native/check_runtime_delta.py --output target/verification-storage/runtime-delta.json
```
