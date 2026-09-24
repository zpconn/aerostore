# Native acquisition and captured-row materialization regressions

These are deterministic tests of real AeroStore, not formal refinement claims.
No production algorithm changed. A narrow `cfg(test)` snapshot hook pauses
inside the API before its lifecycle acquisition; the existing candidate-capture
hook pauses after raw index IDs/dependencies have been captured and query guards
have been released.

The ProcArray test runs real deregistration, slot reuse, another horizon update
and a failed stale-owner deregistration while the snapshot caller waits. After
release/join, the returned snapshot must contain acquired metadata, while the
caller's owned slot and old horizon survived the wait. Assertions run after the
worker is released and joined. Channels establish ordering; no sleep or negative
timeout decides correctness.

The lookup test covers both an older writer active in the reader's snapshot and
a writer that starts after the reader. While a repeated lookup is between capture
and row materialization, writers remove the old posting, move its key, and restore
the same key/row pair with a different value. Repeated vacuum attempts cannot
reclaim the reader's historical version. Final pending writes remove one raw
candidate and add another, including multiple pending values for the same row.
The query returns the historical row plus the final own-write overlay; another
lookup and commit reject the changed dependency even though the posting was
restored. After the reader finishes, vacuum releases the old version and an
actual subsequent write reuses its original offset.

Run the isolated positive and negative campaign with an unused output directory:

```sh
python3 verification/lookup_native/run.py --output target/verification/lookup-native
```

The runner archives the parent crates, overlays current native files, and builds
each variant with its own Cargo target directory and the pinned native compiler.
Five negative controls use API-entry metadata instead of acquired metadata, omit
the retained snapshot horizon, ignore an older active creator, omit own-write
candidates, or hold query guards during materialization. Each must fail its
specified test assertion. Compile failures, timeouts, signals and zero-test
runs cannot pass. Source/compiler/log hashes and an explicit false formal-proof
flag accompany the receipt.

The separate production-change diagnostic compares all 118 current workspace
Rust/Cargo inputs with the milestone baseline:

```sh
python3 verification/lookup_native/check_production_equivalence.py --baseline 1e7311a
python3 -m unittest discover -s verification/lookup_native -p test_production_equivalence.py
```

It compares Rust tokens after excluding only ProcArray's `cfg(test) mod tests`,
OCC's `cfg(test) mod predicate_completion_tests`, and the exact test-only
`SNAPSHOT_ACQUIRING_HOOK` declaration/call at the reviewed pre-acquisition site.
The retention milestone also permits exactly the row publication hooks after
base marking and successful head CAS in both publication paths, plus the
traversal hook immediately after loading `row.next`. Declarations must remain
inside the existing test-only thread-local block; all five calls must have
their reviewed bodies and native cut positions. Missing test attributes,
modified arguments, wrong publication phases and moved calls fail closed.
Other test modules remain in the comparison. Comments/whitespace are discarded;
literal spelling and joint operators remain. Cargo/config inputs are compared
byte for byte. The receipt records the resolved baseline commit, each source
and production-token hash, exclusions, and source stability. Inventory changes,
unknown syntax, unreviewed hook placement, and production-token differences fail.
This is reproducible lexical change detection, not compiler-level semantic
equivalence or a proof that the excluded tests are correct.
