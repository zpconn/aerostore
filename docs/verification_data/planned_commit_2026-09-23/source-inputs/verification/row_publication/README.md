# Native prepared row publication

This campaign translates the actual `OccTable::publish_prepared_write_set`
loop, retaining every fallible resolution, deletion CAS, link store, head CAS,
and early return. It specializes the record to **one prepared write**. The
whole loop remains source-bound; this is not a replacement implementation.

The proof embeds the exact generated `verification/lookup` module and uses its
`Image`, `Row`, `Snapshot`, visibility predicate, and recursive `first_visible`.
`generate.render_module()` allows the storage composition to share one
`crate::lookup` type identity with the retention campaign.

The individual primitive contracts describe native slot resolution, pointer
resolution, `xmax.compare_exchange(0, writer, AcqRel, Acquire)`,
`next.store(base, Release)`, and head CAS. From those individual operations,
the actual loop establishes these exact intermediate projections:

1. The initial prepared image.
2. The predecessor's deletion transaction has been recorded.
3. The new version links to that predecessor.
4. The row head points to the new version.

Successful publication equals the final projection. With the validated base
and exclusive guard preconditions, the CAS operations cannot fail; fallible
slot/row resolution can leave only the actual initial or deletion-marked
prefix. If those resolutions are infallible, the method succeeds. The proof
does not silently treat a partially changed error result as rolled back.

All four projections remain valid retained images. For a writer invisible
to an older snapshot, every prefix preserves that snapshot's selected pointer
and stored values. Both a ghost witness and an executable implementation of all field contracts
exercise an already-linked prepared allocation, as native allocation does,
and a snapshot selecting the old row. The executable witness calls the actual
extracted publication loop and verifies its returned result and field values.
The actual prepared `next` value need not be zero.

The conditional boundary is precise: a row-partition guard excludes other
writers and vacuum from mutating this projected chain, the row and new allocation are named by the explicit `authorized(row_id, new_ptr)`
capability, the fresh prepared allocation has exclusive ownership and is not reachable from a published
head, offsets resolve to the represented fields, and the atomics have their
specified linearization/ordering behavior. The proof does not establish raw
mmap validity, allocation provenance, weak-memory compiler correctness, or
that every native caller establishes ownership and preparation preconditions.
It does not cover the nonprepared publication method, arbitrary multirow
commit histories, rollback after external publication failures, WAL, or the
entire storage history mapping. Those limits are machine-readable false
flags in the receipt; the composed concrete storage slice discharges its own
history mapping separately.

Run:

```sh
python3 verification/row_publication/generate.py --check
python3 -m unittest discover -s verification/row_publication -p 'test_*.py'
python3 verification/row_publication/run.py --output target/verification/row-publication
```

The strict runner checks pinned verifier artifacts, uses `--no-cheating`,
requires eight individually checked roots, and records eleven solver-rejected
semantic mutations. They omit or corrupt deletion, links, head publication,
CAS expectations, the publication loop, row/allocation authorization, and snapshot invisibility. Syntax
errors and timeouts do not count as successful negative controls. Exact
approved `cfg(test)` hooks are excluded from translation; altered hook bodies
are rejected. No production code or runtime proof bookkeeping is added.
