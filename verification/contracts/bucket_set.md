# Predicate bucket contract, version 1

This is the fixed semantic contract for the first implementation experiment.
It specifies the production functions in `aerostore_verified`; the transaction
protocol and platform assumptions remain separate obligations.

Given a finite sequence `input` of machine-sized unsigned bucket identifiers and
a machine-sized `bucket_count`, canonicalization returns:

- `Err(id)` if any input identifier is at least `bucket_count`. The error is the
  first invalid identifier in input order. No invalid identifier is ignored.
- Otherwise `Ok(output)`, where `output` is strictly increasing and an identifier
  occurs in `output` if and only if it occurs in `input`.

Consequently there are no duplicates, every output identifier is in range, and
output length is at most both input length and bucket count. Empty input returns
an empty output, including when bucket count is zero. The two candidate
algorithms must agree on all successful outputs and error results.

The enclosing index code remains responsible for canonical key validation,
hashing, and selecting the input identifiers: equality selects its key's bucket,
`In` selects all its keys' buckets, and inequalities select all buckets. Invalid
keys must still fail even when earlier keys already selected all buckets. The
current production bucket count remains 4,096.

Strictly increasing output is part of the contract because callers use this
order to acquire locks. Set membership alone is insufficient.

The stamp decision function returns true exactly when `stamp < transaction_id`,
including machine-integer boundary values. This proves a scalar decision; it
does not prove registration, stamp allocation, load/store ordering, publication,
or the absence of counter wraparound in the caller.

Rust allocation failure/abort and the standard collection implementations are
part of the tool/platform boundary. Proofs must not claim a hard real-time
bound or successful allocation for an arbitrarily large `bucket_count`.
Performance experiments use the existing production limit and record temporary
memory use as well as elapsed time.

Accepted optimizations may change implementation code, internal invariants, and
proof scripts. They may not weaken this contract, strengthen its valid-input
requirements, add assumptions about the target algorithm, or remove required
proof roots. Such changes establish a different experiment baseline.
