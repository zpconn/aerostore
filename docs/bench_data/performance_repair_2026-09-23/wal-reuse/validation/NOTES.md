# Isolated WAL serializer reuse candidate

Parent: `9676fa9ff602565d5a628af41b6d40ba1576a8b9`. The main checkout is unchanged.
Only `wal_ring.rs`, `wal_writer.rs`, and the new `wal_serializer_allocations.rs` test differ from that parent. `candidate.json` records the exact patch and source hashes; `validation.json` records the full native source manifest, compiler and commands. No performance measurements or acceptance claim are included.

The committer owns a reusable aligned output buffer and an 8192-byte aligned scratch backing allocation. Each preparation clears output and constructs a fresh rkyv serializer, scratch cursor, fallback allocator, and shared-reference map. Errors and panics therefore cannot preserve a partially consumed scratch cursor. Fallback allocations still support large archives and are released when that attempt's serializer drops.

The existing unmodified `serialize_commit_record` is the byte oracle. Six serializer unit tests cover empty, repeated, varying-size and large records, scratch fallback, injected partial-archive error and unwind, retained-storage reuse, and oversized output. Three additional native owner tests cover oversized synchronous success, rejected oversized async enqueue followed by a full-baseline retry, and codec error/unwind followed by successful reuse. The 23 existing WAL protocol regressions and three poison regressions pass. One native allocation test passes. Total: 36 focused tests.

For 100 repeated one-write outer frames, deterministic thread-local allocator accounting observed:

| Serializer | Allocations | Reallocations | Frees | Requested bytes |
| --- | ---: | ---: | ---: | ---: |
| Original | 200 | 800 | 200 | 870300 |
| Warm reusable | 0 | 0 | 0 | 0 |

All compared frames were byte-for-byte equal. The original path allocated one 8192-byte scratch backing per frame. Separately, 100 warmed native one-row commits made 1400 allocations, no reallocations, and requested 51900 bytes; none were 8192-byte allocations. These are allocation measurements in the debug test binary, not latency or throughput measurements.

Between calls, the additional retained backing is at most **73728 bytes per committer**: 8192 scratch bytes plus at most 65536 bytes of output capacity. Bookkeeping and allocator metadata are excluded. Large synchronous frames remain supported; output capacity exceeding the cap is released after the guarded driver returns or unwinds, even after failed enqueue/serialization. During an attempt, memory may exceed that retention cap according to record size, as before. Existing row codec temporaries and the asynchronous baseline-row cache are unchanged.

An outer caller-owned lease keeps output destruction beyond the guarded callback, including error and unwind paths. The callback borrows the bytes for synchronous append or async ring copy. WAL wire bytes, stream binding, writer epoch recheck, poison checks, baseline-cache success semantics, durable-before-publication order, and the source-bound OCC driver are unchanged.

Validation command:

```sh
python3 target/performance-repair/wal-reuse/validate.py
```

The runner uses the isolated `target/performance-repair/wal-reuse/cargo-target`, `--offline --locked`, and strips instrumentation/build overrides. No timing jobs are active.
