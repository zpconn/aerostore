# Temporary signature-affinity checkpoint — 2026-09-25

The calibrated Crucible now supports the temporary `(callsign, registration)` affinity policy confirmed for single-machine HyperFeed and MMHF. Alias changes and expiry can send one flight to different workers. Permanent identity routing remains a control using identical input messages. The [dispatcher guide](../../hyperfeed_affinity.md) defines the experimental TTL, refresh rule and synthetic alias pattern.

This is workload and validation progress toward the ≥10× useful-throughput goal. It changes the benchmark and qualification tools, not the production engine. No capacity, speed ratio, production failure availability or physical MMHF result is qualified.

## Functional results

The [independently reviewed campaign](campaign-review.json.gz) passed **36/36 trials**: direct AeroStore, the Unix-socket service and PostgreSQL; both routing policies; three seeds; full-history runs and exact metrics companions. Each trial admitted 320 foreground messages plus six maintenance jobs and drained all 326. All 18 full histories have valid serial witnesses. The 18 metrics histories remain unverified even though their complete-history companions match.

The inputs were identical across routing policies and engines within each seed. Routing statistics and actual assignments matched an independent Python reconstruction. Affinity exercised hits, expiry and worker changes; the low-rate runs did not exhibit same-flight overlap or stale updates. A separate local TCP full-history run completed 196 messages with a valid witness. Loopback does not establish two-host MMHF behavior.

The [real-process regression](affinity-tests-review.json.gz) deliberately paused one idle worker before admission. A later message for the same flight completed on another worker before the paused worker resumed. All 18 messages eventually committed with a valid serial history, but six fork updates were stale. Every message still had some positive effect; the gate nevertheless rejected equivalent useful-work and performance claims. This checks reordering and honest accounting, not death while holding engine guards or crash recovery.

## Higher-load sensitivity

A separate [failure-aware review](sensitivity-review.json.gz) retains all twelve full-history trials. These used one seed, four foreground workers, 16 families, five seconds of admission, mixed identifiers, a 600 ms experimental affinity TTL, and accelerated 1/2-second maintenance. The backlog bound was 1,000 per worker and each transaction allowed up to 128 retries.

| Routing | Offered foreground messages/sec | Direct AeroStore | Unix service | PostgreSQL |
| --- | ---: | --- | --- | --- |
| Identity | 512 | Valid | Valid | Valid |
| Identity | 2,048 | Projection retry limit | Projection retry limit | Backlog limit |
| Signature affinity | 512 | Valid | Housekeeping retry limit | Valid |
| Signature affinity | 2,048 | Valid | Projection retry limit | Backlog limit |

“Valid” means the offered corpus drained and its recorded history passed the oracle. Failed cells retain their partial histories, progress and primary errors; an interrupted history is not a completed correctness check. No invalid serial witness was demonstrated by these progress failures. The successful affinity runs again showed no measured same-flight overlap or stale effects. The aborted PostgreSQL and service affinity traces did contain overlap, completion inversions and respectively 3,341 and 2,726 stale fork updates. These are lower-bound observations on incomplete received subsets, with no serial-history verdict. The deliberate pause provides a complete, valid history exhibiting reordering and stale updates.

The runs shared a host with verification work and used full-history instrumentation. This single-seed sensitivity is neither a controlled performance ranking nor a PostgreSQL saturation bracket. The native affinity success at 2,048/sec is not a sustainable capacity claim. The failures identify concrete maintenance-progress and queue-pressure cases to investigate with complete jobs and more precise retry attribution.

## Checks and preservation

The final source passed 39 model tests, seven measurement tests, 44 qualification tests, ten remote-helper tests and two real affinity tests. Thirteen existing real-process tests also passed against the identical benchmark executable before the test-only compatibility correction; they were not rerun afterward. The [implementation review](implementation-review.json) records the exact evidence reuse and found no blocking issue.

The [component guardrail archive](guardrails/README.md) records the completed pilot and its precise proof scope. P0 contract coverage, component proofs and finite models do not establish full P1 or whole-engine correctness. The [preservation check](preservation-review.json) confirms all 39 tracked production core and original Extended Crucible implementation files are unchanged from checkpoint `c0d6556`.

The [build receipt](build-provenance.json.gz) binds the executable SHA-256 `f01f19d86061fb4fe6f87cf384fc60eb6b4f3f44126b2ecd94d388808004f48d` to the frozen 482-file source snapshot `fef4c866ad708e8d49335f6432a0ce0e24d288b4232e87efece565d22ad3c32e`. The [source bundle](source.tar.gz), [artifact manifest](artifact-manifest.json) and root checksums retain original and archived hashes. Source and binary bindings stayed unchanged through the final runs. Executables, live mappings, WAL, private worker configurations and database files are omitted.

Earlier development evidence is retained separately: the alias test initially raised a `KeyError` because a legacy identity report omits an optional field. The one-line assertion correction and its boundary fingerprint were the only changes between the two source bundles; the executable bytes were identical. The paused-worker test had already passed. An intentionally interrupted seven-check proof run remains incomplete and contributes nothing to the final pilot result. Redundant pre-correction campaigns are omitted in favor of the final bound rerun.

The [cleanup receipt](postgres-cleanup.json) confirms the exact owned PostgreSQL process had no other clients and was stopped, retaining its data directory.

## Next work

Complete scheduled maintenance jobs and distinguish admitted jobs from their committed batches, while preserving the current batch-only control. Confirm maintenance atomic boundaries before calling a proposed multi-transaction sweep representative. The [next-milestone review](next-milestone-review.md) specifies completion witnesses, concurrency and failure tests, accounting changes, and resource prerequisites for 100/200/300-worker experiments. Population turnover, fork distributions, sustained retention, worker-death availability and matched durability remain open; current gates continue to reject the 10× replacement claim.
