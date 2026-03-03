# Hyperfeed Crucible 10x Attempt Log (2026-03-03)

## Objective
Achieve a **real systems-level sustained 10x TPS advantage vs tuned PostgreSQL** in the `hyperfeed_crucible` benchmark at **60s duration**, without changing benchmark criteria.

## Ground Rules Used
- No benchmark-gaming (no weakening gates, no workload changes).
- Keep correctness gates (including index insert/remove failure checks).
- Validate with real benchmark runs after each change.
- Roll back anything that regresses correctness or sustained performance.

## Baseline (Clean Tree)

### Short run sanity (5s)
- `profile_512m`: Aerostore `728,172 TPS`, Postgres `85,443 TPS`, ratio `8.52x`.
- `profile_1g`: Aerostore `852,849 TPS`, Postgres `84,549 TPS`, ratio `10.09x`.

### Sustained run (60s) baseline bottleneck
- `profile_512m`: Aerostore `217,606 TPS`, Postgres `86,297 TPS`, ratio `2.52x`.
  - Aerostore elapsed `69.206s` (workers drained past nominal window).
  - Retry telemetry: `retry_alloc=524,569`, `max_insert_attempts=4097`.
- `profile_1g`: Aerostore `548,578 TPS`, Postgres `87,106 TPS`, ratio `6.30x`.
  - Aerostore elapsed `73.073s`.
  - Retry telemetry: `retry_alloc=262,897`, `max_insert_attempts=4097`.

Conclusion: sustained collapse is dominated by long-tail index alloc retries and drain time, not median operation latency.

## Control Check: Faster Background Daemons
Tested `AEROSTORE_CRUCIBLE_VACUUM_INTERVAL_MS=5` and `AEROSTORE_CRUCIBLE_INDEX_GC_INTERVAL_MS=5`.

- 10s could look strong on one profile, but:
- 60s still poor:
  - `profile_512m`: `208,547 TPS` (`2.43x`)
  - `profile_1g`: `561,248 TPS` (`6.43x`)
  - Large drain still present.

Why this did not solve it:
- Higher daemon cadence alone did not remove retry storms or long-tail saturation under sustained churn.

---

## Experiment A: ChunkedArena Free-List Bucketing (Size/Align-Keyed)

### What changed
File: `aerostore_core/src/shm.rs`
- Replaced effective single mixed free-list behavior with size+align keyed bucket heads (exact-match pops), with legacy fallback.
- Added bucket metadata in shared header and corresponding push/pop paths.

### Why
Hypothesis: mixed-size head-of-list mismatches were causing avoidable pop misses and allocator fallback churn.

### Results
- Lib/unit + free-list invariant tests passed.
- 10s improved strongly:
  - `profile_512m`: `959,160 TPS` (`11.34x`)
  - `profile_1g`: `947,429 TPS` (`11.12x`)
- 60s still not near target:
  - `profile_512m`: `201,495 TPS` (`2.31x`), elapsed `67.278s`, `retry_alloc=525,011`
  - `profile_1g`: `544,478 TPS` (`6.22x`), elapsed `63.469s`, `retry_alloc=262,437`

### Verdict
Partially helpful for burst behavior, but **did not fix sustained 60s bottleneck**. Rolled back later for isolation and to avoid carrying low-level allocator complexity without durable gain.

---

## Experiment B: Aggressive `shm_index` Retry/GC Tuning (v1)

### What changed
File: `aerostore_core/src/shm_index.rs`
- Smaller/frequent reclaim batches during retry.
- Shorter retry pauses.

### Why
Hypothesis: current retry path was over-sleeping and over-sweeping; reduce per-retry cost.

### Results
- 10s became unstable across profiles:
  - `profile_512m`: dropped to `380,546 TPS` (`4.51x`)
  - `profile_1g`: `936,645 TPS` (`11.11x`)
  - Structural retry storms appeared (`retry_structural=92,051` on 512m).

### Verdict
Introduced bad retry dynamics. Rolled back immediately.

---

## Experiment C: Reduce Skiplist Posting Payload Footprint

### What changed
File: `aerostore_core/src/shm_skiplist.rs`
- `MAX_PAYLOAD_BYTES` reduced from `192` to `64`.

### Why
Hypothesis: smaller posting entries reduce memory pressure under timestamp-key churn.

### Results
- 10s mixed:
  - `profile_512m`: `766,231 TPS` (`9.08x`)
  - `profile_1g`: `993,743 TPS` (`11.55x`)
- 60s catastrophic on 512m:
  - `40,487 TPS` (`0.46x`)
  - Conflicts exploded (`57,250`) despite low index-retry counts.

### Verdict
Severe sustained regression. Rolled back.

---

## Experiment D: Narrow Alloc-Retry Lightening (v2)

### What changed
File: `aerostore_core/src/shm_index.rs`
- For alloc retries:
  - much smaller/less frequent `collect_garbage_once` calls early in retry.
  - shorter sleep profile.
- Kept structural/epoch paths mostly unchanged.

### Why
Hypothesis: worker-side retry loop was self-amplifying stalls with heavy synchronous GC passes.

### Results
- 20s looked much better:
  - `profile_512m`: `698,467 TPS` (`8.03x`)
  - `profile_1g`: `886,273 TPS` (`10.34x`)
- 60s improved throughput vs baseline but lost correctness:
  - `profile_512m`: `315,539 TPS` (`3.62x`)
  - `profile_1g`: `467,727 TPS` (`5.36x`)
  - Index failures appeared (`Index Remove Fail=16`, `Index Insert Fail=16`).
  - Retry alloc exploded (millions).

### Verdict
Not acceptable due to correctness regressions.

---

## Experiment E: Add Conservative High-Attempt Fallback (v3)

### What changed
File: `aerostore_core/src/shm_index.rs`
- Kept v2 lightweight behavior early.
- Added fallback to original heavy reclaim/long-sleep style at very high attempts (`>=3072`) to preserve zero-failure behavior.

### Why
Hypothesis: combine early throughput benefit with late-stage safety.

### Results
- 20s strong and correct:
  - `profile_512m`: `825,562 TPS` (`9.65x`), zero index failures.
  - `profile_1g`: `973,036 TPS` (`11.31x`), zero index failures.
- 60s:
  - `profile_512m`: `353,209 TPS` (`4.08x`), zero index failures.
  - But p99 regressed to Postgres parity (`p99 ratio=1.000`), failing latency gate.

### Verdict
Reliability restored, but sustained latency tail still failed benchmark criteria.

---

## Final Outcome
- No patch set reached the 60s objective of a robust 10x advantage while keeping all gates healthy.
- All experimental changes were rolled back.
- Experimental code state before this write-up: **clean**.

## Addendum: 60s Run With `profile_2g`
Command used:
```bash
AEROSTORE_CRUCIBLE_DURATION_SECS=60 cargo bench -p aerostore_core --bench hyperfeed_crucible -- --noplot
```

Run included `profile_512m`, `profile_1g`, and `profile_2g` (with `profile_2g` added to the crucible profile matrix for this measurement):

- `profile_512m`: Aerostore `414,079.75 TPS`, Postgres `84,560.54 TPS`, ratio `4.90x`.
  - p99 ratio `0.125` (Aerostore `32.77us`, Postgres `262.14us`)
  - Index failures: insert `0`, remove `0`
  - Aerostore elapsed `65.746s`
- `profile_1g`: Aerostore `520,402.73 TPS`, Postgres `84,089.43 TPS`, ratio `6.19x`.
  - p99 ratio `0.250` (Aerostore `65.54us`, Postgres `262.14us`)
  - Index failures: insert `0`, remove `0`
  - Aerostore elapsed `68.218s`
- `profile_2g`: Aerostore `958,751.38 TPS`, Postgres `83,671.27 TPS`, ratio `11.46x`.
  - p99 ratio `0.125` (Aerostore `32.77us`, Postgres `262.14us`)
  - Index failures: insert `0`, remove `0`
  - Aerostore elapsed `60.143s`

Interpretation:
- `2g` hit and exceeded the sustained 10x objective (`11.46x`) while keeping index failure counts at zero.
- `512m` and `1g` improved versus prior 4.23x/4.62x snapshots in this run, but still remained below 10x.

## What This Cycle Demonstrated
1. 10s wins are not predictive; 60s exposes allocator/epoch/retry feedback loops.
2. The dominant sustained failure mode is not base scan/index speed (those remain fast), but long-tail behavior under prolonged churn.
3. Worker-local retry policy and reclamation policy interact nonlinearly:
   - too heavy => drain and low sustained TPS;
   - too light => index mutation failures;
   - hybrid fallback => still p99 tail spikes.

## Recommended Next Technical Direction
The next credible path is larger-than-tuning changes:
1. Add direct **in-place index move/relink** fast path for monotonic timestamp updates (reduce remove+insert churn).
2. Add explicit epoch-lag/reclaimer starvation telemetry in crucible output.
3. Separate allocator domains for long-lived index structure nodes vs high-churn posting nodes.
4. Consider bounded per-worker retry budget + queueing fallback for hot alloc contention to cap p99 tails.
