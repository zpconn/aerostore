# Crucible Allocator Telemetry Report (2026-03-03)

## Objective
Capture time-series allocator/reclaimer signals during a long `hyperfeed_crucible` run to test whether observed long-duration performance decay looks like:
- a true memory leak, or
- allocator/reclaimer saturation and reuse failure (thrash).

## Instrumentation Added
Code changes used for this capture:
- [aerostore_core/src/shm.rs](/home/zpconn/code/aerostore/aerostore_core/src/shm.rs)
  - Added `ShmArena::free_list_depth_estimate(max_nodes) -> (depth, truncated)`.
- [aerostore_core/benches/hyperfeed_crucible.rs](/home/zpconn/code/aerostore/aerostore_core/benches/hyperfeed_crucible.rs)
  - Added optional allocator telemetry recorder (disabled by default).
  - Recorder writes low-frequency CSV samples using existing atomic counters.

Telemetry is opt-in only. Normal benchmark runs are unchanged unless telemetry env vars are set.

## Capture Run
Command:
```bash
AEROSTORE_CRUCIBLE_DURATION_SECS=120 \
AEROSTORE_CRUCIBLE_PROFILE_FILTER=profile_2g \
AEROSTORE_CRUCIBLE_ALLOC_TELEMETRY_PATH=/tmp/aerostore_alloc_{profile}_120s.csv \
AEROSTORE_CRUCIBLE_ALLOC_TELEMETRY_INTERVAL_MS=5000 \
AEROSTORE_CRUCIBLE_ALLOC_TELEMETRY_DEPTH_SCAN_LIMIT=20000 \
cargo bench -p aerostore_core --bench hyperfeed_crucible -- --noplot
```

Output file:
- `/tmp/aerostore_alloc_profile_2g_120s.csv`
- Committed snapshot: [docs/bench_data/aerostore_alloc_profile_2g_120s_2026-03-03.csv](/home/zpconn/code/aerostore/docs/bench_data/aerostore_alloc_profile_2g_120s_2026-03-03.csv)

## Crucible Result For This Capture
- Aerostore TPS: `538,236.15`
- Postgres TPS: `86,606.26`
- Ratio: `6.21x`
- Aerostore elapsed: `132.721s`
- Index failures: `0/0`

## Sample Schema
CSV columns:
- `elapsed_ms`
- `head_offset`
- `head_peak`
- `free_list_head_offset`
- `free_list_depth_est`
- `free_list_depth_truncated`
- `free_list_pushes`
- `free_list_pops`
- `free_list_net` (`pushes - pops`)
- `free_list_pop_misses`
- `retry_alloc`
- `retry_loops`
- `retry_structural`
- `max_insert_attempts`
- `max_remove_attempts`

## Time-Series Highlights
From the captured CSV:

- Samples: `25`
- Window: `0ms -> 132,771ms`
- `head_offset`:
  - first: `25,372,960`
  - last/max: `2,147,483,648` (arena cap)
  - first hit cap at: `65,189ms`
- `free_list_net` (`pushes - pops`):
  - first: `0`
  - last/max: `2,278,774`
- `free_list_depth_est`:
  - first: `0`
  - last: `20,000` (truncated)
  - remained truncated once saturated
- `retry_alloc`:
  - first: `0`
  - last/max: `656,160`
  - began rising after allocator reached cap
- `max_insert_attempts`:
  - reached `4097` at `75,210ms`
- `free_list_pop_misses`:
  - first: `250,010`
  - last: `17,604,299`

Monotonicity checks:
- `head_offset` drops: `0`
- `free_list_net` drops: `0`
- `free_list_depth_est` drops: `0`

## Key Milestones
At `65,189ms` (first moment `head_offset == 2,147,483,648`):
- `free_list_net = 2,278,774`
- `free_list_pop_misses = 16,950,686`
- `retry_alloc = 640`

At `75,210ms`:
- `max_insert_attempts = 4097`
- `retry_alloc = 66,192`

After `70,205ms`:
- `free_list_pushes` and `free_list_pops` stopped progressing in this run snapshot.
- allocator retries and pop misses continued increasing.

## Interpretation
This capture is more consistent with allocator/reuse saturation than a simple leak:

- The arena high-water mark hits cap around mid-run.
- A large reusable backlog exists (`free_list_net` in the millions), but allocation still fails fast enough to drive `retry_alloc` and `max_insert_attempts` to stress levels.
- `free_list_pop_misses` growth indicates failed reuse attempts despite non-empty free-list state.

That behavior suggests shape/fit reuse constraints and allocator pressure feedback loops (fragmentation-like thrash), rather than unbounded live-memory growth from never-reclaimed objects.

## Notes
- Free-list depth is a bounded estimate (`depth_scan_limit=20000`), so values at limit are lower bounds.
- This report captures one run and should be paired with repeated runs for variance characterization.
