# Ordered due-range dependency experiment

The contention Crucible supports an optional ordered publication policy for its
`due` index. It tests whether unrelated future scheduled updates cause avoidable
projection retries. The default remains `--due-index hashed`; PostgreSQL's SQL,
prepared statements and indexes are unchanged by this selector.

```text
--due-index ordered
--due-index-origin 1700000000000000000
--due-index-width 1000000000
```

These defaults use the calibrated workload's nanosecond epoch and one-second
intervals. They are raw indexed-value units, not wall-clock settings. The older
legacy/fleet workloads use integer seconds and need appropriate explicit values.
The qualification and remote helpers forward all three options. Reports record
requested and effective settings, and exact correctness companions must match
them. The fixture's immutable identity and the index's shared header prevent
attached processes from silently using different policies.

The policy changes publication dependencies, not query results. A signed key
below the origin uses bucket 0. The next 4093 intervals use buckets 1–4093;
larger signed keys share bucket 4094. Other key types share bucket 4095. A signed
`<` or `<=` query protects the prefix through its cutoff bucket; `>` or `>=`
protects the suffix, including other value types. Strict comparisons retain the
whole cutoff bucket. Equality and `In` use the same mapping as writers. Ranges
bounded by other key types conservatively protect all buckets.

Writers still lock and stamp both their old and new key buckets. This retains
dependencies on removed keys and empty ranges. Historical indexed queries keep
their complete-or-retry contract: this experiment does not add historical
postings or allow incomplete snapshot results. OCC validation, publication order,
retry limits and WAL acknowledgement are unchanged.

The window is fixed. It never wraps or automatically rebases while transactions
are active. At the default width, its precise intervals span 4093 seconds;
conflict selectivity can degrade as timestamps enter the overflow tail. A wider
interval extends the window but increases conservative conflicts within each
interval. Queries far into the window also acquire longer bucket prefixes.
These are explicit limits of this prototype, not a sustained-performance claim.

The shared index header is version 3, including for the hashed control. Older
version 1/2 mappings are rejected before layout-dependent fields are interpreted.
Existing mappings require an exclusive rebuild; no online conversion or durable
format migration is supplied. Measurements use disposable fresh mappings and
the same binary/header layout for both policies.

The [verification boundary](../verification/ordered_range/README.md) explains
what existing proofs cover. Native coverage checks and transaction schedules
exercise the new mapping, but a universal arithmetic/refinement proof remains
open. The candidate is an investigation tool for the 10× goal; it is not promoted
to the default or qualified as a PostgreSQL replacement.
