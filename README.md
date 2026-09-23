# Aerostore

<img width="600" height="600" alt="Aerostore Logo" src="https://github.com/user-attachments/assets/7d64557f-9733-40b7-8f40-d251a48a5205" />

A Rust database engine for high-ingest, frequently updated data shared between processes on a single host. Aerostore combines shared-memory storage, indexed queries, transactions, and write-ahead logging. It includes a Tcl extension with a flight-tracking example for batch ingestion and search.

**Status:** Experimental and under active development. APIs and storage formats can change. The project targets Linux, including WSL2, and is intended for development and workload evaluation.

## Features

- **Shared-memory storage:** multiple processes can access mapped rows and indexes through relative pointers.
- **Transactions:** optimistic concurrency control, versioned rows, savepoints, and coordinated indexed updates.
- **Indexes and queries:** skiplist secondary indexes, bounded range scans, and a rule-based query planner.
- **Durability and restart:** synchronous or asynchronous WAL commits, delta-encoded updates, checkpoints, replay, and warm attachment to compatible shared mappings.
- **Memory reuse:** background vacuum and index garbage collection reclaim storage during sustained updates.
- **Tcl integration:** batch TSV ingestion and field-based search through the included `FlightState` bridge.

Aerostore is a Rust library and Tcl extension. SQL compatibility, distributed replication, and production authentication are outside the current scope.

## Getting started

### Prerequisites

- Linux or WSL2. The implementation and process tests use Unix facilities such as `fork`, shared mappings, and signals.
- Rust and Cargo. The current validation used Rust **1.93.1**; a minimum supported Rust version has not been declared.
- Tcl and development tools to build the Tcl extension.
- Python 3 for the sustained benchmark script, and Docker for the PostgreSQL comparison. Aerostore-only runs do not require Docker.

On Debian or Ubuntu, install the native build dependencies:

```bash
sudo apt-get update
sudo apt-get install -y build-essential pkg-config tcl tcl-dev clang libclang-dev python3
```

From a checkout of this repository:

```bash
cargo build --release --workspace
```

To build only the Rust engine:

```bash
cargo build --release -p aerostore_core
```

### Try the Tcl bridge

Run this from the repository root after building the workspace. It creates a fresh temporary data directory and a dedicated shared mapping:

```bash
export AEROSTORE_DEMO_DIR="$(mktemp -d)"
export AEROSTORE_SHM_PATH="$AEROSTORE_DEMO_DIR/shared.mmap"

tclsh <<'TCL'
load ./target/release/libaerostore_tcl.so Aerostore
package require aerostore

aerostore::init $env(AEROSTORE_DEMO_DIR)
aerostore::set_config aerostore.synchronous_commit on

FlightState ingest_tsv "UAL123\t37.618805\t-122.375416\t35000\t451\t1709000000" 1
puts [FlightState search -compare {{= flight_id UAL123}} -limit 10]
TCL
```

Expected output: `1`. `FlightState search` returns the number of matching rows. The six TSV columns are flight ID, latitude, longitude, altitude, ground speed, and update timestamp.

The Tcl bridge currently uses a fixed flight schema, a 32,768-row capacity, and one database instance per process. Keep a dedicated mapping path for each database; `AEROSTORE_SHM_PATH` is separate from the data-directory argument. The default mapping path is `/dev/shm/aerostore.mmap`.

See [the Tcl example](aerostore_tcl/test.tcl) for more ingestion and query examples. For Rust integration, start with the [public API](aerostore_core/src/lib.rs) and [indexed transaction examples](aerostore_core/tests/occ_index_ordering.rs).

## Tests and verification

Run the release workspace suite with serial test scheduling for the process-heavy tests:

```bash
cargo test --workspace --release -- --test-threads=1
```

Run the focused Loom concurrency models in a separate build directory:

```bash
RUSTFLAGS='--cfg aerostore_loom' \
CARGO_TARGET_DIR=/tmp/aerostore-loom-target \
cargo test -p aerostore_core --test shm_mutation_model --release
```

The release workspace suite and all five bounded models passed in the September 2026 validation. The models cover specific concurrency invariants; their bounds and the implementation's remaining limits are documented in the [correctness and verification report](docs/sustained_churn_correctness.md).

## The Crucible benchmark

Crucible exercises 50,000 rows with 16 workers: 80% keyed upserts and 20% indexed range scans, with 5% of upserts targeting hot keys. It checks exact table/index agreement, allocation ownership, reclamation, operation failures, memory growth, and sustained throughput.

Run a 30-second Aerostore-only check with a 128 MiB arena:

```bash
AEROSTORE_CRUCIBLE_AEROSTORE_ONLY=1 \
AEROSTORE_CRUCIBLE_SHM_MIB=128 \
AEROSTORE_CRUCIBLE_DURATION_SECS=30 \
cargo bench -p aerostore_core --bench hyperfeed_crucible -- --noplot
```

Run the 120- and 240-second 2 GiB comparison against PostgreSQL, with Docker running:

```bash
./scripts/check_crucible_2g_120_vs_240.sh
```

### Validated results

On 2026-09-22, the 2 GiB comparison produced these results on an Intel Core Ultra 9 285K host running WSL2, with PostgreSQL 16 and asynchronous commit in both engines:

| Duration | Aerostore ops/s | PostgreSQL ops/s | Throughput ratio |
| --- | ---: | ---: | ---: |
| 120 seconds | 358,954 | 51,290 | 7.00× |
| 240 seconds | 376,727 | 49,161 | 7.66× |

The 128 MiB Aerostore-only runs also passed at both durations. Across all four runs, **267 million operations** completed with correct final indexes, no unaccounted structural allocations, and arena high-water usage below **21 MiB**. These results are specific to this workload and host. Aerostore's direct shared-memory access and PostgreSQL's client/server path have different overheads.

The latest sustained validation covers the 2 GiB and 128 MiB configurations. Other default arena profiles were not rerun in that validation. See the [raw results and reproduction commands](docs/bench_data/crucible_fixed_2026-09-22/README.md) and [performance runbook](docs/nightly_perf.md) for the full scope.

## Project layout

| Path | Contents |
| --- | --- |
| [`aerostore_core/`](aerostore_core/) | Storage, transactions, indexes, queries, WAL, recovery, tests, and benchmarks |
| [`aerostore_tcl/`](aerostore_tcl/) | Tcl extension and flight-data example |
| [`aerostore_macros/`](aerostore_macros/) | Procedural macros for row metadata |
| [`docs/`](docs/) | Verification reports, benchmark data, and runbooks |
| [`scripts/`](scripts/) | Sustained benchmark checks |

## Compatibility and recovery

The current shared-memory layout is **version 3**, with boot metadata **version 6**. Older mappings require a cold rebuild using the appropriate durable recovery inputs. Preserve WAL and checkpoint data when upgrading.

Applications using the Rust table and index APIs directly must follow the indexed-write coordination protocol. Process death while holding a shared lock and failures during postcommit maintenance across multiple indexes also require recovery handling. The [correctness report](docs/sustained_churn_correctness.md#current-protocol-and-invariants) describes those contracts and limits.

## Contributing

Bug reports, reproducible workloads, and focused pull requests are welcome. For correctness or performance changes, include the failing case, the commands used to verify the change, and any relevant workload or host details. Keep the correctness and allocation checks enabled when comparing performance.

The [performance runbook](docs/nightly_perf.md) lists additional focused and stress suites. Earlier architecture notes and benchmark tables are preserved in the [archived README](docs/archive/README-2026-09-22.md); use the current reports when evaluating the current implementation.

## License

[MIT](LICENSE).
