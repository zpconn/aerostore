# HyperFeed: local validation and a future two-host run

The corrected public server/client path has already passed a [local TCP-loopback full-history check](bench_data/architecture_2026-09-25/continuous-functional/README.md): 200 fleet messages, a valid serial witness, successful final audits, and an independently checked continuous admission-to-drain clock. Reuse that evidence. No second host has been exercised.

The local PostgreSQL fixtures have since been [shut down](bench_data/fixture_cleanup_2026-09-25.json): the native data directory was retained and the owned Docker container was removed. Future PostgreSQL commands require a prepared, running disposable database.

These commands prepare a transport and stress investigation, not a complete HyperFeed comparison. They retain `--workload fleet` to reproduce the existing stress fixture. The separate [cadence and ordering profile](hyperfeed_calibrated.md) supports `--workload calibrated`, `--projection-interval-seconds` and `--housekeeping-interval-seconds` on both the remote helper and PostgreSQL driver. For that profile, the rate counts only foreground messages and two maintenance workers are additional to `--workers`. Use identical settings on both paths; a 30-second default-cadence run exercises no timer jobs. Both helpers also accept `--dispatch signature-affinity --affinity-ttl-ms N --signature-pattern mixed` for the [temporary signature dispatcher](hyperfeed_affinity.md). Supply the same explicit experimental TTL and pattern on both paths; retain `--dispatch identity` with zero TTL as the corpus-matched control. Both paths also accept `--maintenance-mode sweep --projection-batch-size 4 --housekeeping-batch-size 32 --max-maintenance-batches 4096` for [complete maintenance jobs](hyperfeed_maintenance.md). Keep all four settings identical on the owner/client and PostgreSQL paths; these batch sizes are experimental. Job completion requires a committed terminal empty query, and the cap includes that transaction. Fixed population and finite seeded expiry cohorts still limit representativeness. Retain cross-flight concurrency and out-of-order traffic as separate stress coverage when making architecture decisions.

## Select the actual benchmark executable

Run from the repository root on Linux using the [prepared pinned toolchain](../verification/README.md). Capture Cargo's executable record instead of selecting an arbitrary file from `target/release/deps`:

```bash
set -euo pipefail
source target/verification-tools/environment.sh
run_root=$(mktemp -d "$PWD/target/two-host.XXXXXX")
cargo build --release --locked -p aerostore_core \
  --bench hyperfeed_contention_crucible --message-format=json \
  > "$run_root/build.jsonl"
bench_binary=$(python3 - "$run_root/build.jsonl" <<'PY'
import json, sys
paths = set()
with open(sys.argv[1]) as source:
    for line in source:
        event = json.loads(line)
        target = event.get("target", {})
        if (event.get("reason") == "compiler-artifact"
                and target.get("name") == "hyperfeed_contention_crucible"
                and "bench" in target.get("kind", [])
                and event.get("executable")):
            paths.add(event["executable"])
if len(paths) != 1:
    raise SystemExit(f"expected one benchmark executable, found {len(paths)}")
print(paths.pop())
PY
)
sha256sum "$bench_binary" > "$run_root/binary.sha256"
```

Preserve the build log, compiler/toolchain identity and source inventory alongside new results. A later source or binary change requires new correctness companions; rebuilding does not retroactively change the archived evidence.

If a fresh local check is needed after a change, this reproduces the validated command shape:

```bash
python3 scripts/run_remote_contention.py \
  --binary "$bench_binary" --output-dir "$run_root/loopback-full" \
  --workload fleet --families 16 --hot-percent 0 --seed 20260925 \
  --workers 4 --seconds 2 --arrival-rate 100 --evidence full \
  --max-backlog 1000 --max-messages 20000 --shm-mib 256 --query-plan family
```

The helper creates an isolated owner and worker processes, validates the completion handshake and final state, and records their bounded supervision. It does not install a service or contact SSH without `--ssh-host`.

## Prepare host A and host B

Host **A** runs the same benchmark client and workers for both engines. Host **B** runs either the AeroStore owner or PostgreSQL. Run the engines sequentially on the same database host, network path and storage class. This tests one worker host talking to one database host; it does not cover several simultaneous MMHF worker hosts.

Before using the remote commands, provide:

- Two compatible Linux hosts. Host B needs Python 3, `sha256sum`, a writable scratch directory and a preinstalled benchmark executable with exactly the same SHA-256 as host A. The helper neither uploads nor installs it. SSH must work in `BatchMode`; the advertised numeric IP and chosen TCP port must be reachable from A.
- A dedicated PostgreSQL test database on B. Its role needs connection/schema-creation privileges, ownership of the created scratch schemas and access to the diagnostic/WAL functions used by the adapter. The adapter creates and later removes only its marked per-run schemas.
- Explicit PostgreSQL settings matching the investigated baseline: `fsync=on`, `full_page_writes=on`, `wal_writer_delay=10s`, `deadlock_timeout=10ms` and autovacuum enabled. Configure restricted settings through the authorized server administrator; an ordinary benchmark role cannot be assumed to set them. The adapter sets asynchronous business acknowledgements, a 60-second statement timeout and `SERIALIZABLE` transactions. Use buffered writes. Record actual settings, version, query plans and maintenance counters.
- An isolated trusted test network. Service RPC has no authentication or TLS; the PostgreSQL adapter uses `NoTls`, so its connection must permit plaintext (`sslmode=disable` makes that explicit). SSH protects orchestration traffic, not either benchmark data connection. This is not a deployment recipe for an untrusted network.

The existing [PostgreSQL evidence](bench_data/architecture_2026-09-25/postgres-and-network/README.md) records the local baseline. Recheck the server settings on B instead of assuming those local values carry over. A normal final WAL drain does not establish equivalent crash durability or recovery.

## Matched 30-second commands for a future pair

Run these on A only after B is available and prepared. Replace the documentation placeholders. Supply the PostgreSQL connection string, including any required credentials, through your local secret handling in `AEROSTORE_REMOTE_PG_URL`; it must point to PostgreSQL on the same B address. The qualification driver redacts that value from its saved public command.

```bash
database_ssh='bench@DATABASE_HOST'
database_ip='192.0.2.20'              # Replace with B's reachable private IP.
database_binary='/srv/bench/hyperfeed_contention_crucible'
: "${AEROSTORE_REMOTE_PG_URL:?Set the dedicated PostgreSQL connection string for B}"
export AEROSTORE_REMOTE_PG_URL

common=(--binary "$bench_binary" --workload fleet --families 16 --hot-percent 0
        --workers 4 --seconds 30 --max-backlog 1000 --max-messages 20000
        --shm-mib 256)
remote=("${common[@]}" --ssh-host "$database_ssh"
        --remote-binary "$database_binary" --advertise-address "$database_ip"
        --bind "$database_ip:46200" --seed 20260925 --arrival-rate 100
        --query-plan family)
postgres=("${common[@]}" --engines postgres --pg-url-env AEROSTORE_REMOTE_PG_URL
          --seeds 20260925 --rates 100 --pg-write-mode buffered --slo-ms 50)

python3 scripts/run_remote_contention.py "${remote[@]}" \
  --output-dir "$run_root/service-full" --evidence full
python3 scripts/qualify_hyperfeed.py "${postgres[@]}" \
  --output "$run_root/postgres-full" --evidence full

python3 scripts/run_remote_contention.py "${remote[@]}" \
  --output-dir "$run_root/service-metrics" --evidence metrics
python3 scripts/qualify_hyperfeed.py "${postgres[@]}" \
  --output "$run_root/postgres-metrics" --evidence metrics \
  --correctness-report "$run_root/postgres-full/campaign.json"
```

Here 50 ms is a declared synthetic test budget, not a HyperFeed SLA. Both paths offer the same 3,000-message corpus with the same worker count, seed and workload parameters. The PG driver uses the family query plan. The remote helper creates a unique directory on B, verifies the binary hash, runs the owner there, and transfers setup/final frames through SSH. Its default deadline is duration plus 240 seconds; remote owner/descendant supervision remains bounded if the client disconnects.

This is a **single-seed functional/manual pair**, not an automated remote qualification campaign. `qualify_hyperfeed.py` does not accept `service-remote`, so it cannot combine these two outputs into its capacity gate. The PostgreSQL metrics companion requires exact source/binary/configuration agreement. The remote metrics run has no automated companion gate. For either engine, a full-history companion does not prove an unrecorded metrics history. Inspect all failures, useful-work counts and final populations before considering latency or throughput.

## What to retain and what remains unqualified

Keep reports, complete full-mode histories, serial witnesses, build/source identities, orchestration/supervisor records and settings. Verify ordered client timestamps and `elapsed_seconds_including_drain = (drain_confirmed_ns - admission_started_ns) / 1e9`. Remote timing includes delivery of the final owner snapshot/audit confirmation; it never subtracts clocks on different hosts.

Record host identities, CPU/affinity, memory limits, kernel, storage, network/RTT and interference on **both** A and B. Current host metadata covers the client host only. Declared CPU budgets are not enforced, and the native arena setting is not an equal total-memory budget for PostgreSQL. Relation-file bytes are not RSS. The harness deliberately leaves physical-host verification and architecture-promotion flags false; SSH or TCP alone must not be presented as proof of either.

Choosing an architecture still requires the calibrated cadence/ordering profile, repeated rate/worker/seed trials, observed execution overlap, a PostgreSQL saturation bracket, longer retention runs and a matched durability contract. The requirement that other workers keep running also needs remote worker-failure/survivor checks. A clean owner shutdown and the existing local failure tests do not by themselves establish MMHF availability or host-crash recovery.
