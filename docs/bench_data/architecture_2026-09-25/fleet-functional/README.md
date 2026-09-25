# Fleet functional follow-up, September 25, 2026

The new `fleet` workload passed three short full-history checks on one fixed build. These checks validate workload/backend integration and the remote lifecycle; they do not measure sustainable capacity or establish a 10× improvement.

| Path | Configured identities | Completed messages | Live families, initial → final | Retries | Serial-history oracle |
| --- | ---: | ---: | ---: | ---: | --- |
| AeroStore direct mapping | 64 | 200 | 44 → 45 | 0 | Valid |
| Native PostgreSQL Unix socket | 64 | 200 | 44 → 45 | 0 | Valid |
| AeroStore service, public remote orchestration over TCP loopback | 16 | 200 | 12 → 15 | 1 | Valid |

Each run offered 100 messages/second for two seconds with four workers, `hot-percent=0`, a 256 MiB arena setting and full transaction observations. Initial/final population counts are snapshots, not a measured runtime minimum. The different fleet sizes make these functional cases unsuitable for comparing throughput or latency across paths. The two native-host engines used the same 64-identity corpus and reached matching final population counts.

This preserved build also has a known throughput-accounting gap: `completed_messages_per_second_including_drain` omits an interval between the workload timer and the drain timer around worker shutdown. That field is not complete shutdown-inclusive throughput. It does not invalidate the full histories, state checks or per-message timestamps reported here.

The remote service completed its independent final audit, drained its WAL/GC work, and reported zero active sessions, retained outcomes or transaction registrations, with no backend failures. Both supervisor reports confirm bounded owner/descendant cleanup. The seven orchestration unit tests passed, including stale-run rejection, shell argument handling, owner deadline enforcement and orphaned descendant cleanup.

“Remote” identifies the public server/client orchestration path. Both processes ran on this one WSL host over TCP loopback. No SSH machine was contacted and no physical MMHF topology was measured. RPC remains a trusted-fixture protocol without authentication or TLS. PostgreSQL was the existing isolated native 16.13 process over its private Unix socket, with buffered SERIALIZABLE writes and asynchronous business acknowledgements followed by the explicit post-work WAL flush fence. See the [PostgreSQL contract/settings archive](../postgres-and-network/README.md).

## Identity and evidence

The executed binary was copied to a private path and checked before and after execution: `f37b288b38be0b86a29f1db32079f5819c4fe28220937219f41260c9c8549033`. The source inventory is `7b02ad0fd7c2e1a04db8603c4321015c10c4d82ee3bfaa2cfd6adda457558d87`. See [fleet-build-provenance.json](../fleet-build-provenance.json) and this folder's execution record. The executable itself and database files are not archived.

The first attempt, under `target/architecture-qualification-2026-09-25/fleet-functional-followup`, passed functionally but observed a shared binary replacement during the run sequence. It is explicitly marked `NONFINAL.json` and excluded from these final receipts. The final checks were repeated on the immutable copy under `target/architecture-qualification-2026-09-25/fleet-functional-final`. This evidence is distinct from the earlier [turnover-stress archive](../stress-retention/README.md) and leaves its provenance unchanged.

The archive retains unmodified reports, logs, execution commands, supervisor/orchestration records, full transaction histories, initial/final states and serial witnesses. Larger inputs are losslessly gzip-compressed. The manifest records both archived bytes and original paths/hashes so decompression can be checked without relying on mutable working-tree source. Timings in raw reports are incidental diagnostics from these short functional checks.
