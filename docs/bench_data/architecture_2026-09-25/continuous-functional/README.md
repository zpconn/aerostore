# Continuous-clock remote functional check

The public remote orchestration path passed a full-history `fleet` check on the corrected continuous-clock build: 200 messages, a valid serial witness, passing final audits and eight retries. Configuration: 16 identities, `hot-percent=0`, four workers, two seconds, 100 offered messages/second and a 256 MiB arena setting. Both processes ran on one WSL host over TCP loopback. This is a functional check, not a capacity or physical MMHF result.

The four reported timestamps are ordered and share the client's monotonic clock:

| Timestamp | Nanoseconds |
| --- | ---: |
| Admission started | 16,906,299,525,616 |
| Workload completed | 16,908,299,690,608 |
| Workers stopped | 16,908,320,142,681 |
| Drain confirmation received | 16,908,522,967,428 |

Independent arithmetic exactly reproduces `elapsed_seconds_including_drain = 2.223441812` and `completed_messages_per_second_including_drain = 200 / 2.223441812 = 89.95063370698185`. The denominator includes 20.452073 ms between workload completion and worker shutdown, followed by 202.824747 ms until the client receives remote drain confirmation. That latter interval includes the remote confirmation protocol; it is not a measurement of WAL flushing alone. No server/client clock subtraction is used.

The executed binary was an immutable private copy with SHA-256 `346d9393338c7ebdbce5c8975f05a06057176d0a53ef9d4de5739dcdc4db20a3`, checked before and after execution. The source inventory is `d817ba814d857356f29c70d1f5dce340e380ce453601d028b69df939df6a5c4a`; see [continuous-build-provenance.json](../continuous-build-provenance.json). Earlier [fleet functional receipts](../fleet-functional/README.md) and [retention evidence](../fleet-retention/README.md) remain separate and retain their original throughput-accounting limitation.

The archive contains the unmodified client report, full transaction history, initial/final states, serial witness, setup/final frames, execution command, timing-identity checks, orchestration and supervisor reports, and logs. Larger files are losslessly gzip-compressed. The manifest records archived hashes and original uncompressed hashes/paths. It omits binaries, database files and semantically duplicate server-frame copies. Business acknowledgements remain asynchronous, and RPC is restricted to a trusted fixture network without authentication or TLS; this clock correction does not establish recovery equivalence or durable business acknowledgements.
