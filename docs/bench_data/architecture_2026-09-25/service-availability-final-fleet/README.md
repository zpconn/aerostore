# Final frozen-source service regression rerun

All **27 debug tests passed** after the final fleet campaigns completed. The exact [test log](tests.log), [Unix report](unix.json), [TCP report](tcp.json), and [source/receipt hashes](manifest.json) are retained. These tests use the actual native adapter in disposable fixtures and kill application clients at five protocol cuts while an existing survivor session commits against the same and disjoint rows.

Every test-bundle source hash is identical to the [earlier repaired service snapshot](../service-availability/README.md). That snapshot retains the source bundle, debug/release receipts, deterministic pre-fix negative control, and separate bounded TLA checks. The fleet generator changed elsewhere in the workspace, but this service target imports the unchanged shared record model. This rerun records the final reviewed 370-entry workspace boundary explicitly.

This is selected application-client death containment and protocol regression evidence. It does not establish arbitrary native instruction kill coverage, service/engine crash recovery, crash durability, sustained performance, or a fix to direct shared-memory worker death. Reported kill-to-survivor timings include fixture observation and polling; they are not architecture latency measurements. Fleet workload failures remain failures.
