The database-owned service prototype passed selected application-client failure
tests over Unix sockets and loopback TCP. A separate survivor process keeps its
session across five victim SIGKILL cuts, then commits against both the same row
and a disjoint family. No mappings or registrations are reset. This does not
repair the existing direct-worker mode or establish service-crash recovery.

The [release Unix](release/unix.json) and [TCP](release/tcp.json) reports contain
the actual native adapter results; [debug](debug/tests.log) and
[release](release/tests.log) runs each passed 27 tests. Fifteen are inherited
pure transaction-model tests, eight check protocol edge cases, three are process
entry points, and one runs the combined Unix/TCP real-native campaign. The
logged injected executor panic is an intentional test: it checks that the owner
closes existing sessions and reports failed shutdown rather than continuing
after uncertain engine health.

The five cuts are request acceptance, returned query, returned private write,
accepted commit request, and committed outcome recorded before reply. The last
two resolve to Committed after the client dies. The other transactions abort;
all native ProcArray registrations disappear. Savepoint rollback, a living
client's idle timeout, and 24 private-write disconnects are also checked. The
last twelve allocation high-water samples must plateau. This is a 16-row,
32 MiB functional fixture, not a realistic HyperFeed throughput comparison.

The [separate TLC receipt](tla/report.json) passes two complete finite searches,
one positive lost-reply/survivor witness, and four intended counterexamples.
The model treats native operations as atomic and assumes weak fairness for
conditional progress. It does not prove refinement to Rust. The new source
directory is enrolled in frozen-boundary change detection;
[two enrollment tests](boundary-tests.log) check coverage and rejection of a
model edit. Neither enrollment nor this independent TLC receipt changes the
existing 71-check component pilot's claims.

The [manifest](manifest.json) records all hashes and scope. The
[source bundle](source.tar.gz) contains the exact service, storage/native
adapters, test fixture, protocol model, and relevant runner sources associated
with these receipts. Reports retain their original temporary-output paths.
The engine's existing production sources remain at the recorded Git baseline;
the larger architecture archive supplies their unchanged-source audit.

Reproduce with the pinned compiler and local socket permissions:

```sh
source target/verification-tools/environment.sh
AEROSTORE_SERVICE_EVIDENCE="$PWD/target/service-prototype/release" \
  cargo test --release -p aerostore_core --test contention_service -- --nocapture
python3 verification/service_protocol/test_boundary.py
python3 verification/service_protocol/check.py
```

Independent review also found a reply-backpressure deadline gap: with a 250 ms transaction limit and four-second idle timeout, a non-reading client retained its transaction for 4.13 seconds. The [negative-control receipt](reply-deadline-negative-control/report.json), [log](reply-deadline-negative-control/tests.log), and exact failing source are retained. The repaired service caps replies by the open transaction deadline. A received fatal commit reply also closes its Client while preserving the unresolved token, preventing accidental reuse from overwriting it. Both regressions pass in the final debug and release suites.

The protocol model assumes accepted abstract commits succeed. Native conflicts, fatal results, and indeterminate commits are outside that model; separate native/protocol tests cover their implementation behavior.
