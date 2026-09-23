# Native transaction lifecycle schedules

This diagnostic runs actual Rust transactions and index operations, including
empty-query creation and key moves. It is native regression evidence, not a
formal refinement proof or performance measurement.

The two new `predicate_lifecycle_*` tests each exercise two commit cuts:

- Rows and postings are published but the writer is still registered.
- The writer is deregistered but its fresh predicate stamp is not published.

At each cut, a real reader starts and captures a snapshot. Another reader
attempts indexed lookup, and a reader with an earlier empty dependency attempts
commit. The existing bucket-contention hook confirms that both attempts encounter
held predicate guards. After releasing and joining the writer, the tests check
old/new row visibility, rejection of the reader started during publication, and
a successful complete query from a fresh reader. Key-move negative controls can
otherwise return an empty result for an old key whose row the reader still sees.

The only new execution seams are thread-local `cfg(test)` hooks around
`self.shm.end_transaction(registration)?` in `finish_transaction`. They are
installed from the existing row-publication hook, after temporary skiplist pins
have finished. Production builds contain none of these hooks. Parked writers
are released and joined before outcome assertions.

The existing ProcArray reserved-ID-gap test and indexed old-writer-after-newer-
writer test are reused. The gap test's lifecycle-latch observation is asserted
after worker cleanup, so a missing registration lock produces a normal failed
test instead of abandoning a parked writer. Its existing 20 ms receive window
encourages a raced snapshot, but detection of the missing registration lock does
not depend on that timeout: latch ownership is observed synchronously while the
writer is parked. No timeout counts as successful negative evidence.

```sh
python3 verification/lifecycle_native/run.py --output target/verification/lifecycle-native
```

Choose an empty output directory. The runner archives the parent commit's
workspace crates and overlays the current two native test files. Every variant
has its own source tree and Cargo target directory. It verifies the reviewed
Rust 1.93.1 compiler commit and records source, tool, archive, and log hashes.

Five native negative controls must produce their intended test assertions:
missing registration lifecycle lock, omitted active writers in snapshots,
stamping before deregistration, reusing the writer's old transaction ID as the
publication stamp, and releasing predicate guards before deregistration.
Compiler errors, process signals, timeouts, and zero selected tests are failures
of the diagnostic campaign. The receipt checks that main inputs remain stable
throughout the run. The source mutations are confined to archived copies.
