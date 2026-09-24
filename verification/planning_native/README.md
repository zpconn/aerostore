# Native final-write planning evidence

This campaign exercises native public `OccTable` APIs. It is deterministic
scenario testing, not a formal refinement proof or evidence that P1 is complete.
The scenarios complement the symbolic planning and validation proofs with actual
allocation, index storage, transaction cleanup and returned commit records.

Three new tests cover these outcomes:

- Interleaved repeated writes to three rows, including a nested discarded suffix,
  yield exactly one report entry per row in row order. Each entry carries the
  original committed base offset/value, last retained private offset/value, and
  final explicit dirty mask. The committed heads and complete index contents
  agree with that report; intermediate and rolled-back keys leave no postings.
- An update that temporarily changes the indexed key but finishes with the
  original key keeps precisely the original posting. A reader of the abandoned
  intermediate key retains its empty predicate through commit.
- A writer with one uncontested row and one stale write base rejects before
  publishing either row or changing the index. The uncontested row sorts first,
  making publication of a prefix observable if validation is skipped. Both
  older and newer winning-transaction registrations are covered; the rejected
  transaction closes and a fresh two-row retry succeeds.

The existing nested-savepoint/abort regression is reused as a fourth positive
case. Eleven native-source controls select first writes, omit a final row,
resolve either index key from the wrong pointer, emit a spurious unchanged-key
change, bypass write-base validation, or corrupt one of the returned base/new
offsets, values, and dirty masks. Pointer-report mutations alter only the report;
they do not intentionally create invalid raw pointers or cyclic row chains.
Every control must compile and fail exactly the intended semantic assertion in
one selected test. Compilation errors, other assertions, missing tests, signals,
and timeouts never count as successful negative evidence.

```sh
source target/verification-tools/environment.sh
python3 -m unittest discover -s verification/planning_native -p 'test_*.py'
python3 verification/planning_native/run.py --output target/verification/planning-native
```

The runner pins the native Rust compiler, snapshots all four workspace crates,
overlays every current Rust/Cargo input, and builds each mutant in its own source
and target directory. Its receipt records input, compiler, executable, log and
final source freshness hashes. Inputs must remain stable for the full campaign.
The scenario schedules are sequential interleavings of overlapping transactions;
no production hooks, runtime edits, or timing assumptions are introduced. These
cases run with default buckets here and with all bucket implementations in the
regular transaction/index integration matrix.
