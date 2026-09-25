The service protocol model checks ownership and outcome bookkeeping for two
one-shot clients. One client may die; the other has an independent executor.
Transaction execution, successful commit, and abort before commit acceptance
are abstract atomic engine operations. Once accepted, a commit is assumed to
succeed in this model: native conflict, fatal, and indeterminate commit results
are not modeled. The implementation's handling of those results needs separate
native/protocol tests; conditional progress here does not assert that every
real accepted commit succeeds.
Weak fairness assumes that runnable executors and engine operations eventually
complete. It does not cover an engine thread stuck on an abandoned mutex.

Run `python3 verification/service_protocol/check.py`. The wrapper reuses the
existing strict, checksum-pinned TLC runner and records the associated Rust
source hashes. It does not modify the existing verification campaign or claim
a Rust refinement proof. The model checks safety and conditional progress,
demonstrates a lost-reply commit alongside survivor progress, and rejects four
intentional mutations: cancelling an accepted commit, leaking registration,
reexecuting an accepted commit, and treating an unknown outcome as an abort.

The implementation separately enforces frame, session, transaction-lifetime and
outcome-retention bounds. Native process tests kill application clients at
request acceptance, query completion, private write completion, commit
acceptance, and recorded commit before its reply. Those are service protocol
cuts, not arbitrary instruction-level native publication cuts. The model does
not include WAL durability, multirow publication, reclamation internals, socket
parsing, allocation failure, weak memory, authentication, or service restart.
The outcome resolver's Unknown result after expiry or a new service incarnation
never authorizes a transaction replay.
