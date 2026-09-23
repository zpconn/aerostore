# Native lock ownership and interference

This campaign verifies the selected **current native lock operations** using
opaque, affine leases. It replaces the earlier guard campaign's assumption that
`try_lock` simply returns ownership with a checked transition from the native
successful CAS, through held permission, to the native consuming release.
Nothing in this directory is linked into the database.

## Source and ownership

The restricted adapter extracts:

- `ShmMutex::try_acquire`: its actual `compare_exchange(0, 1, Acquire, Relaxed)`,
  success/failure selection, and guard construction;
- `ShmMutex::try_lock`: both priority observations, the acquisition, and the
  explicit priority-race guard drop;
- `ShmMutexGuard::drop`: the actual store of zero with Release ordering;
- `SecondaryIndex::transactional_try_lock_bucket`: header, poison/bounds checks,
  selected bucket, and its actual lock call;
- `OccTable::acquire_index_bucket`: the native bounded retry loop and its
  yield/spin branches.

`Authority` is a tracked ghost resource for a physical arena's mutex states.
Each `ReadLease` contains a private tracked token identifying one grant. Neither
has a public constructor; the private initializer exists only for consistency
witnesses. Native guard fields, its lifetime-bound borrowed mutex reference,
non-Send marker and sole constructor are checked by the adapter. The model's
leases cannot be copied or forged from numeric arena/header/bucket fields.

A successful CAS grants one fresh lease at that atomic event. Release consumes
that lease and revokes its authorization at the native store. The proof checks
that a competing acquisition fails while a lease is held, that a subsequent
handoff gets a fresh grant, and that unrelated lock operations preserve borrowed
permissions. Ghost grant numbers are unbounded proof identities, not a new native
counter or a runtime overflow fix.

The index wrappers additionally require `physical_domain(driver, authority)`:
every registered header's in-range bucket must already belong to that authority.
The raw bucket resolver requires membership for an in-range request; it cannot
conjure a lock from another arena or an empty authority. Domain correspondence is
an explicit native resolution premise, preserved as other actors change ownership.
A total reply witness proves that each admitted bucket request has an allowed
response; a concrete one-bucket witness permits resolution and a successful grant.

The public `read_permission` API borrows `&ReadLease` and `Tracked<&Authority>` and
requires current authorization. Protected stamp/raw-index primitives must take
those borrowed resources too; the returned numeric key alone is **not** permission
for a later access. Moving the lease into `drop_lease` or `release_all` makes the
old lease unavailable to such calls. The adjacent indexed-read composition uses
this distinction to copy candidate IDs under guards, then materialize those IDs
after consuming the guards under its separate snapshot/MVCC contract.

## Interference is represented

Multi-step acquisition does **not** frame all foreign state while waiting.
`Scheduler::interfere` supplies a finite trace of legal Acquire, Release and
Observe events. Acquire and Release are checked refinements of the extracted CAS
and Drop bodies; an inductive trace proof derives protection of the caller's
actual borrowed lease IDs. The rely contract excludes consuming those IDs,
rather than asserting that unrelated metadata remained unchanged.

Source lowering admits these traces before priority observations, before CAS,
and at retry yield/spin points. After successful CAS, the wrapper adds its new
local lease to the protected set through the priority recheck. It can release a
foreign-held target and acquire it on a subsequent attempt. The wrappers promise
preservation of the caller's listed leases and correct returned identity, not
unchanged foreign cells on failure or success based on the entry lock bit.

`release_all` also admits interference between consuming releases. It proves that
all old consumed lease IDs are unauthorized at exit; it does **not** claim that
every released location is still unlocked. A foreign actor may already own a new
lease there. The checked monotone-grant theorem prevents an old token from being
reactivated. The helper is a verified lowering of destruction of distinct guard
resources; correspondence to Rust Vec moves/destruction and its legal destruction
order remains a frontend obligation, not an implementation proof of Vec.

Positive witnesses show that the rely relation permits a foreign target release
while retaining a local owner, and permits reacquisition of a released location
without restoring the old lease. Another witness calls actual extracted failed
CAS, foreign-owner Drop, and successful CAS in sequence. These establish useful
permitted interleavings and consistency, not fairness or scheduler guarantees.

## Explicit primitive boundary

- One authoritative state must be coupled to each uniquely resolved physical
  mutex. Arena initialization, actual address/offset resolution, mapping identity,
  and pointer validity remain obligations. Clients cannot mint another authority
  through this proof API, but the native bootstrap/coupling is not proved here.
- Atomic contracts supply the native compare-exchange/store linearization and
  priority observations with the checked ordering arguments. The proof does not
  establish all Rust weak-memory or cross-process mmap semantics. Existing native
  Loom campaigns exercise the actual lock's handoff and weakened-Acquire control;
  those finite schedules support, but do not replace, this boundary.
- Legal environment traces use the same ownership protocol and cannot consume
  the caller's borrowed tokens. Connecting arbitrary native threads, unsafe code,
  and all call sites to that rely condition remains a correspondence obligation.
  This is stronger than a frozen acquired-input frame, but is not an unrestricted
  theorem about arbitrary writes to mutex bytes.
- Exclusive recovery/reset requires quiescence. Owner death, unsafe guard
  duplication, forgotten guards, poisoning policy, priority-counter overflow,
  allocator behavior and unbounded progress are outside this proof. The bounded
  acquisition can return a contention error.
- Higher-level row/MVCC behavior, raw candidate completeness, transaction histories
  and whole-engine correctness are proved separately or remain open. No timing or
  performance improvement is claimed; production source is unchanged.

## Checks and reproduction

```sh
python3 verification/guard_ownership/generate.py --check
python3 -m unittest discover -s verification/guard_ownership -p test_generate.py
python3 verification/guard_ownership/run.py --output target/verification/guard-ownership
```

The runner authenticates the pinned verifier and exact input/log hashes, checks
all named roots, and rejects semantic controls only for solver obligation failures.
Controls alter native CAS arguments, orderings, success selection, Drop, selected
bucket, ownership identity, protection sets, release cleanup and the rely relation.
The foreign-release witness rejects an accidentally frozen interference relation.
Omitting explicit `drop(guard)` is deliberately **not** a semantic negative:
ordinary Rust implicit Drop would still release it at return. The restricted
adapter instead rejects that changed syntax pending a reviewed RAII lowering.

Separately classified type controls must fail with the expected Rust or Verus diagnostic for
double Drop, copying a borrowed lease, forging a numeric lease and accessing the
private authority initializer. They are not counted as solver-rejected semantic
mutants. Parser errors, unrelated compiler errors and timeouts do not count as
evidence for either category. The receipt retains the two categories separately.
