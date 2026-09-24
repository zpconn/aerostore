# Native publication and completion composition

This conditional Verus slice joins ordinary row/index publication to the actual
transaction deregistration and same-clock predicate stamping operations. It is
one selected write and one index, admitted after validation under guards. It
does not prove the complete P1 scenario from the public transaction APIs.

`generate.py` embeds the exact current `commit_data` and `lifecycle_scenario`
modules and extracts native `finish_transaction`. The native commit suffix is
checked for finish, stamping, error poison and guard release order. The composed
call order is checked separately because state postconditions on disjoint model
views cannot by themselves establish the native concurrent schedule. Only the
two test hooks are removed from finish; taking the registration and propagating
the actual end result are preserved. The lifecycle guard's native RAII release
is represented by its existing explicit primitive release operation.

The proof establishes:

- Successful publication has the exact selected row/index relation and returned
  write report supplied by the ordinary publisher.
- Successful finish consumes the token and removes the writer's registration.
  A valid owned registration succeeds; failure also consumes the token, matching
  native `Option::take` behavior.
- Stamping follows deregistration and reserves the shared ProcArray clock. The
  returned stamp exceeds the writer ID and updates precisely affected buckets.
- Those stamps invalidate a dependency in an affected bucket under the explicit
  earlier-reader clock bound. This lemma does not itself capture a dependency or
  call the native validator; that operation composition remains open.
- A data error leaves the old data or poisons it. Deregistration/stamp errors
  after publication poison storage; they are not clean aborts.

A constructed live registration/stamp state witnesses the completion premises.
The embedded data campaign separately supplies create, move and delete witnesses
and a consistent primitive implementation. These witnesses check inhabitation;
they are not a proof that every native transaction reaches the admitted state.

## Explicit remaining boundaries

The incoming plan must be the complete selected native final-write plan. This
slice checks its row/base/new-pointer and transaction-ID agreement but does not
prove `final_write_indices`, extraction or validation establishes that plan.
Extra pending writes are permitted in the representation; completeness and
one-selected-write admission remain caller obligations.

The posting, row and stamping projections must refer to the same native table,
registered index and arena. Their equal binding counts do not establish physical
identity. Disjoint modeled fields do not prove that native shared-memory writes
are disjoint. Guard ownership, initialization, allocation authority, low-level
atomics and framing remain explicit primitive assumptions. Cleanup between row
publication and finish is assumed to preserve the modeled published projections.
Finish also retains the lifecycle component's acquired-state frame; it does not
prove arbitrary ProcArray interference while waiting for the lifecycle mutex.
The data-error branch omits native cleanup/finish and makes no claim about its
token or registration state.

This is a source-bound cutpoint composition, not an arbitrary-history theorem,
a Lean-to-Rust translation proof, a whole-commit refinement, a WAL/recovery
composition, or a native pointer/weak-memory proof. The handwritten join and
adapters remain part of the trusted boundary. Full P1 remains open.

## Checks

```sh
python3 verification/commit_completion/generate.py --check
python3 verification/commit_completion/test_generate.py
python3 verification/commit_completion/run.py --output target/verification/commit-completion
```

The runner verifies the complete generated crate and six individual roots with
pinned Verus, then requires fourteen targeted semantic mutations to fail for
proof reasons. The controls remove publication, finish, stamps, poison or key
premises; corrupt registration handling and change translation; return a wrong
stamp; and erase capture freshness. Compiler failures do not count. Receipts
retain commands, toolchain/source/log hashes and before/after input fingerprints.
There are no production code changes or runtime proof checks in this slice.
