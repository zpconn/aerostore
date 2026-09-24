# Complete-or-retry indexed lookup mathematics

[QueryCompleteness.lean](AerostoreProofs/QueryCompleteness.lean) proves a query
result from publication history, rather than assuming that current index
candidates already cover historical snapshot rows. Its definitions and eight
audited root types are frozen together. This is abstract mathematics; native
execution, heap, retained-chain and atomic correspondence remain separate work.

A history event names a stable row ID, its prior and replacement extracted
keys, a writer identity/start ID, and its publication reservation label. `none`
means that a row has no indexed key, including a logical deletion from the
index. It does not assert physical deletion of an AeroStore row. The baseline
row map describes versions already committed before the modeled history.

The premises describe independent protocol facts:

- Each event's old key equals the preceding live row key. Posting replay removes
  that exact old-key/row pair and inserts its new pair. The theorem derives
  correspondence between those maintained postings and the final live row map.
- Overlapping buckets receive nondecreasing publication labels. Equal labels
  admit multiple changed rows from one transaction. Disjoint stores may reorder;
  the proof does not impose a globally increasing physical store order.
- Event writer starts and publication labels map into a reachable lifecycle
  history. Active IDs in the reader's snapshot map to its observed writers.
  Lifecycle induction derives that publication follows the writer's own start
  and that an observed active writer publishes after the reader starts.
- The snapshot's upper bound exceeds its reader ID, matching registration and
  snapshot capture. Every matching key maps into the query's locked bucket set.
  The query accepts only stamps below its reader ID.

The proof derives that every query-affecting event with an accepted stamp must
be visible to the reader. Replaying those events preserves predicate membership
between live postings and the snapshot-visible row map. A missing matching
historical posting therefore implies a visited bucket whose stamp forces retry.
Neither candidate completeness nor the missing-candidate/retry conclusion is
an input premise.

Materialization unions current posting candidates with every private write,
then filters the snapshot plus private overlay. The resulting set equals all
matching rows, including private insertions, moves and keyless writes. Arbitrary
extra candidates are allowed: filtering removes stale candidates. The same
contract proves both no missing rows and no extra rows.

The native `xmin`/`xmax` branch order is represented separately. The two-version
selection root proves that an invisible replacement leaves its retained old
version visible and that a visible replacement excludes its old version. It
preserves the creator's own-transaction early return, the active-ID checks and
the deletion boundary. This is a two-version result; the file does not prove a
general raw version-chain traversal or safe retention after releasing guards.

Eight roots cover overlapping stamp history, writer chronology, visibility,
exact posting replay, complete successful lookup, missing-candidate retry,
two-version selection and live witnesses. Seven semantic controls remove old
bucket coverage, omit destination postings, ignore creator/deleter active IDs,
omit private candidates, allow stamp regression or filter before applying the
private overlay. Concrete witnesses include reordered disjoint labels, a
removed old-key posting, private deletion and an insertion with no raw candidate.

The source-bound Verus lookup campaign separately checks the native algorithms.
No Lean theorem is imported as an unchecked Verus assumption. The eventual
history join must establish event completeness, raw posting enumeration,
snapshot construction, guard ownership and retained-chain correspondence for
the actual concurrent execution, including changes after candidate capture.
