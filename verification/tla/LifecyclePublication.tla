--------------------------- MODULE LifecyclePublication ---------------------------
EXTENDS Integers, FiniteSets

\* Finite lifecycle/publication refinement of the earlier Publication model.
\* Reserve and register are separate steps under one lifecycle owner; snapshot
\* metadata and deregistration use the same owner. Publication reservations
\* remain outside that owner but under affected bucket guards. Raw MVCC rows,
\* per-bucket atomics and real mutex/mmap behavior remain abstract primitives.
CONSTANTS Scenario, Mutation, BucketCount, Witness
Tx == {1, 2}
Rows == {1, 2}
Buckets == 0..(BucketCount - 1)
Bucket(k) == k % BucketCount
QueryKey(t) == IF Scenario = "Disjoint" THEN t ELSE 1
WriteRow(t) == IF Scenario = "Move" THEN 1 ELSE t
WriteKey(t) == IF Scenario = "Move" THEN 2 ELSE QueryKey(t)
InitialRows == [r \in Rows |-> IF Scenario = "Move" /\ r = 1 THEN 1 ELSE 0]

VARIABLES pc, nextId, txid, active, snapRows, logicalRows, rowHeads,
          postings, owner, stamps, dependencies, captured, answers, answered,
          willWrite, committed, rejected, lifecycle, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, beganBetweenReserveAndStore
vars == <<pc, nextId, txid, active, snapRows, logicalRows, rowHeads,
          postings, owner, stamps, dependencies, captured, answers, answered,
          willWrite, committed, rejected, lifecycle, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, beganBetweenReserveAndStore>>

\* A published physical head is still uncommitted while its writer remains
\* registered. Keep one old version per row in this two-transaction model.
\* Actual version traversal and safe retention remain primitive assumptions.
VisibleRows == [r \in Rows |->
    LET writers == {t \in active : willWrite[t] /\ WriteRow(t) = r /\ pc[t] \in {"PreStamp", "EndLock", "Deregister"}}
    IN IF writers = {} THEN rowHeads[r] ELSE snapRows[CHOOSE t \in writers: TRUE][r]]

Init ==
    /\ pc = [t \in Tx |-> "Start"]
    /\ nextId = 1
    /\ txid = [t \in Tx |-> 0]
    /\ active = {}
    /\ snapRows = [t \in Tx |-> InitialRows]
    /\ logicalRows = InitialRows /\ rowHeads = InitialRows
    /\ postings = {<<InitialRows[r], r>> : r \in {x \in Rows : InitialRows[x] # 0}}
    /\ owner = [b \in Buckets |-> 0]
    /\ stamps = [b \in Buckets |-> 0]
    /\ dependencies = [t \in Tx |-> [b \in Buckets |-> -1]]
    /\ captured = [t \in Tx |-> {}]
    /\ answers = [t \in Tx |-> [rows |-> {}, afterMove |-> FALSE]]
    /\ answered = {}
    /\ willWrite = [t \in Tx |-> FALSE]
    /\ committed = {} /\ rejected = {}
    /\ lifecycle = 0 /\ ended = {} /\ snapped = {}
    /\ snapshotXmax = [t \in Tx |-> 0]
    /\ snapshotActive = [t \in Tx |-> {}]
    /\ snapshotEnded = [t \in Tx |-> {}]
    /\ publicationStamp = [t \in Tx |-> 0]
    /\ beganBetweenReserveAndStore = {}

LifecycleFree == lifecycle = 0
OwnLifecycle(t) == lifecycle = t \/ Mutation = "NoLifecycle"
StartLock(t) ==
    /\ pc[t] = "Start" /\ LifecycleFree
    /\ (Scenario # "Move" \/ t = 1 \/ txid[1] > 0)
    /\ lifecycle' = IF Mutation = "NoLifecycle" THEN 0 ELSE t
    /\ pc' = [pc EXCEPT ![t] = "Reserve"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, nextId, txid, active, snapRows, logicalRows, rowHeads, postings,
        owner, stamps, dependencies, captured, answers, answered, willWrite, committed, rejected>>
Reserve(t) ==
    /\ pc[t] = "Reserve" /\ OwnLifecycle(t)
    /\ txid' = [txid EXCEPT ![t] = nextId]
    /\ beganBetweenReserveAndStore' = IF \E writer \in Tx: pc[writer] = "StoreStamp"
        THEN beganBetweenReserveAndStore \cup {t} ELSE beganBetweenReserveAndStore
    /\ nextId' = nextId + 1
    /\ pc' = [pc EXCEPT ![t] = "Register"]
    /\ UNCHANGED <<lifecycle, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, active, snapRows, logicalRows, rowHeads, postings, owner, stamps,
        dependencies, captured, answers, answered, willWrite, committed, rejected>>
Register(t) ==
    /\ pc[t] = "Register" /\ OwnLifecycle(t)
    /\ active' = active \cup {t}
    /\ pc' = [pc EXCEPT ![t] = "BeginUnlock"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, lifecycle, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, nextId, txid, snapRows, logicalRows, rowHeads, postings, owner, stamps,
        dependencies, captured, answers, answered, willWrite, committed, rejected>>
BeginUnlock(t) ==
    /\ pc[t] = "BeginUnlock" /\ OwnLifecycle(t)
    /\ lifecycle' = 0
    /\ pc' = [pc EXCEPT ![t] = "SnapshotLock"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, nextId, txid, active, snapRows, logicalRows, rowHeads, postings, owner, stamps,
        dependencies, captured, answers, answered, willWrite, committed, rejected>>
SnapshotLock(t) ==
    /\ pc[t] = "SnapshotLock" /\ LifecycleFree
    /\ lifecycle' = IF Mutation = "NoLifecycle" THEN 0 ELSE t
    /\ pc' = [pc EXCEPT ![t] = "Snapshot"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, nextId, txid, active, snapRows, logicalRows, rowHeads, postings, owner, stamps,
        dependencies, captured, answers, answered, willWrite, committed, rejected>>
Snapshot(t) ==
    /\ pc[t] = "Snapshot" /\ OwnLifecycle(t)
    /\ snapRows' = [snapRows EXCEPT ![t] = VisibleRows]
    /\ snapshotXmax' = [snapshotXmax EXCEPT ![t] = nextId]
    /\ snapshotActive' = [snapshotActive EXCEPT ![t] = {txid[w] : w \in active}]
    /\ snapshotEnded' = [snapshotEnded EXCEPT ![t] = ended]
    /\ snapped' = snapped \cup {t}
    /\ lifecycle' = 0
    /\ willWrite' = [willWrite EXCEPT ![t] = (Scenario = "Move" /\ t = 1)]
    /\ pc' = [pc EXCEPT ![t] = IF Scenario = "Move" /\ t = 1 THEN "Acquire" ELSE "LookupLock"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, ended, publicationStamp, nextId, txid, active, logicalRows, rowHeads, postings, owner,
                   stamps, dependencies, captured, answers, answered, committed, rejected>>

LookupLock(t) ==
    /\ pc[t] = "LookupLock" /\ owner[Bucket(QueryKey(t))] = 0
    /\ owner' = [owner EXCEPT ![Bucket(QueryKey(t))] = t]
    /\ pc' = [pc EXCEPT ![t] = "Capture"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, lifecycle, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, nextId, txid, active, snapRows, logicalRows, rowHeads,
                   postings, stamps, dependencies, captured, answers, answered,
                   willWrite, committed, rejected>>

Capture(t) ==
    /\ pc[t] = "Capture"
    /\ LET b == Bucket(QueryKey(t))
           candidates == {r \in Rows : <<QueryKey(t), r>> \in postings}
       IN /\ pc' = [pc EXCEPT ![t] = IF stamps[b] >= txid[t] THEN "Reject" ELSE "Materialize"]
          /\ captured' = [captured EXCEPT ![t] = candidates]
          /\ dependencies' = IF Mutation = "OmitEmpty" /\ candidates = {}
                              THEN dependencies
                              ELSE [dependencies EXCEPT ![t][b] = stamps[b]]
          /\ owner' = [owner EXCEPT ![b] = 0]
    /\ UNCHANGED <<beganBetweenReserveAndStore, lifecycle, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, nextId, txid, active, snapRows, logicalRows, rowHeads, postings,
                   stamps, answers, answered, willWrite, committed, rejected>>

Materialize(t) ==
    /\ pc[t] = "Materialize"
    /\ LET result == {r \in captured[t] : snapRows[t][r] = QueryKey(t)}
       IN /\ answers' = [answers EXCEPT ![t] = [rows |-> result,
                        afterMove |-> (Scenario = "Move" /\ logicalRows[1] = 2)]]
          /\ willWrite' = [willWrite EXCEPT ![t] = (Scenario # "Move" /\ result = {})]
    /\ answered' = answered \cup {t}
    /\ pc' = [pc EXCEPT ![t] = "Acquire"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, lifecycle, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, nextId, txid, active, snapRows, logicalRows, rowHeads, postings,
                   owner, stamps, dependencies, captured, committed, rejected>>

ChangedBuckets(t) == IF ~willWrite[t] THEN {} ELSE
    {Bucket(k) : k \in ({snapRows[t][WriteRow(t)], WriteKey(t)} \ {0})}
Needed(t) == ChangedBuckets(t) \cup {b \in Buckets : dependencies[t][b] >= 0}
Missing(t) == {b \in Needed(t) : owner[b] # t}

AcquireOne(t) ==
    /\ pc[t] = "Acquire" /\ Missing(t) # {}
    /\ \E b \in Missing(t):
           /\ \A other \in Missing(t): b <= other
           /\ owner[b] = 0
           /\ owner' = [owner EXCEPT ![b] = t]
    /\ UNCHANGED <<beganBetweenReserveAndStore, lifecycle, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, pc, nextId, txid, active, snapRows, logicalRows, rowHeads,
                   postings, stamps, dependencies, captured, answers, answered,
                   willWrite, committed, rejected>>

Acquired(t) ==
    /\ pc[t] = "Acquire" /\ Missing(t) = {}
    /\ pc' = [pc EXCEPT ![t] = "Validate"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, lifecycle, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, nextId, txid, active, snapRows, logicalRows, rowHeads,
                   postings, owner, stamps, dependencies, captured, answers,
                   answered, willWrite, committed, rejected>>

Validate(t) ==
    /\ pc[t] = "Validate"
    /\ LET predicateOK == Mutation = "SkipValidation" \/
               (\A b \in Buckets: dependencies[t][b] < 0 \/
                    (stamps[b] = dependencies[t][b] /\ stamps[b] < txid[t]))
           baseOK == ~willWrite[t] \/ logicalRows[WriteRow(t)] = snapRows[t][WriteRow(t)]
       IN pc' = [pc EXCEPT ![t] = IF predicateOK /\ baseOK THEN "Prepare" ELSE "Reject"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, lifecycle, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, nextId, txid, active, snapRows, logicalRows, rowHeads,
                   postings, owner, stamps, dependencies, captured, answers,
                   answered, willWrite, committed, rejected>>

Prepare(t) ==
    /\ pc[t] = "Prepare"
    /\ postings' = IF willWrite[t] THEN postings \cup {<<WriteKey(t), WriteRow(t)>>} ELSE postings
    /\ pc' = [pc EXCEPT ![t] = "RemoveSource"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, lifecycle, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, nextId, txid, active, snapRows, logicalRows, rowHeads,
                   owner, stamps, dependencies, captured, answers, answered,
                   willWrite, committed, rejected>>

RemoveSource(t) ==
    /\ pc[t] = "RemoveSource"
    /\ postings' = IF willWrite[t] THEN postings \ {<<snapRows[t][WriteRow(t)], WriteRow(t)>>} ELSE postings
    /\ pc' = [pc EXCEPT ![t] = "PublishRow"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, lifecycle, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, nextId, txid, active, snapRows, logicalRows, rowHeads,
                   owner, stamps, dependencies, captured, answers, answered,
                   willWrite, committed, rejected>>

PublishRow(t) ==
    /\ pc[t] = "PublishRow"
    /\ rowHeads' = IF willWrite[t] THEN [rowHeads EXCEPT ![WriteRow(t)] = WriteKey(t)] ELSE rowHeads
    /\ owner' = IF Mutation = "EarlyUnlock" THEN [b \in Buckets |-> IF owner[b] = t THEN 0 ELSE owner[b]] ELSE owner
    /\ pc' = [pc EXCEPT ![t] = IF Mutation = "PrematureStamp" THEN "PreStamp" ELSE "EndLock"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, lifecycle, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, nextId, txid, active, snapRows, logicalRows, postings,
                   stamps, dependencies, captured, answers, answered,
                   willWrite, committed, rejected>>

PreStamp(t) ==
    /\ pc[t] = "PreStamp"
    /\ publicationStamp' = [publicationStamp EXCEPT ![t] = IF ChangedBuckets(t) = {} THEN 0 ELSE nextId]
    /\ nextId' = nextId + IF ChangedBuckets(t) = {} THEN 0 ELSE 1
    /\ pc' = [pc EXCEPT ![t] = "EndLock"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, stamps, lifecycle, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, txid, active, snapRows, logicalRows, rowHeads, postings, owner,
        dependencies, captured, answers, answered, willWrite, committed, rejected>>
EndLock(t) ==
    /\ pc[t] = "EndLock" /\ LifecycleFree
    /\ lifecycle' = IF Mutation = "NoLifecycle" THEN 0 ELSE t
    /\ pc' = [pc EXCEPT ![t] = "Deregister"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, nextId, txid, active, snapRows, logicalRows, rowHeads, postings, owner, stamps,
        dependencies, captured, answers, answered, willWrite, committed, rejected>>
Deregister(t) ==
    /\ pc[t] = "Deregister" /\ OwnLifecycle(t)
    /\ logicalRows' = IF willWrite[t] THEN [logicalRows EXCEPT ![WriteRow(t)] = WriteKey(t)] ELSE logicalRows
    /\ active' = active \ {t} /\ ended' = ended \cup {t}
    /\ lifecycle' = 0
    /\ pc' = [pc EXCEPT ![t] = IF Mutation = "PrematureStamp" THEN "StoreStamp" ELSE "Stamp"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, nextId, txid, snapRows, rowHeads, postings, owner, stamps,
                   dependencies, captured, answers, answered, willWrite, committed, rejected>>

\* The global fetch_add and bucket stores are separate actions. Other
\* transactions may reserve IDs between them while publication guards stay held.
ReserveStamp(t) ==
    /\ pc[t] = "Stamp"
    /\ publicationStamp' = [publicationStamp EXCEPT ![t] = IF ChangedBuckets(t) = {} THEN 0
        ELSE IF Mutation = "BeginStamp" THEN txid[t] ELSE nextId]
    /\ nextId' = nextId + IF ChangedBuckets(t) = {} THEN 0 ELSE 1
    /\ pc' = [pc EXCEPT ![t] = "StoreStamp"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, lifecycle, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded,
        txid, active, snapRows, logicalRows, rowHeads, postings, owner, stamps,
        dependencies, captured, answers, answered, willWrite, committed, rejected>>
StoreStamp(t) ==
    /\ pc[t] = "StoreStamp"
    /\ stamps' = [b \in Buckets |-> IF b \in ChangedBuckets(t) THEN publicationStamp[t] ELSE stamps[b]]
    /\ pc' = [pc EXCEPT ![t] = "Unlock"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, lifecycle, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp,
        nextId, txid, active, snapRows, logicalRows, rowHeads, postings, owner,
        dependencies, captured, answers, answered, willWrite, committed, rejected>>

Unlock(t) ==
    /\ pc[t] = "Unlock"
    /\ owner' = [b \in Buckets |-> IF owner[b] = t THEN 0 ELSE owner[b]]
    /\ committed' = committed \cup {t}
    /\ pc' = [pc EXCEPT ![t] = "Done"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, lifecycle, ended, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, nextId, txid, active, snapRows, logicalRows, rowHeads, postings,
                   stamps, dependencies, captured, answers, answered, willWrite, rejected>>

Reject(t) ==
    /\ pc[t] = "Reject" /\ LifecycleFree
    /\ active' = active \ {t}
    /\ owner' = [b \in Buckets |-> IF owner[b] = t THEN 0 ELSE owner[b]]
    /\ rejected' = rejected \cup {t}
    /\ ended' = ended \cup {t}
    /\ pc' = [pc EXCEPT ![t] = "Done"]
    /\ UNCHANGED <<beganBetweenReserveAndStore, lifecycle, snapped, snapshotXmax, snapshotActive, snapshotEnded, publicationStamp, nextId, txid, snapRows, logicalRows, rowHeads, postings,
                   stamps, dependencies, captured, answers, answered, willWrite, committed>>

Terminal == \A t \in Tx: pc[t] = "Done"
Next == (\E t \in Tx: StartLock(t) \/ Reserve(t) \/ Register(t) \/ BeginUnlock(t) \/ SnapshotLock(t) \/ Snapshot(t) \/ LookupLock(t) \/ Capture(t) \/
         Materialize(t) \/ AcquireOne(t) \/ Acquired(t) \/ Validate(t) \/ Prepare(t) \/
         RemoveSource(t) \/ PublishRow(t) \/ PreStamp(t) \/ EndLock(t) \/ Deregister(t) \/ ReserveStamp(t) \/ StoreStamp(t) \/ Unlock(t) \/ Reject(t))
        \/ (Terminal /\ UNCHANGED vars)

TypeOK == /\ active \subseteq Tx /\ committed \subseteq Tx /\ rejected \subseteq Tx
          /\ owner \in [Buckets -> (Tx \cup {0})]
          /\ logicalRows \in [Rows -> 0..2] /\ rowHeads \in [Rows -> 0..2]
          /\ postings \subseteq (1..2) \X Rows
          /\ nextId \in 1..5 /\ lifecycle \in (Tx \cup {0})
          /\ ended \subseteq Tx /\ snapped \subseteq Tx
LookupComplete == \A t \in answered: answers[t].rows = {r \in Rows : snapRows[t][r] = QueryKey(t)}
UniqueCreation == Scenario # "Empty" \/ Cardinality({r \in Rows : logicalRows[r] = 1}) <= 1
FinalIndexMatches == ~Terminal \/ postings = {<<logicalRows[r], r>> : r \in {x \in Rows : logicalRows[x] # 0}}
\* A reserved, unfinished transaction below xmax must be in the captured
\* in-flight IDs; otherwise an MVCC version could be mistaken for committed.
RegistrationSafety == \A reader \in snapped: \A writer \in Tx:
    (txid[writer] > 0 /\ txid[writer] < snapshotXmax[reader] /\ writer \notin snapshotEnded[reader])
    => txid[writer] \in snapshotActive[reader]
LatePublication == \A reader \in snapped: \A writer \in Tx:
    (reader # writer /\ txid[writer] \in snapshotActive[reader] /\ publicationStamp[writer] > 0)
    => publicationStamp[writer] > txid[reader]
LifecycleSafety == LookupComplete /\ UniqueCreation /\ FinalIndexMatches /\ VisibleRows = logicalRows
    /\ RegistrationSafety /\ LatePublication

Reached == CASE Witness = "both_commit" -> committed = Tx
             [] Witness = "creator_conflict" -> Cardinality(committed) = 1 /\ Cardinality(rejected) = 1
                  /\ answered = Tx /\ answers[1].rows = {} /\ answers[2].rows = {}
             [] Witness = "old_snapshot_retry" -> 2 \in rejected /\ snapRows[2][1] = 1 /\ logicalRows[1] = 2
             [] Witness = "captured_old_read" -> 2 \in answered /\ answers[2].rows = {1} /\ answers[2].afterMove
             [] Witness = "reserved_gap_blocks_snapshot" -> lifecycle = 1 /\ pc[1] = "Register"
                  /\ pc[2] = "SnapshotLock"
             [] Witness = "late_older_writer" -> publicationStamp[1] > txid[2] /\ txid[1] < txid[2]
                  /\ txid[1] \in snapshotActive[2] /\ 2 \in rejected
             [] Witness = "reader_after_deregister_before_stamp" -> 1 \in snapshotEnded[2]
                  /\ snapshotXmax[2] > txid[2] /\ publicationStamp[1] > txid[2] /\ 2 \in rejected
             [] Witness = "reader_between_reservation_and_store" -> 2 \in beganBetweenReserveAndStore
                  /\ publicationStamp[1] < txid[2] /\ 2 \in committed
             [] Witness = "disjoint_store_reordering" -> Scenario = "Disjoint"
                  /\ pc[1] = "StoreStamp" /\ pc[2] \in {"Unlock", "Done"}
                  /\ publicationStamp[1] > 0 /\ publicationStamp[1] < publicationStamp[2]
             [] OTHER -> FALSE
WitnessNotReached == ~Reached
=============================================================================
