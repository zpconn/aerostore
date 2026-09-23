--------------------------- MODULE Publication ---------------------------
EXTENDS Integers, FiniteSets

\* Finite protocol pilot, NOT a Rust/weak-memory/allocator verification.
\* Two one-shot transactions; one indexed value per row; exact MVCC snapshot
\* rows are an abstract primitive. Snapshot copying and registration are
\* indivisible lifecycle operations; candidate materialization is separate.
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
          willWrite, committed, rejected
vars == <<pc, nextId, txid, active, snapRows, logicalRows, rowHeads,
          postings, owner, stamps, dependencies, captured, answers, answered,
          willWrite, committed, rejected>>

\* A published physical head is still uncommitted while its writer remains
\* registered. Keep one old version per row in this two-transaction model.
\* Actual version traversal and safe retention remain primitive assumptions.
VisibleRows == [r \in Rows |->
    LET writers == {t \in active : willWrite[t] /\ WriteRow(t) = r /\ pc[t] = "Deregister"}
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

Start(t) ==
    /\ pc[t] = "Start"
    \* Exercise an older writer that can finish after the reader begins.
    /\ (Scenario # "Move" \/ t = 1 \/ txid[1] > 0)
    /\ txid' = [txid EXCEPT ![t] = nextId]
    /\ nextId' = nextId + 1
    /\ active' = active \cup {t}
    /\ pc' = [pc EXCEPT ![t] = "Snapshot"]
    /\ UNCHANGED <<snapRows, logicalRows, rowHeads, postings, owner, stamps,
                   dependencies, captured, answers, answered, willWrite, committed, rejected>>

Snapshot(t) ==
    /\ pc[t] = "Snapshot"
    /\ snapRows' = [snapRows EXCEPT ![t] = VisibleRows]
    /\ willWrite' = [willWrite EXCEPT ![t] = (Scenario = "Move" /\ t = 1)]
    /\ pc' = [pc EXCEPT ![t] = IF Scenario = "Move" /\ t = 1 THEN "Acquire" ELSE "LookupLock"]
    /\ UNCHANGED <<nextId, txid, active, logicalRows, rowHeads, postings, owner,
                   stamps, dependencies, captured, answers, answered, committed, rejected>>

LookupLock(t) ==
    /\ pc[t] = "LookupLock" /\ owner[Bucket(QueryKey(t))] = 0
    /\ owner' = [owner EXCEPT ![Bucket(QueryKey(t))] = t]
    /\ pc' = [pc EXCEPT ![t] = "Capture"]
    /\ UNCHANGED <<nextId, txid, active, snapRows, logicalRows, rowHeads,
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
    /\ UNCHANGED <<nextId, txid, active, snapRows, logicalRows, rowHeads, postings,
                   stamps, answers, answered, willWrite, committed, rejected>>

Materialize(t) ==
    /\ pc[t] = "Materialize"
    /\ LET result == {r \in captured[t] : snapRows[t][r] = QueryKey(t)}
       IN /\ answers' = [answers EXCEPT ![t] = [rows |-> result,
                        afterMove |-> (Scenario = "Move" /\ logicalRows[1] = 2)]]
          /\ willWrite' = [willWrite EXCEPT ![t] = (Scenario # "Move" /\ result = {})]
    /\ answered' = answered \cup {t}
    /\ pc' = [pc EXCEPT ![t] = "Acquire"]
    /\ UNCHANGED <<nextId, txid, active, snapRows, logicalRows, rowHeads, postings,
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
    /\ UNCHANGED <<pc, nextId, txid, active, snapRows, logicalRows, rowHeads,
                   postings, stamps, dependencies, captured, answers, answered,
                   willWrite, committed, rejected>>

Acquired(t) ==
    /\ pc[t] = "Acquire" /\ Missing(t) = {}
    /\ pc' = [pc EXCEPT ![t] = "Validate"]
    /\ UNCHANGED <<nextId, txid, active, snapRows, logicalRows, rowHeads,
                   postings, owner, stamps, dependencies, captured, answers,
                   answered, willWrite, committed, rejected>>

Validate(t) ==
    /\ pc[t] = "Validate"
    /\ LET predicateOK == Mutation = "SkipValidation" \/
               (\A b \in Buckets: dependencies[t][b] < 0 \/
                    (stamps[b] = dependencies[t][b] /\ stamps[b] < txid[t]))
           baseOK == ~willWrite[t] \/ logicalRows[WriteRow(t)] = snapRows[t][WriteRow(t)]
       IN pc' = [pc EXCEPT ![t] = IF predicateOK /\ baseOK THEN "Prepare" ELSE "Reject"]
    /\ UNCHANGED <<nextId, txid, active, snapRows, logicalRows, rowHeads,
                   postings, owner, stamps, dependencies, captured, answers,
                   answered, willWrite, committed, rejected>>

Prepare(t) ==
    /\ pc[t] = "Prepare"
    /\ postings' = IF willWrite[t] THEN postings \cup {<<WriteKey(t), WriteRow(t)>>} ELSE postings
    /\ pc' = [pc EXCEPT ![t] = "RemoveSource"]
    /\ UNCHANGED <<nextId, txid, active, snapRows, logicalRows, rowHeads,
                   owner, stamps, dependencies, captured, answers, answered,
                   willWrite, committed, rejected>>

RemoveSource(t) ==
    /\ pc[t] = "RemoveSource"
    /\ postings' = IF willWrite[t] THEN postings \ {<<snapRows[t][WriteRow(t)], WriteRow(t)>>} ELSE postings
    /\ pc' = [pc EXCEPT ![t] = "PublishRow"]
    /\ UNCHANGED <<nextId, txid, active, snapRows, logicalRows, rowHeads,
                   owner, stamps, dependencies, captured, answers, answered,
                   willWrite, committed, rejected>>

PublishRow(t) ==
    /\ pc[t] = "PublishRow"
    /\ rowHeads' = IF willWrite[t] THEN [rowHeads EXCEPT ![WriteRow(t)] = WriteKey(t)] ELSE rowHeads
    /\ owner' = IF Mutation = "EarlyUnlock" THEN [b \in Buckets |-> IF owner[b] = t THEN 0 ELSE owner[b]] ELSE owner
    /\ pc' = [pc EXCEPT ![t] = "Deregister"]
    /\ UNCHANGED <<nextId, txid, active, snapRows, logicalRows, postings,
                   stamps, dependencies, captured, answers, answered,
                   willWrite, committed, rejected>>

Deregister(t) ==
    /\ pc[t] = "Deregister"
    /\ logicalRows' = IF willWrite[t] THEN [logicalRows EXCEPT ![WriteRow(t)] = WriteKey(t)] ELSE logicalRows
    /\ active' = active \ {t}
    /\ pc' = [pc EXCEPT ![t] = "Stamp"]
    /\ UNCHANGED <<nextId, txid, snapRows, rowHeads, postings, owner, stamps,
                   dependencies, captured, answers, answered, willWrite, committed, rejected>>

Stamp(t) ==
    /\ pc[t] = "Stamp"
    /\ stamps' = [b \in Buckets |-> IF b \in ChangedBuckets(t)
                  THEN (IF Mutation = "BeginStamp" THEN txid[t] ELSE nextId) ELSE stamps[b]]
    /\ nextId' = nextId + IF ChangedBuckets(t) = {} THEN 0 ELSE 1
    /\ pc' = [pc EXCEPT ![t] = "Unlock"]
    /\ UNCHANGED <<txid, active, snapRows, logicalRows, rowHeads, postings, owner,
                   dependencies, captured, answers, answered, willWrite, committed, rejected>>

Unlock(t) ==
    /\ pc[t] = "Unlock"
    /\ owner' = [b \in Buckets |-> IF owner[b] = t THEN 0 ELSE owner[b]]
    /\ committed' = committed \cup {t}
    /\ pc' = [pc EXCEPT ![t] = "Done"]
    /\ UNCHANGED <<nextId, txid, active, snapRows, logicalRows, rowHeads, postings,
                   stamps, dependencies, captured, answers, answered, willWrite, rejected>>

Reject(t) ==
    /\ pc[t] = "Reject"
    /\ active' = active \ {t}
    /\ owner' = [b \in Buckets |-> IF owner[b] = t THEN 0 ELSE owner[b]]
    /\ rejected' = rejected \cup {t}
    /\ pc' = [pc EXCEPT ![t] = "Done"]
    /\ UNCHANGED <<nextId, txid, snapRows, logicalRows, rowHeads, postings,
                   stamps, dependencies, captured, answers, answered, willWrite, committed>>

Terminal == \A t \in Tx: pc[t] = "Done"
Next == (\E t \in Tx: Start(t) \/ Snapshot(t) \/ LookupLock(t) \/ Capture(t) \/
         Materialize(t) \/ AcquireOne(t) \/ Acquired(t) \/ Validate(t) \/ Prepare(t) \/
         RemoveSource(t) \/ PublishRow(t) \/ Deregister(t) \/ Stamp(t) \/ Unlock(t) \/ Reject(t))
        \/ (Terminal /\ UNCHANGED vars)

TypeOK == /\ active \subseteq Tx /\ committed \subseteq Tx /\ rejected \subseteq Tx
          /\ owner \in [Buckets -> (Tx \cup {0})]
          /\ logicalRows \in [Rows -> 0..2] /\ rowHeads \in [Rows -> 0..2]
          /\ postings \subseteq (1..2) \X Rows
          /\ nextId \in 1..5
LookupComplete == \A t \in answered: answers[t].rows = {r \in Rows : snapRows[t][r] = QueryKey(t)}
UniqueCreation == Scenario # "Empty" \/ Cardinality({r \in Rows : logicalRows[r] = 1}) <= 1
FinalIndexMatches == ~Terminal \/ postings = {<<logicalRows[r], r>> : r \in {x \in Rows : logicalRows[x] # 0}}
Safety == LookupComplete /\ UniqueCreation /\ FinalIndexMatches /\ VisibleRows = logicalRows

Reached == CASE Witness = "both_commit" -> committed = Tx
             [] Witness = "creator_conflict" -> Cardinality(committed) = 1 /\ Cardinality(rejected) = 1
                  /\ answered = Tx /\ answers[1].rows = {} /\ answers[2].rows = {}
             [] Witness = "old_snapshot_retry" -> 2 \in rejected /\ snapRows[2][1] = 1 /\ logicalRows[1] = 2
             [] Witness = "captured_old_read" -> 2 \in answered /\ answers[2].rows = {1} /\ answers[2].afterMove
             [] OTHER -> FALSE
WitnessNotReached == ~Reached
=============================================================================
