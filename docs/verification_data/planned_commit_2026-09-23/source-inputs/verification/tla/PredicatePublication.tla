---------------------- MODULE PredicatePublication ----------------------
EXTENDS Integers, FiniteSets

\* Data-bearing finite slice with two repeated reader lookups and a one-shot
\* older writer. The reader may stage a local insert/move/delete. Raw candidate
\* lookup, pinned snapshot rows, and atomic registration are abstract primitives.
CONSTANTS Scenario, Mutation, BucketCount, Witness
Rows == {1, 2}
Buckets == 0..(BucketCount - 1)
Bucket(k) == k % BucketCount
QueryKey == 1
OldKey == IF Scenario = "Empty" THEN 0 ELSE 1
NewKey == IF Scenario = "Empty" THEN 1 ELSE 2
OwnEnabled == Scenario \in {"OwnInsert", "OwnMoveIn", "OwnMoveOut", "OwnDelete"}
OwnInitial == CASE Scenario = "OwnMoveIn" -> 2
                  [] Scenario \in {"OwnMoveOut", "OwnDelete"} -> 1
                  [] OTHER -> 0
OwnValue == CASE Scenario \in {"OwnInsert", "OwnMoveIn"} -> 1
                [] Scenario = "OwnMoveOut" -> 2
                [] OTHER -> 0
InitialRows == [r \in Rows |-> IF r = 1 THEN OldKey ELSE OwnInitial]
ChangedBuckets == {Bucket(k) : k \in ({OldKey, NewKey} \ {0})}
WriterBuckets == IF Mutation = "OmitOldBucket" THEN {Bucket(NewKey)} ELSE ChangedBuckets

VARIABLES readerPC, writerPC, nextId, readerId, registered, snapshot,
          logicalRows, rowHeads, postings, owner, stamps, dependency, captured,
          answers, answerCount, commitRows, committed, rejected, captureAfterMove,
          repeatedAfterMove, sawOwn, readerStartedDuringPublication
vars == <<readerPC, writerPC, nextId, readerId, registered, snapshot,
          logicalRows, rowHeads, postings, owner, stamps, dependency, captured,
          answers, answerCount, commitRows, committed, rejected, captureAfterMove,
          repeatedAfterMove, sawOwn, readerStartedDuringPublication>>

VisibleRows == IF registered /\ writerPC \in {"Deregister", "Stamp", "Unlock", "Done"}
               THEN [rowHeads EXCEPT ![1] = OldKey] ELSE rowHeads
ReadView == [r \in Rows |-> IF OwnEnabled /\ r = 2 THEN OwnValue ELSE snapshot[r]]
ExpectedAnswer == {r \in Rows : ReadView[r] = QueryKey}
Init ==
    /\ readerPC = "Start" /\ writerPC = "Acquire"
    /\ nextId = 2 /\ readerId = 0 /\ registered = TRUE
    /\ snapshot = InitialRows /\ logicalRows = InitialRows /\ rowHeads = InitialRows
    /\ postings = {<<InitialRows[r], r>> : r \in {x \in Rows : InitialRows[x] # 0}}
    /\ owner = [b \in Buckets |-> 0] /\ stamps = [b \in Buckets |-> 0]
    /\ dependency = -1 /\ captured = {} /\ answers = {} /\ answerCount = 0
    /\ commitRows = InitialRows /\ committed = FALSE /\ rejected = FALSE
    /\ captureAfterMove = FALSE /\ repeatedAfterMove = FALSE /\ sawOwn = FALSE
    /\ readerStartedDuringPublication = FALSE

ReaderStart ==
    /\ readerPC = "Start"
    /\ readerId' = nextId /\ nextId' = nextId + 1
    /\ snapshot' = VisibleRows
    /\ readerStartedDuringPublication' = (writerPC = "Stamp")
    /\ readerPC' = "LookupLock"
    /\ UNCHANGED <<writerPC, registered, logicalRows, rowHeads, postings, owner,
                   stamps, dependency, captured, answers, answerCount, commitRows,
                   committed, rejected, captureAfterMove, repeatedAfterMove, sawOwn>>
ReaderLock ==
    /\ readerPC = "LookupLock" /\ owner[Bucket(QueryKey)] = 0
    /\ owner' = [owner EXCEPT ![Bucket(QueryKey)] = 2]
    /\ readerPC' = "Capture"
    /\ UNCHANGED <<writerPC, nextId, readerId, registered, snapshot, logicalRows,
                   rowHeads, postings, stamps, dependency, captured, answers,
                   answerCount, commitRows, committed, rejected, captureAfterMove,
                   repeatedAfterMove, sawOwn, readerStartedDuringPublication>>
ReaderCapture ==
    /\ readerPC = "Capture"
    /\ LET stamp == stamps[Bucket(QueryKey)]
           candidates == {r \in Rows : <<QueryKey, r>> \in postings}
           valid == (stamp < readerId /\ (dependency = -1 \/ dependency = stamp))
                    \/ (Mutation = "SkipRepeatValidation" /\ answerCount = 1)
       IN /\ readerPC' = IF valid THEN "Materialize" ELSE "Reject"
          /\ dependency' = IF valid /\ ~(Mutation = "OmitEmpty" /\ candidates = {})
                            THEN stamp ELSE dependency
          /\ captured' = candidates
    /\ captureAfterMove' = (logicalRows[1] = NewKey)
    /\ owner' = [owner EXCEPT ![Bucket(QueryKey)] = 0]
    /\ UNCHANGED <<writerPC, nextId, readerId, registered, snapshot, logicalRows,
                   rowHeads, postings, stamps, answers, answerCount, commitRows,
                   committed, rejected, repeatedAfterMove, sawOwn,
                   readerStartedDuringPublication>>
ReaderMaterialize ==
    /\ readerPC = "Materialize"
    /\ LET candidates == captured \cup (IF OwnEnabled /\ Mutation # "OmitOwnCandidates" THEN {2} ELSE {})
           result == {r \in candidates : ReadView[r] = QueryKey}
       IN /\ answers' = result
          /\ sawOwn' = (sawOwn \/ (OwnEnabled /\ OwnValue = QueryKey /\ 2 \in result))
    /\ repeatedAfterMove' = (repeatedAfterMove \/ (answerCount = 1 /\ captureAfterMove))
    /\ answerCount' = answerCount + 1
    /\ readerPC' = IF answerCount = 0 THEN "LookupLock" ELSE "CommitLock"
    /\ UNCHANGED <<writerPC, nextId, readerId, registered, snapshot, logicalRows,
                   rowHeads, postings, owner, stamps, dependency, captured,
                   commitRows, committed, rejected, captureAfterMove,
                   readerStartedDuringPublication>>
ReaderCommitLock ==
    /\ readerPC = "CommitLock"
    /\ dependency = -1 \/ owner[Bucket(QueryKey)] = 0
    /\ owner' = IF dependency = -1 THEN owner ELSE [owner EXCEPT ![Bucket(QueryKey)] = 2]
    /\ readerPC' = "Validate"
    /\ UNCHANGED <<writerPC, nextId, readerId, registered, snapshot, logicalRows,
                   rowHeads, postings, stamps, dependency, captured, answers,
                   answerCount, commitRows, committed, rejected, captureAfterMove,
                   repeatedAfterMove, sawOwn, readerStartedDuringPublication>>
ReaderValidate ==
    /\ readerPC = "Validate"
    /\ readerPC' = IF dependency = -1 \/
        (dependency = stamps[Bucket(QueryKey)] /\ stamps[Bucket(QueryKey)] < readerId)
        THEN "Commit" ELSE "Reject"
    /\ owner' = IF Mutation = "UnlockAfterValidation"
                THEN [b \in Buckets |-> IF owner[b] = 2 THEN 0 ELSE owner[b]] ELSE owner
    /\ UNCHANGED <<writerPC, nextId, readerId, registered, snapshot, logicalRows,
                   rowHeads, postings, stamps, dependency, captured, answers,
                   answerCount, commitRows, committed, rejected, captureAfterMove,
                   repeatedAfterMove, sawOwn, readerStartedDuringPublication>>
ReaderFinish ==
    /\ readerPC \in {"Commit", "Reject"}
    /\ committed' = (readerPC = "Commit") /\ rejected' = (readerPC = "Reject")
    /\ commitRows' = logicalRows
    /\ owner' = [b \in Buckets |-> IF owner[b] = 2 THEN 0 ELSE owner[b]]
    /\ readerPC' = "Done"
    /\ UNCHANGED <<writerPC, nextId, readerId, registered, snapshot, logicalRows,
                   rowHeads, postings, stamps, dependency, captured, answers,
                   answerCount, captureAfterMove, repeatedAfterMove, sawOwn,
                   readerStartedDuringPublication>>
WriterAcquire ==
    /\ writerPC = "Acquire"
    /\ \A b \in WriterBuckets: owner[b] = 0
    /\ owner' = [b \in Buckets |-> IF b \in WriterBuckets THEN 1 ELSE owner[b]]
    /\ writerPC' = "Prepare"
    /\ UNCHANGED <<readerPC, nextId, readerId, registered, snapshot, logicalRows,
                   rowHeads, postings, stamps, dependency, captured, answers,
                   answerCount, commitRows, committed, rejected, captureAfterMove,
                   repeatedAfterMove, sawOwn, readerStartedDuringPublication>>
WriterPrepare ==
    /\ writerPC = "Prepare"
    /\ postings' = postings \cup {<<NewKey, 1>>}
    /\ writerPC' = "Remove"
    /\ UNCHANGED <<readerPC, nextId, readerId, registered, snapshot, logicalRows,
                   rowHeads, owner, stamps, dependency, captured, answers,
                   answerCount, commitRows, committed, rejected, captureAfterMove,
                   repeatedAfterMove, sawOwn, readerStartedDuringPublication>>
WriterRemove ==
    /\ writerPC = "Remove"
    /\ postings' = postings \ {<<OldKey, 1>>}
    /\ writerPC' = "Publish"
    /\ UNCHANGED <<readerPC, nextId, readerId, registered, snapshot, logicalRows,
                   rowHeads, owner, stamps, dependency, captured, answers,
                   answerCount, commitRows, committed, rejected, captureAfterMove,
                   repeatedAfterMove, sawOwn, readerStartedDuringPublication>>
WriterPublish ==
    /\ writerPC = "Publish"
    /\ rowHeads' = [rowHeads EXCEPT ![1] = NewKey]
    /\ owner' = IF Mutation = "EarlyUnlock"
                THEN [b \in Buckets |-> IF owner[b] = 1 THEN 0 ELSE owner[b]] ELSE owner
    /\ writerPC' = "Deregister"
    /\ UNCHANGED <<readerPC, nextId, readerId, registered, snapshot, logicalRows,
                   postings, stamps, dependency, captured, answers, answerCount,
                   commitRows, committed, rejected, captureAfterMove,
                   repeatedAfterMove, sawOwn, readerStartedDuringPublication>>
WriterDeregister ==
    /\ writerPC = "Deregister"
    /\ registered' = FALSE /\ logicalRows' = [logicalRows EXCEPT ![1] = NewKey]
    /\ writerPC' = "Stamp"
    /\ UNCHANGED <<readerPC, nextId, readerId, snapshot, rowHeads, postings,
                   owner, stamps, dependency, captured, answers, answerCount,
                   commitRows, committed, rejected, captureAfterMove,
                   repeatedAfterMove, sawOwn, readerStartedDuringPublication>>
WriterStamp ==
    /\ writerPC = "Stamp"
    /\ stamps' = [b \in Buckets |-> IF b \in WriterBuckets
                   THEN (IF Mutation = "BeginStamp" THEN 1 ELSE nextId) ELSE stamps[b]]
    /\ nextId' = nextId + 1 /\ writerPC' = "Unlock"
    /\ UNCHANGED <<readerPC, readerId, registered, snapshot, logicalRows,
                   rowHeads, postings, owner, dependency, captured, answers,
                   answerCount, commitRows, committed, rejected, captureAfterMove,
                   repeatedAfterMove, sawOwn, readerStartedDuringPublication>>
WriterUnlock ==
    /\ writerPC = "Unlock"
    /\ owner' = [b \in Buckets |-> IF owner[b] = 1 THEN 0 ELSE owner[b]]
    /\ writerPC' = "Done"
    /\ UNCHANGED <<readerPC, nextId, readerId, registered, snapshot, logicalRows,
                   rowHeads, postings, stamps, dependency, captured, answers,
                   answerCount, commitRows, committed, rejected, captureAfterMove,
                   repeatedAfterMove, sawOwn, readerStartedDuringPublication>>
Terminal == readerPC = "Done" /\ writerPC = "Done"
Next == ReaderStart \/ ReaderLock \/ ReaderCapture \/ ReaderMaterialize \/ ReaderCommitLock
        \/ ReaderValidate \/ ReaderFinish \/ WriterAcquire \/ WriterPrepare \/ WriterRemove
        \/ WriterPublish \/ WriterDeregister \/ WriterStamp \/ WriterUnlock
        \/ (Terminal /\ UNCHANGED vars)
TypeOK == /\ owner \in [Buckets -> 0..2] /\ stamps \in [Buckets -> 0..4]
          /\ nextId \in 2..4 /\ readerId \in 0..3 /\ answerCount \in 0..2
          /\ postings \subseteq (1..2) \X Rows /\ captured \subseteq Rows
LookupComplete == answerCount = 0 \/ answers = ExpectedAnswer
NoMissedPublication == ~committed \/ ((snapshot[1] = QueryKey) = (commitRows[1] = QueryKey))
FinalPostings == ~Terminal \/ postings = {<<logicalRows[r], r>> : r \in {x \in Rows : logicalRows[x] # 0}}
PredicateSafety == LookupComplete /\ NoMissedPublication /\ FinalPostings /\ VisibleRows = logicalRows
Reached == CASE Witness = "own_insert" -> committed /\ sawOwn
                [] Witness = "repeat_success" -> committed /\ answerCount = 2
                [] Witness = "repeat_retry" -> rejected /\ answerCount = 1 /\ writerPC = "Done"
                [] Witness = "start_between_deregister_and_stamp" -> readerStartedDuringPublication /\ rejected
                [] Witness = "captured_before_move" -> answerCount = 1 /\ answers = ExpectedAnswer
                      /\ snapshot[1] = OldKey /\ logicalRows[1] = NewKey /\ ~captureAfterMove
                [] OTHER -> FALSE
WitnessNotReached == ~Reached
=============================================================================
