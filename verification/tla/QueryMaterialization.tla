-------------------------- MODULE QueryMaterialization --------------------------
EXTENDS Integers, FiniteSets

\* One writer and one indexed reader, two stable row IDs, one retained old row.
\* Snapshot metadata is read only after actual lifecycle acquisition; waiting
\* does not freeze the caller's API-entry view. Posting operations and row
\* creator/deleter publication are separate actions under affected bucket guards.
\* Guards are released after raw candidates are copied, before MVCC traversal.
\* Native atomic/heap/retention correspondence and arbitrary chains are not proved.
CONSTANTS Scenario, Query, Own, BucketCount, ExtraPosting, Mutation, Witness
Rows == {1, 2}
Keys == {1, 2}
Buckets == 0..(BucketCount - 1)
Bucket(k) == k % BucketCount
Matches(k) == k # 0 /\ (Query = "All" \/ (Query = "Old" /\ k = 1) \/ (Query = "New" /\ k = 2))
OldKey == IF Scenario = "Create" THEN 0 ELSE 1
NewKey == IF Scenario = "Delete" THEN 0 ELSE 2
SecondKey == IF Own = "Insert" THEN 0 ELSE 2
InitialRows == [r \in Rows |-> IF r = 1 THEN OldKey ELSE SecondKey]
QueryBuckets == {Bucket(k) : k \in {x \in Keys : Matches(x)}}
ChangedBuckets == {Bucket(k) : k \in ({OldKey, NewKey} \ {0})}
OwnRows == IF Own = "None" THEN {} ELSE IF Own \in {"Insert", "MoveIn"} THEN {2} ELSE {1}
OwnKey == IF Own \in {"Insert", "MoveIn"} THEN 1 ELSE IF Own = "MoveOut" THEN 2 ELSE 0
MaxSet(values) == CHOOSE m \in values: \A n \in values: n <= m
MinSet(values) == CHOOSE m \in values: \A n \in values: m <= n

VARIABLE s
vars == <<s>>
Init == s = [
    wpc |-> "Begin", rpc |-> "Begin", clock |-> 1, wid |-> 0, rid |-> 0,
    active |-> {}, lifecycle |-> 0, owners |-> [b \in Buckets |-> 0],
    postings |-> {<<InitialRows[r], r>> : r \in {x \in Rows : InitialRows[x] # 0}}
        \cup (IF ExtraPosting THEN {<<1, 2>>} ELSE {}),
    stamps |-> [b \in Buckets |-> 0], pubStamp |-> 0, stored |-> FALSE,
    oldXmax |-> 0, newHead |-> FALSE, retained |-> TRUE, committedKey |-> OldKey,
    snapMin |-> 0, snapMax |-> 0, snapActive |-> {}, snapshotTaken |-> FALSE,
    expectedActive |-> {}, expectedKey |-> OldKey, cachedActive |-> {},
    requested |-> FALSE, waitedMetadata |-> FALSE, waitedBuckets |-> FALSE,
    beganAfterReservation |-> FALSE, raw |-> {}, captured |-> FALSE,
    dependency |-> [b \in Buckets |-> -1], captureEpoch |-> 0,
    answer |-> {}, answered |-> FALSE, rejected |-> FALSE, committed |-> FALSE,
    commitEpoch |-> 0, materializedAfterWriter |-> FALSE]

WriterBeginLock == /\ s.wpc = "Begin" /\ s.lifecycle = 0
    /\ s' = [s EXCEPT !.lifecycle = 1, !.wpc = "Register"]
WriterRegister == /\ s.wpc = "Register" /\ s.lifecycle = 1
    /\ s' = [s EXCEPT !.wid = s.clock, !.clock = @ + 1,
        !.active = @ \cup {s.clock}, !.lifecycle = 0, !.wpc = "Acquire"]
WriterAcquire == /\ s.wpc = "Acquire" /\ \A b \in ChangedBuckets: s.owners[b] = 0
    /\ s' = [s EXCEPT !.owners = [b \in Buckets |-> IF b \in ChangedBuckets THEN 1 ELSE s.owners[b]],
        !.wpc = "Prepare"]
WriterPrepare == /\ s.wpc = "Prepare"
    /\ s' = [s EXCEPT !.postings = IF NewKey = 0 THEN @ ELSE @ \cup {<<NewKey, 1>>},
        !.wpc = "Remove"]
WriterRemove == /\ s.wpc = "Remove"
    /\ s' = [s EXCEPT !.postings = @ \ {<<OldKey, 1>>}, !.wpc = "SetXmax",
        !.owners = IF Mutation = "EarlyUnlock" THEN [b \in Buckets |-> IF s.owners[b] = 1 THEN 0 ELSE s.owners[b]] ELSE @]
WriterSetXmax == /\ s.wpc = "SetXmax"
    /\ s' = [s EXCEPT !.oldXmax = s.wid, !.wpc = "SetHead"]
WriterSetHead == /\ s.wpc = "SetHead"
    /\ s' = [s EXCEPT !.newHead = TRUE, !.wpc = "EndLock",
        !.retained = IF Mutation = "DropRetainedVersion" THEN FALSE ELSE @]
WriterEndLock == /\ s.wpc = "EndLock" /\ s.lifecycle = 0
    /\ s' = [s EXCEPT !.lifecycle = 1, !.wpc = "End"]
WriterEnd == /\ s.wpc = "End" /\ s.lifecycle = 1
    /\ s' = [s EXCEPT !.active = @ \ {s.wid}, !.committedKey = NewKey,
        !.lifecycle = 0, !.wpc = "ReserveStamp"]
WriterReserveStamp == /\ s.wpc = "ReserveStamp"
    /\ s' = [s EXCEPT !.pubStamp = IF Mutation = "BeginStamp" THEN s.wid ELSE s.clock,
        !.clock = @ + 1, !.wpc = "StoreStamp"]
WriterStoreStamp == /\ s.wpc = "StoreStamp"
    /\ LET touched == IF Mutation = "OmitOldStamp" THEN ChangedBuckets \ {Bucket(OldKey)} ELSE ChangedBuckets
       IN s' = [s EXCEPT !.stamps = [b \in Buckets |-> IF b \in touched THEN s.pubStamp ELSE s.stamps[b]],
            !.stored = TRUE, !.wpc = "Release"]
WriterRelease == /\ s.wpc = "Release"
    /\ s' = [s EXCEPT !.owners = [b \in Buckets |-> IF s.owners[b] = 1 THEN 0 ELSE s.owners[b]], !.wpc = "Done"]

ReaderBeginLock == /\ s.rpc = "Begin" /\ s.lifecycle = 0
    /\ s' = [s EXCEPT !.lifecycle = 2, !.rpc = "Register"]
ReaderRegister == /\ s.rpc = "Register" /\ s.lifecycle = 2
    /\ s' = [s EXCEPT !.rid = s.clock, !.clock = @ + 1,
        !.active = @ \cup {s.clock}, !.lifecycle = 0, !.rpc = "RequestSnapshot",
        !.beganAfterReservation = s.wpc = "StoreStamp"]
ReaderRequestSnapshot == /\ s.rpc = "RequestSnapshot"
    /\ s' = [s EXCEPT !.rpc = "SnapshotWait", !.requested = TRUE,
        !.cachedActive = s.active, !.waitedMetadata = s.lifecycle = 1]
ReaderSnapshotLock == /\ s.rpc = "SnapshotWait" /\ s.lifecycle = 0
    /\ s' = [s EXCEPT !.lifecycle = 2, !.rpc = "Snapshot"]
ReaderSnapshot == /\ s.rpc = "Snapshot" /\ s.lifecycle = 2
    /\ \E sampled \in 1..s.clock:
        s' = [s EXCEPT !.snapMin = MinSet(s.active \cup {sampled}),
            !.snapMax = MaxSet({id + 1 : id \in s.active} \cup {sampled}),
            !.snapActive = IF Mutation = "CachedSnapshotBeforeWait" THEN s.cachedActive ELSE s.active,
            !.expectedActive = s.active, !.expectedKey = s.committedKey,
            !.snapshotTaken = TRUE, !.lifecycle = 0, !.rpc = "LookupWait"]
ReaderObserveBucketWait == /\ s.rpc = "LookupWait" /\ ~s.waitedBuckets
    /\ \E b \in QueryBuckets: s.owners[b] = 1
    /\ s' = [s EXCEPT !.waitedBuckets = TRUE]
ReaderLookupLock == /\ s.rpc = "LookupWait" /\ \A b \in QueryBuckets: s.owners[b] = 0
    /\ s' = [s EXCEPT !.owners = [b \in Buckets |-> IF b \in QueryBuckets THEN 2 ELSE s.owners[b]], !.rpc = "Capture"]
ReaderCapture == /\ s.rpc = "Capture"
    /\ LET valid == Mutation = "SkipCaptureCheck" \/ \A b \in QueryBuckets: s.stamps[b] < s.rid
       IN s' = [s EXCEPT !.raw = {row \in Rows: \E key \in Keys: Matches(key) /\ <<key, row>> \in s.postings},
            !.dependency = [b \in Buckets |-> IF b \in QueryBuckets THEN s.stamps[b] ELSE -1],
            !.captured = valid, !.captureEpoch = IF s.stored THEN 1 ELSE 0,
            !.owners = [b \in Buckets |-> IF s.owners[b] = 2 THEN 0 ELSE s.owners[b]],
            !.rpc = IF valid THEN "Materialize" ELSE "Reject"]

CreatorVisible(creator) == creator = s.rid \/ creator < s.snapMin \/
    ((Mutation = "IgnoreCreatorXmax" \/ creator < s.snapMax) /\
     (Mutation = "IgnoreCreatorActive" \/ creator \notin s.snapActive))
VersionVisible(creator, deleter) == creator = s.rid \/
    (CreatorVisible(creator) /\ (deleter = 0 \/ (deleter # s.rid /\
        (deleter >= s.snapMax \/ deleter \in s.snapActive))))
SnapshotRow(row) == IF row = 2 THEN SecondKey ELSE
    IF s.newHead /\ VersionVisible(s.wid, 0) THEN NewKey
    ELSE IF s.retained /\ VersionVisible(0, s.oldXmax) THEN OldKey ELSE 0
MaterialRow(row) == IF row \in OwnRows THEN OwnKey ELSE SnapshotRow(row)
ExpectedRow(row) == IF row \in OwnRows THEN OwnKey ELSE IF row = 1 THEN s.expectedKey ELSE SecondKey
ReaderMaterialize == /\ s.rpc = "Materialize"
    /\ LET candidates == s.raw \cup (IF Mutation = "OmitOwnCandidates" THEN {} ELSE OwnRows)
           answer == {row \in candidates: MaterialRow(row) # 0 /\ (Mutation = "SkipKeyFilter" \/ Matches(MaterialRow(row)))}
       IN s' = [s EXCEPT !.answer = answer, !.answered = TRUE, !.rpc = "CommitWait",
            !.materializedAfterWriter = s.wpc = "Done"]
ReaderCommitLock == /\ s.rpc = "CommitWait" /\ \A b \in QueryBuckets: s.owners[b] = 0
    /\ s' = [s EXCEPT !.owners = [b \in Buckets |-> IF b \in QueryBuckets THEN 2 ELSE s.owners[b]], !.rpc = "Validate"]
ReaderValidate == /\ s.rpc = "Validate"
    /\ LET valid == Mutation = "SkipCommitValidation" \/ \A b \in QueryBuckets:
            s.stamps[b] = s.dependency[b] /\ s.stamps[b] < s.rid
       IN s' = [s EXCEPT !.committed = valid, !.commitEpoch = IF s.stored THEN 1 ELSE 0,
            !.rpc = IF valid THEN "Done" ELSE "Reject",
            !.owners = [b \in Buckets |-> IF s.owners[b] = 2 THEN 0 ELSE s.owners[b]]]
ReaderReject == /\ s.rpc = "Reject"
    /\ s' = [s EXCEPT !.rejected = TRUE, !.rpc = "Done"]

Terminal == s.wpc = "Done" /\ s.rpc = "Done"
Next == WriterBeginLock \/ WriterRegister \/ WriterAcquire \/ WriterPrepare \/ WriterRemove \/
    WriterSetXmax \/ WriterSetHead \/ WriterEndLock \/ WriterEnd \/ WriterReserveStamp \/ WriterStoreStamp \/ WriterRelease \/
    ReaderBeginLock \/ ReaderRegister \/ ReaderRequestSnapshot \/ ReaderSnapshotLock \/ ReaderSnapshot \/
    ReaderObserveBucketWait \/ ReaderLookupLock \/ ReaderCapture \/ ReaderMaterialize \/ ReaderCommitLock \/ ReaderValidate \/ ReaderReject \/
    (Terminal /\ UNCHANGED vars)

TypeOK == s.clock \in 1..4 /\ s.lifecycle \in 0..2 /\ s.owners \in [Buckets -> 0..2]
    /\ s.answer \subseteq Rows /\ s.active \subseteq 1..3
SnapshotAcquired == ~s.snapshotTaken \/ s.snapActive = s.expectedActive
LookupComplete == ~s.answered \/ s.answer = {row \in Rows: Matches(ExpectedRow(row))}
CommitStable == ~s.committed \/ ChangedBuckets \cap QueryBuckets = {} \/ s.commitEpoch = s.captureEpoch
Safety == TypeOK /\ SnapshotAcquired /\ LookupComplete /\ CommitStable
Reached == CASE Witness = "old_row_after_release" -> s.answered /\ s.materializedAfterWriter
        /\ s.expectedKey = OldKey /\ 1 \in s.answer /\ OldKey # NewKey /\ Own = "None"
    [] Witness = "writer_ends_while_waiting" -> s.waitedBuckets /\ s.rejected /\ s.wpc = "Done"
    [] Witness = "acquisition_refresh" -> s.waitedMetadata /\ s.snapshotTaken /\ s.wid \in s.cachedActive
        /\ s.wid \notin s.snapActive /\ s.expectedKey = NewKey
    [] Witness = "reader_after_reservation" -> s.beganAfterReservation /\ s.committed /\ s.pubStamp < s.rid
    [] Witness = "private_insert" -> Own = "Insert" /\ s.answered /\ 2 \in s.answer /\ 2 \notin s.raw
    [] Witness = "stale_filtered" -> ExtraPosting /\ s.answered /\ 2 \in s.raw /\ 2 \notin s.answer
    [] Witness = "empty_success" -> s.committed /\ s.answer = {}
    [] Witness = "capture_then_commit_retry" -> s.answered /\ s.rejected /\ s.stored /\ s.captureEpoch = 0
    [] OTHER -> FALSE
WitnessNotReached == ~Reached
=============================================================================
