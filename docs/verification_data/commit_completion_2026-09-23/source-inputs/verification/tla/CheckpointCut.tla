-------------------------- MODULE CheckpointCut --------------------------
EXTENDS Naturals, FiniteSets

\* Implemented ordering alternative: allow new transaction starts while the
\* checkpoint excludes publication with all row partitions. Capture rows and
\* a coherent (allocation cut, active IDs), persist, truncate, then unlock.
\* This is still a finite abstract model, not a Rust/OS refinement proof.
CONSTANTS Mutation, Witness
Tx == {1, 2}
Partitions == {1, 2}
CheckpointOwner == 3
VARIABLES pc, txid, nextId, active, owners, visible, durableLog, acknowledged,
          checkpointPc, snapshotRows, sampledCut, sampledActive,
          checkpointImage, durableCut, durableActive, beganDuringCheckpoint,
          crashed, recovered
vars == <<pc, txid, nextId, active, owners, visible, durableLog, acknowledged,
          checkpointPc, snapshotRows, sampledCut, sampledActive,
          checkpointImage, durableCut, durableActive, beganDuringCheckpoint,
          crashed, recovered>>

Init == /\ pc = [t \in Tx |-> "Idle"]
        /\ txid = [t \in Tx |-> 0] /\ nextId = 1 /\ active = {}
        /\ owners = [p \in Partitions |-> 0]
        /\ visible = {} /\ durableLog = {} /\ acknowledged = {}
        /\ checkpointPc = "Idle" /\ snapshotRows = {}
        /\ sampledCut = 0 /\ sampledActive = {}
        /\ checkpointImage = {} /\ durableCut = 0 /\ durableActive = {}
        /\ beganDuringCheckpoint = {} /\ crashed = FALSE /\ recovered = {}

\* Lifecycle registration/snapshot is an atomic primitive here. Starts have
\* no checkpoint exclusion: their commit partition acquisition supplies it.
Begin(t) ==
    /\ ~crashed /\ pc[t] = "Idle"
    /\ txid' = [txid EXCEPT ![t] = nextId] /\ nextId' = nextId + 1
    /\ active' = active \cup {t} /\ pc' = [pc EXCEPT ![t] = "Lock"]
    /\ beganDuringCheckpoint' = IF checkpointPc \notin {"Idle", "Done"}
                                      THEN beganDuringCheckpoint \cup {t}
                                      ELSE beganDuringCheckpoint
    /\ UNCHANGED <<owners, visible, durableLog, acknowledged, checkpointPc,
          snapshotRows, sampledCut, sampledActive, checkpointImage, durableCut,
          durableActive, crashed, recovered>>

Lock(t) ==
    /\ ~crashed /\ pc[t] = "Lock" /\ owners[t] = 0
    /\ owners' = [owners EXCEPT ![t] = t] /\ pc' = [pc EXCEPT ![t] = "Log"]
    /\ UNCHANGED <<txid, nextId, active, visible, durableLog, acknowledged,
          checkpointPc, snapshotRows, sampledCut, sampledActive, checkpointImage,
          durableCut, durableActive, beganDuringCheckpoint, crashed, recovered>>
DurableAppend(t) ==
    /\ ~crashed /\ pc[t] = "Log" /\ owners[t] = t
    /\ durableLog' = durableLog \cup {t} /\ pc' = [pc EXCEPT ![t] = "Publish"]
    /\ UNCHANGED <<txid, nextId, active, owners, visible, acknowledged,
          checkpointPc, snapshotRows, sampledCut, sampledActive, checkpointImage,
          durableCut, durableActive, beganDuringCheckpoint, crashed, recovered>>
Publish(t) ==
    /\ ~crashed /\ pc[t] = "Publish" /\ owners[t] = t
    /\ visible' = visible \cup {t} /\ pc' = [pc EXCEPT ![t] = "Deregister"]
    /\ UNCHANGED <<txid, nextId, active, owners, durableLog, acknowledged,
          checkpointPc, snapshotRows, sampledCut, sampledActive, checkpointImage,
          durableCut, durableActive, beganDuringCheckpoint, crashed, recovered>>
Deregister(t) ==
    /\ ~crashed /\ pc[t] = "Deregister" /\ owners[t] = t
    /\ active' = active \ {t} /\ pc' = [pc EXCEPT ![t] = "Unlock"]
    /\ UNCHANGED <<txid, nextId, owners, visible, durableLog, acknowledged,
          checkpointPc, snapshotRows, sampledCut, sampledActive, checkpointImage,
          durableCut, durableActive, beganDuringCheckpoint, crashed, recovered>>
Unlock(t) ==
    /\ ~crashed /\ pc[t] = "Unlock" /\ owners[t] = t
    /\ owners' = [owners EXCEPT ![t] = 0] /\ pc' = [pc EXCEPT ![t] = "Ack"]
    /\ UNCHANGED <<txid, nextId, active, visible, durableLog, acknowledged,
          checkpointPc, snapshotRows, sampledCut, sampledActive, checkpointImage,
          durableCut, durableActive, beganDuringCheckpoint, crashed, recovered>>
Ack(t) ==
    /\ ~crashed /\ pc[t] = "Ack"
    /\ acknowledged' = acknowledged \cup {t} /\ pc' = [pc EXCEPT ![t] = "Done"]
    /\ UNCHANGED <<txid, nextId, active, owners, visible, durableLog,
          checkpointPc, snapshotRows, sampledCut, sampledActive, checkpointImage,
          durableCut, durableActive, beganDuringCheckpoint, crashed, recovered>>

CheckpointStart ==
    /\ ~crashed /\ checkpointPc = "Idle" /\ checkpointPc' = "Lock1"
    /\ UNCHANGED <<pc, txid, nextId, active, owners, visible, durableLog,
          acknowledged, snapshotRows, sampledCut, sampledActive, checkpointImage,
          durableCut, durableActive, beganDuringCheckpoint, crashed, recovered>>
CheckpointLock(p) ==
    /\ ~crashed /\ checkpointPc = (IF p = 1 THEN "Lock1" ELSE "Lock2")
    /\ owners[p] = 0
    /\ owners' = [owners EXCEPT ![p] = CheckpointOwner]
    /\ checkpointPc' = IF p = 1 THEN "Lock2" ELSE "Snapshot"
    /\ UNCHANGED <<pc, txid, nextId, active, visible, durableLog, acknowledged,
          snapshotRows, sampledCut, sampledActive, checkpointImage, durableCut,
          durableActive, beganDuringCheckpoint, crashed, recovered>>
CheckpointSnapshot ==
    /\ ~crashed /\ checkpointPc = "Snapshot"
    /\ snapshotRows' = visible /\ checkpointPc' = "SampleCut"
    /\ owners' = IF Mutation = "ReleaseAfterSnapshot"
                     THEN [p \in Partitions |-> 0] ELSE owners
    /\ UNCHANGED <<pc, txid, nextId, active, durableLog, acknowledged, visible,
          sampledCut, sampledActive, checkpointImage, durableCut, durableActive,
          beganDuringCheckpoint, crashed, recovered>>
SampleCut ==
    /\ ~crashed /\ checkpointPc = "SampleCut"
    /\ sampledCut' = nextId - 1 /\ sampledActive' = {txid[t] : t \in active}
    /\ checkpointPc' = IF Mutation = "TruncateBeforePersist" THEN "Truncate" ELSE "Persist"
    /\ UNCHANGED <<pc, txid, nextId, active, owners, durableLog, acknowledged,
          visible, snapshotRows, checkpointImage, durableCut, durableActive,
          beganDuringCheckpoint, crashed, recovered>>
PersistCheckpoint ==
    /\ ~crashed /\ checkpointPc = "Persist"
    /\ checkpointImage' = snapshotRows /\ durableCut' = sampledCut
    /\ durableActive' = IF Mutation = "OmitActive" THEN {} ELSE sampledActive
    /\ checkpointPc' = IF Mutation = "TruncateBeforePersist" THEN "Release" ELSE "Truncate"
    /\ UNCHANGED <<pc, txid, nextId, active, owners, durableLog, acknowledged,
          visible, snapshotRows, sampledCut, sampledActive,
          beganDuringCheckpoint, crashed, recovered>>
Truncate ==
    /\ ~crashed /\ checkpointPc = "Truncate"
    /\ durableLog' = {}
    /\ checkpointPc' = IF Mutation = "TruncateBeforePersist" THEN "Persist" ELSE "Release"
    /\ UNCHANGED <<pc, txid, nextId, active, owners, acknowledged, visible,
          snapshotRows, sampledCut, sampledActive, checkpointImage, durableCut,
          durableActive, beganDuringCheckpoint, crashed, recovered>>
CheckpointRelease ==
    /\ ~crashed /\ checkpointPc = "Release" /\ checkpointPc' = "Done"
    /\ owners' = [p \in Partitions |-> IF owners[p] = CheckpointOwner THEN 0 ELSE owners[p]]
    /\ UNCHANGED <<pc, txid, nextId, active, visible, durableLog, acknowledged,
          snapshotRows, sampledCut, sampledActive, checkpointImage, durableCut,
          durableActive, beganDuringCheckpoint, crashed, recovered>>
Crash ==
    /\ ~crashed /\ crashed' = TRUE
    /\ recovered' = checkpointImage \cup {t \in durableLog :
                              txid[t] > durableCut \/ txid[t] \in durableActive}
    /\ UNCHANGED <<pc, txid, nextId, active, owners, visible, durableLog,
          acknowledged, checkpointPc, snapshotRows, sampledCut, sampledActive,
          checkpointImage, durableCut, durableActive, beganDuringCheckpoint>>
Next == (\E t \in Tx: Begin(t) \/ Lock(t) \/ DurableAppend(t) \/ Publish(t)
                         \/ Deregister(t) \/ Unlock(t) \/ Ack(t))
        \/ CheckpointStart \/ (\E p \in Partitions: CheckpointLock(p))
        \/ CheckpointSnapshot \/ SampleCut \/ PersistCheckpoint \/ Truncate
        \/ CheckpointRelease \/ Crash \/ (crashed /\ UNCHANGED vars)

TypeOK == /\ pc \in [Tx -> {"Idle", "Lock", "Log", "Publish", "Deregister", "Unlock", "Ack", "Done"}]
          /\ txid \in [Tx -> 0..2] /\ nextId \in 1..3
          /\ owners \in [Partitions -> 0..3] /\ active \subseteq Tx
          /\ visible \subseteq Tx /\ durableLog \subseteq Tx /\ acknowledged \subseteq Tx
          /\ snapshotRows \subseteq Tx /\ checkpointImage \subseteq Tx /\ recovered \subseteq Tx
          /\ sampledCut \in 0..2 /\ durableCut \in 0..2
          /\ sampledActive \subseteq Tx /\ durableActive \subseteq Tx
          /\ beganDuringCheckpoint \subseteq Tx /\ crashed \in BOOLEAN
          /\ checkpointPc \in {"Idle", "Lock1", "Lock2", "Snapshot", "SampleCut", "Persist", "Truncate", "Release", "Done"}
CheckpointSafety == ~crashed \/ acknowledged \subseteq recovered
Reached == CASE Witness = "old_start_late_commit" ->
                  crashed /\ checkpointPc = "Done" /\
                  (\E t \in acknowledged: txid[t] <= durableCut /\ txid[t] \in durableActive
                        /\ t \notin checkpointImage /\ t \in recovered)
             [] Witness = "start_during_checkpoint" ->
                  crashed /\ checkpointPc = "Done" /\
                  (\E t \in acknowledged \cap beganDuringCheckpoint: t \in recovered)
             [] OTHER -> FALSE
WitnessNotReached == ~Reached
=============================================================================
