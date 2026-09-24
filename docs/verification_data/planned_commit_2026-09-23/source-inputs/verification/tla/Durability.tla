---------------------------- MODULE Durability ----------------------------
EXTENDS Naturals, FiniteSets

\* MODEL-ONLY design exploration. SafeProtocol is a proposed write-ahead,
\* globally quiescent-checkpoint protocol, NOT a claim about current Rust.
\* Two transactions: T2 can start only after observing T1's visible output.
\* DurableAppend and PersistCheckpoint are trusted atomic persistence steps;
\* torn records, fsync/rename ordering, mmap recovery and ring details omitted.
CONSTANTS Scenario, SafeProtocol, Witness
Tx == {1, 2}
VARIABLES pc, visible, allocated, durableLog, acknowledged,
          checkpointPc, snapshotRows, sampledCut, checkpointImage, durableCut,
          crashed, recovered
vars == <<pc, visible, allocated, durableLog, acknowledged, checkpointPc,
          snapshotRows, sampledCut, checkpointImage, durableCut, crashed, recovered>>
Init == /\ pc = [t \in Tx |-> "Start"]
        /\ visible = {} /\ allocated = {} /\ durableLog = {} /\ acknowledged = {}
        /\ checkpointPc = "Idle" /\ snapshotRows = {} /\ sampledCut = 0
        /\ checkpointImage = {} /\ durableCut = 0
        /\ crashed = FALSE /\ recovered = {}

WriteAhead == SafeProtocol \/ Scenario = "Checkpoint"
TransactionAllowed == ~crashed /\ ~(SafeProtocol /\ Scenario = "Checkpoint"
                         /\ checkpointPc \notin {"Idle", "Done"})
Start(t) == /\ TransactionAllowed /\ pc[t] = "Start"
            /\ (t = 1 \/ 1 \in visible)
            /\ allocated' = allocated \cup {t}
            /\ pc' = [pc EXCEPT ![t] = IF WriteAhead THEN "Log" ELSE "Publish"]
            /\ UNCHANGED <<visible, durableLog, acknowledged, checkpointPc,
                 snapshotRows, sampledCut, checkpointImage, durableCut, crashed, recovered>>
DurableAppend(t) ==
    /\ TransactionAllowed /\ pc[t] = "Log"
    /\ durableLog' = durableLog \cup {t}
    /\ pc' = [pc EXCEPT ![t] = IF WriteAhead THEN "Publish" ELSE "Ack"]
    /\ UNCHANGED <<visible, allocated, acknowledged, checkpointPc, snapshotRows,
                    sampledCut, checkpointImage, durableCut, crashed, recovered>>
Publish(t) ==
    /\ TransactionAllowed /\ pc[t] = "Publish"
    /\ visible' = visible \cup {t}
    /\ pc' = [pc EXCEPT ![t] = IF WriteAhead THEN "Ack" ELSE "Log"]
    /\ UNCHANGED <<allocated, durableLog, acknowledged, checkpointPc, snapshotRows,
                    sampledCut, checkpointImage, durableCut, crashed, recovered>>
Ack(t) == /\ TransactionAllowed /\ pc[t] = "Ack"
          /\ acknowledged' = acknowledged \cup {t}
          /\ pc' = [pc EXCEPT ![t] = "Done"]
          /\ UNCHANGED <<visible, allocated, durableLog, checkpointPc, snapshotRows,
                    sampledCut, checkpointImage, durableCut, crashed, recovered>>

CheckpointSnapshot ==
    /\ ~crashed /\ Scenario = "Checkpoint" /\ checkpointPc = "Idle"
    \* The safe design stops new transactions and drains old ones before the
    \* snapshot. The unsafe variant has no inter-process exclusion.
    /\ (~SafeProtocol \/ \A t \in Tx: pc[t] \in {"Start", "Done"})
    /\ snapshotRows' = visible
    /\ checkpointPc' = "SampleCut"
    /\ UNCHANGED <<pc, allocated, durableLog, acknowledged, visible, sampledCut,
                    checkpointImage, durableCut, crashed, recovered>>
SampleCut ==
    /\ ~crashed /\ checkpointPc = "SampleCut"
    /\ sampledCut' = IF allocated = {} THEN 0 ELSE IF 2 \in allocated THEN 2 ELSE 1
    /\ checkpointPc' = "Persist"
    /\ UNCHANGED <<pc, allocated, durableLog, acknowledged, visible, snapshotRows,
                    checkpointImage, durableCut, crashed, recovered>>
PersistCheckpoint ==
    /\ ~crashed /\ checkpointPc = "Persist"
    /\ checkpointImage' = snapshotRows /\ durableCut' = sampledCut
    /\ checkpointPc' = "Truncate"
    /\ UNCHANGED <<pc, allocated, durableLog, acknowledged, visible, snapshotRows,
                    sampledCut, crashed, recovered>>
Truncate ==
    /\ ~crashed /\ checkpointPc = "Truncate"
    /\ durableLog' = {} /\ checkpointPc' = "Done"
    /\ UNCHANGED <<pc, allocated, acknowledged, visible, snapshotRows, sampledCut,
                    checkpointImage, durableCut, crashed, recovered>>
Crash ==
    /\ ~crashed /\ crashed' = TRUE
    \* Match checkpoint replay's start-ID cutoff. Exposing an incoherent cut
    \* can lose an acknowledged record even before the whole-log truncation.
    /\ recovered' = checkpointImage \cup {t \in durableLog : t > durableCut}
    /\ UNCHANGED <<pc, allocated, durableLog, acknowledged, visible, snapshotRows,
                    sampledCut, checkpointImage, durableCut, checkpointPc>>
Next == (\E t \in Tx: Start(t) \/ DurableAppend(t) \/ Publish(t) \/ Ack(t))
        \/ CheckpointSnapshot \/ SampleCut \/ PersistCheckpoint \/ Truncate \/ Crash
        \/ (crashed /\ UNCHANGED vars)

TypeOK == /\ visible \subseteq Tx /\ allocated \subseteq Tx /\ durableLog \subseteq Tx
          /\ acknowledged \subseteq Tx /\ recovered \subseteq Tx
          /\ snapshotRows \subseteq Tx /\ checkpointImage \subseteq Tx
          /\ sampledCut \in 0..2 /\ durableCut \in 0..2 /\ crashed \in BOOLEAN
DurabilitySafety == ~crashed \/
                    (acknowledged \subseteq recovered /\ (2 \in recovered => 1 \in recovered))
\* Separate target obtains a trace in which the dependent caller has actually
\* received success, not only a trace with an unacknowledged surviving record.
AcknowledgedSafety == ~crashed \/ (acknowledged \subseteq recovered /\
                                  (2 \in acknowledged => 1 \in recovered))
Reached == CASE Witness = "nonempty_recovery" -> crashed /\ acknowledged # {} /\ acknowledged \subseteq recovered
             [] Witness = "dependent_recovery" -> crashed /\ 2 \in acknowledged /\ recovered = Tx
             [] Witness = "checkpoint_recovery" -> crashed /\ checkpointPc = "Done"
                       /\ checkpointImage # {} /\ acknowledged \subseteq recovered
             [] OTHER -> FALSE
WitnessNotReached == ~Reached
=============================================================================
