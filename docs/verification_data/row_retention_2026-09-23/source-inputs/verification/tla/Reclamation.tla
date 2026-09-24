--------------------------- MODULE Reclamation ---------------------------
EXTENDS Naturals, FiniteSets
\* One old version, an older writer, and two reusable reader slots.
\* The Boolean "old" epoch is a scenario abstraction, NOT a proved general
\* timestamp quotient. No integer wrap is used. Readers turn over forever.
CONSTANTS Mutation, UseGuard, Witness
Readers == {1, 2}
VARIABLES phase, writerActive, readers, needsOld, pinsOld, turn, retired, free, guard
vars == <<phase, writerActive, readers, needsOld, pinsOld, turn, retired, free, guard>>
Init == /\ phase = "FirstReader" /\ writerActive = TRUE
        /\ readers = {} /\ needsOld = {} /\ pinsOld = {} /\ turn = 1
        /\ retired = FALSE /\ free = FALSE /\ guard = UseGuard

FirstReader ==
    /\ phase = "FirstReader"
    /\ readers' = {1} /\ needsOld' = {1} /\ pinsOld' = {1}
    /\ phase' = "WriterCommit"
    /\ UNCHANGED <<writerActive, turn, retired, free, guard>>
WriterCommit ==
    /\ phase = "WriterCommit"
    /\ writerActive' = FALSE /\ retired' = TRUE
    /\ phase' = "SecondReader"
    /\ UNCHANGED <<readers, needsOld, pinsOld, turn, free, guard>>
SecondReader ==
    /\ phase = "SecondReader"
    /\ readers' = {1, 2}
    /\ pinsOld' = IF Mutation = "InheritedHorizon" THEN {1, 2} ELSE {1}
    /\ phase' = "EndFirst"
    /\ UNCHANGED <<writerActive, needsOld, turn, retired, free, guard>>
EndFirst ==
    /\ phase = "EndFirst"
    /\ readers' = {2} /\ needsOld' = {} /\ pinsOld' = pinsOld \ {1}
    /\ phase' = "StartNext"
    /\ UNCHANGED <<writerActive, turn, retired, free, guard>>
StartNext ==
    /\ phase = "StartNext"
    /\ readers' = readers \cup {turn}
    /\ pinsOld' = IF Mutation = "InheritedHorizon" /\ pinsOld # {}
                  THEN pinsOld \cup {turn} ELSE pinsOld
    /\ phase' = "EndPrevious"
    /\ UNCHANGED <<writerActive, needsOld, turn, retired, free, guard>>
EndPrevious ==
    /\ phase = "EndPrevious"
    /\ readers' = readers \ {3 - turn} /\ pinsOld' = pinsOld \ {3 - turn}
    /\ turn' = 3 - turn /\ phase' = "StartNext"
    /\ UNCHANGED <<writerActive, needsOld, retired, free, guard>>
Foreground == FirstReader \/ WriterCommit \/ SecondReader \/ EndFirst \/ StartNext \/ EndPrevious

ReleaseGuard == /\ guard /\ retired /\ needsOld = {}
                /\ guard' = FALSE
                /\ UNCHANGED <<phase, writerActive, readers, needsOld, pinsOld, turn, retired, free>>
Collect ==
    /\ retired /\ ~free
    /\ (IF Mutation = "ActiveOnly" THEN ~writerActive ELSE pinsOld = {})
    /\ (~guard \/ Mutation = "IgnoreGuard")
    /\ free' = TRUE
    /\ UNCHANGED <<phase, writerActive, readers, needsOld, pinsOld, turn, retired, guard>>
Next == Foreground \/ ReleaseGuard \/ Collect
\* Assumptions: reader operations eventually take scheduled steps, a guard
\* whose actual reader obligations ended eventually drops, and an eligible
\* collector eventually runs. Collect fairness does NOT make it eligible.
Spec == Init /\ [][Next]_vars /\ WF_vars(Foreground) /\ WF_vars(ReleaseGuard) /\ WF_vars(Collect)
TypeOK == /\ readers \subseteq Readers /\ needsOld \subseteq readers /\ pinsOld \subseteq readers
          /\ turn \in Readers /\ writerActive \in BOOLEAN /\ retired \in BOOLEAN
          /\ free \in BOOLEAN /\ guard \in BOOLEAN
Safety == ~free \/ (needsOld = {} /\ ~guard)
ReclaimedEventually == <>free
WitnessNotReached == ~(Witness = "reused_with_continuous_readers" /\ free /\ readers # {})
=============================================================================
