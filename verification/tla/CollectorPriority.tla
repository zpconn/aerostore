------------------------- MODULE CollectorPriority -------------------------
EXTENDS Naturals
\* One foreground actor and one collector share an abstract CAS mutex.
\* A cached pre-acquisition priority observation exposes the recheck race.
CONSTANTS Mutation, Witness
VARIABLES owner, fgPhase, waiting, acquiredWhileWaiting, done
vars == <<owner, fgPhase, waiting, acquiredWhileWaiting, done>>
Init == /\ owner = "Free" /\ fgPhase = "Idle" /\ waiting = FALSE
        /\ acquiredWhileWaiting = FALSE /\ done = FALSE

Request == /\ ~waiting /\ ~done /\ waiting' = TRUE
           /\ UNCHANGED <<owner, fgPhase, acquiredWhileWaiting, done>>
ForegroundObserve ==
    /\ fgPhase = "Idle" /\ owner = "Free"
    /\ (~waiting \/ Mutation = "NoPriority")
    /\ fgPhase' = "Ready"
    /\ UNCHANGED <<owner, waiting, acquiredWhileWaiting, done>>
ForegroundAcquire ==
    /\ fgPhase = "Ready" /\ owner = "Free"
    /\ owner' = "Foreground" /\ fgPhase' = "Recheck"
    /\ acquiredWhileWaiting' = waiting
    /\ UNCHANGED <<waiting, done>>
ForegroundRecheck ==
    /\ fgPhase = "Recheck" /\ owner = "Foreground"
    /\ IF waiting /\ Mutation \notin {"NoPriority", "NoRecheck"}
          THEN /\ owner' = "Free" /\ fgPhase' = "Idle"
          ELSE /\ owner' = owner /\ fgPhase' = "Work"
    /\ UNCHANGED <<waiting, acquiredWhileWaiting, done>>
ForegroundRelease ==
    /\ fgPhase = "Work" /\ owner = "Foreground"
    /\ owner' = "Free" /\ fgPhase' = "Idle"
    /\ UNCHANGED <<waiting, acquiredWhileWaiting, done>>
CollectorAcquire ==
    /\ waiting /\ owner = "Free" /\ owner' = "Collector"
    /\ UNCHANGED <<fgPhase, waiting, acquiredWhileWaiting, done>>
CollectorFinish ==
    /\ owner = "Collector" /\ owner' = "Free" /\ waiting' = FALSE /\ done' = TRUE
    /\ UNCHANGED <<fgPhase, acquiredWhileWaiting>>
Next == Request \/ ForegroundObserve \/ ForegroundAcquire \/ ForegroundRecheck
        \/ ForegroundRelease \/ CollectorAcquire \/ CollectorFinish
\* ONLY weak fairness: a collector CAS enabled intermittently can starve.
\* In the priority design, eventually it remains continuously enabled.
Spec == Init /\ [][Next]_vars /\ WF_vars(Request)
        /\ WF_vars(ForegroundObserve) /\ WF_vars(ForegroundAcquire)
        /\ WF_vars(ForegroundRecheck) /\ WF_vars(ForegroundRelease)
        /\ WF_vars(CollectorAcquire) /\ WF_vars(CollectorFinish)
TypeOK == /\ owner \in {"Free", "Foreground", "Collector"}
          /\ fgPhase \in {"Idle", "Ready", "Recheck", "Work"}
          /\ waiting \in BOOLEAN /\ acquiredWhileWaiting \in BOOLEAN /\ done \in BOOLEAN
Safety == owner = "Foreground" => fgPhase \in {"Recheck", "Work"}
AdmissionSafety == Safety /\ ~(owner = "Foreground" /\ fgPhase = "Work" /\ acquiredWhileWaiting)
CollectorEventuallyRuns == <>done
WitnessNotReached == ~(Witness = "collector_runs" /\ done)
=============================================================================
