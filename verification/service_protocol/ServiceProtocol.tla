--------------------------- MODULE ServiceProtocol ---------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS CancelAccepted, LeakRegistration, ReplayAccepted, UnknownMeansAbort
Clients == 1..2
VARIABLES phase, alive, registered, accepted, applied, outcome, learned
vars == <<phase, alive, registered, accepted, applied, outcome, learned>>

Init ==
    /\ phase = [c \in Clients |-> "new"]
    /\ alive = [c \in Clients |-> TRUE]
    /\ registered = [c \in Clients |-> FALSE]
    /\ accepted = [c \in Clients |-> FALSE]
    /\ applied = [c \in Clients |-> 0]
    /\ outcome = [c \in Clients |-> "unknown"]
    /\ learned = [c \in Clients |-> "unknown"]

Begin(c) ==
    /\ alive[c] /\ phase[c] = "new"
    /\ phase' = [phase EXCEPT ![c] = "open"]
    /\ registered' = [registered EXCEPT ![c] = TRUE]
    /\ UNCHANGED <<alive, accepted, applied, outcome, learned>>

Accept(c) ==
    /\ alive[c] /\ phase[c] = "open"
    /\ phase' = [phase EXCEPT ![c] = "accepted"]
    /\ accepted' = [accepted EXCEPT ![c] = TRUE]
    /\ outcome' = [outcome EXCEPT ![c] = "pending"]
    /\ UNCHANGED <<alive, registered, applied, learned>>

Finish(c) ==
    /\ phase[c] = "accepted"
    /\ phase' = [phase EXCEPT ![c] = "committed"]
    /\ registered' = [registered EXCEPT ![c] = FALSE]
    /\ applied' = [applied EXCEPT ![c] = @ + 1]
    /\ outcome' = [outcome EXCEPT ![c] = "committed"]
    /\ UNCHANGED <<alive, accepted, learned>>

(* Only client 1 dies; client 2 is the surviving independent executor. *)
Kill ==
    /\ alive[1]
    /\ alive' = [alive EXCEPT ![1] = FALSE]
    /\ UNCHANGED <<phase, registered, accepted, applied, outcome, learned>>

Cleanup(c) ==
    /\ ~alive[c]
    /\ phase[c] = "open" \/ (CancelAccepted /\ phase[c] = "accepted")
    /\ phase' = [phase EXCEPT ![c] = "aborted"]
    /\ registered' = [registered EXCEPT ![c] = LeakRegistration]
    /\ outcome' = [outcome EXCEPT ![c] = "aborted"]
    /\ UNCHANGED <<alive, accepted, applied, learned>>

(* An independent resolver can observe a lost reply's outcome. Expiry and
   acknowledgement share the same forgetting transition; neither is an abort. *)
Resolve(c) ==
    /\ learned' = [learned EXCEPT ![c] =
         IF UnknownMeansAbort /\ outcome[c] = "unknown"
         THEN "aborted" ELSE outcome[c]]
    /\ UNCHANGED <<phase, alive, registered, accepted, applied, outcome>>

Forget(c) ==
    /\ outcome[c] \in {"committed", "aborted"}
    /\ outcome' = [outcome EXCEPT ![c] = "unknown"]
    /\ UNCHANGED <<phase, alive, registered, accepted, applied, learned>>

BadReplay(c) ==
    /\ ReplayAccepted /\ phase[c] = "committed" /\ applied[c] = 1
    /\ applied' = [applied EXCEPT ![c] = 2]
    /\ UNCHANGED <<phase, alive, registered, accepted, outcome, learned>>

Next == Kill \/ \E c \in Clients:
    Begin(c) \/ Accept(c) \/ Finish(c) \/ Cleanup(c) \/ Resolve(c)
    \/ Forget(c) \/ BadReplay(c)

Spec == Init /\ [][Next]_vars
    /\ \A c \in Clients: WF_vars(Begin(c)) /\ WF_vars(Accept(c))
         /\ WF_vars(Finish(c)) /\ WF_vars(Cleanup(c))

TypeOK ==
    /\ phase \in [Clients -> {"new", "open", "accepted", "committed", "aborted"}]
    /\ alive \in [Clients -> BOOLEAN]
    /\ registered \in [Clients -> BOOLEAN]
    /\ accepted \in [Clients -> BOOLEAN]
    /\ applied \in [Clients -> 0..2]
    /\ outcome \in [Clients -> {"unknown", "pending", "committed", "aborted"}]
    /\ learned \in [Clients -> {"unknown", "pending", "committed", "aborted"}]

Safety == \A c \in Clients:
    /\ applied[c] <= 1
    /\ (accepted[c] => phase[c] \in {"accepted", "committed"})
    /\ (phase[c] \in {"committed", "aborted"} => ~registered[c])
    /\ (learned[c] = "committed" => applied[c] = 1)
    /\ (learned[c] = "aborted" => applied[c] = 0 /\ ~accepted[c])
    /\ (outcome[c] = "committed" => applied[c] = 1)

Progress ==
    /\ \A c \in Clients:
         (phase[c] = "accepted" ~> phase[c] = "committed")
    /\ (~alive[1] /\ phase[1] = "open" ~> ~registered[1])
    /\ (phase[2] = "new" ~> phase[2] = "committed")

(* Counterexample of this invariant is a positive reachability witness. *)
NoLostReplySurvivorWitness ==
    ~(~alive[1] /\ applied[1] = 1 /\ learned[1] = "committed"
      /\ applied[2] = 1 /\ ~registered[1] /\ ~registered[2])
=============================================================================
