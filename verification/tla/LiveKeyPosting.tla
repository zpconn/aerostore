--------------------------- MODULE LiveKeyPosting ---------------------------
EXTENDS Naturals, FiniteSets
\* A permanent posting 0 keeps its key alive. Two additional posting blocks
\* repeatedly move free -> reachable -> retired -> free under this live key.
CONSTANTS Mutation, Witness
Blocks == {1, 2}
VARIABLES reachable, retired, free, reclaimed
vars == <<reachable, retired, free, reclaimed>>
Init == /\ reachable = {0} /\ retired = {} /\ free = Blocks /\ reclaimed = FALSE
Insert(p) == /\ p \in free /\ reachable' = reachable \cup {p} /\ free' = free \ {p}
             /\ UNCHANGED <<retired, reclaimed>>
Remove(p) == /\ p \in reachable /\ reachable' = reachable \ {p} /\ retired' = retired \cup {p}
             /\ UNCHANGED <<free, reclaimed>>
Collect == /\ retired # {} /\ (Mutation # "OnlyEmptyKeys" \/ reachable = {})
           /\ free' = free \cup retired /\ retired' = {} /\ reclaimed' = TRUE
           /\ UNCHANGED reachable
\* An exhausted allocation may retry forever; don't turn the bad collector
\* into a deadlock error and mistakenly count that as a liveness failure.
RetryWhenExhausted == free = {} /\ UNCHANGED vars
Next == (\E p \in Blocks: Insert(p) \/ Remove(p)) \/ Collect \/ RetryWhenExhausted
\* One full collection pass is atomic here; finite traversal/admission are
\* assumptions. The priority pilot explores admission separately; their
\* composition and the actual traversal implementation are not proved here.
Spec == Init /\ [][Next]_vars /\ WF_vars(Collect)
TypeOK == /\ reachable \subseteq Blocks \cup {0} /\ retired \subseteq Blocks
          /\ free \subseteq Blocks /\ reclaimed \in BOOLEAN
Safety == /\ 0 \in reachable /\ (reachable \ {0}) \cup retired \cup free = Blocks
          /\ reachable \cap retired = {} /\ reachable \cap free = {} /\ retired \cap free = {}
RetiredEventuallyReusable == \A p \in Blocks: (p \in retired) ~> (p \in free)
WitnessNotReached == ~(Witness = "reclaims_live_key_posting" /\ reclaimed /\ 0 \in reachable)
=============================================================================
