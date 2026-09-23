-------------------------- MODULE PrimaryKeyInsert --------------------------
EXTENDS Naturals, FiniteSets

\* Two one-shot insertions into one append-only hash bucket. Published entries
\* and their next links are immutable, with no deletion, reuse, or ABA.
\* Search of a captured chain is one abstract step; this is NOT Rust refinement.
CONSTANTS Scenario, Api, Mutation, Witness
Workers == {1, 2}
Nodes == Workers
Keys == {1, 2}
Key(p) == IF Scenario = "SameKey" THEN 1 ELSE p
VARIABLES pc, head, link, searchedHead, expectedHead, proposed, nextRowId,
          allocated, published, result, winner, retried, foundAfterRetry
vars == <<pc, head, link, searchedHead, expectedHead, proposed, nextRowId,
          allocated, published, result, winner, retried, foundAfterRetry>>

\* There are at most two published nodes. Following the head and its successor
\* is the entire chain, rather than a depth cutoff on a larger structure.
Chain(h) == IF h = 0 THEN {} ELSE {h} \cup (IF link[h] = 0 THEN {} ELSE {link[h]})
Matching(h, p) == {n \in Chain(h): Key(n) = Key(p)}

Init == /\ pc = [p \in Workers |-> "Capture"]
        /\ head = 0 /\ link = [n \in Nodes |-> 0]
        /\ searchedHead = [p \in Workers |-> 0]
        /\ expectedHead = [p \in Workers |-> 0]
        /\ proposed = [p \in Workers |-> 0] /\ nextRowId = 1
        /\ allocated = {} /\ published = {}
        /\ result = [p \in Workers |-> 0] /\ winner = [k \in Keys |-> 0]
        /\ retried = {} /\ foundAfterRetry = {}

Capture(p) ==
    /\ pc[p] = "Capture"
    /\ searchedHead' = [searchedHead EXCEPT ![p] = head]
    /\ pc' = [pc EXCEPT ![p] = "Search"]
    /\ UNCHANGED <<head, link, expectedHead, proposed, nextRowId,
          allocated, published, result, winner, retried, foundAfterRetry>>

Search(p) ==
    /\ pc[p] = "Search"
    /\ LET matches == Matching(searchedHead[p], p)
       IN IF matches # {} THEN
              /\ result' = [result EXCEPT ![p] = proposed[CHOOSE n \in matches: TRUE]]
              /\ pc' = [pc EXCEPT ![p] = "Done"]
              /\ foundAfterRetry' = IF p \in retried THEN foundAfterRetry \cup {p}
                                     ELSE foundAfterRetry
          ELSE
              /\ pc' = [pc EXCEPT ![p] = IF p \in allocated THEN "Prepare" ELSE "Allocate"]
              /\ UNCHANGED <<result, foundAfterRetry>>
    /\ UNCHANGED <<head, link, searchedHead, expectedHead, proposed, nextRowId,
          allocated, published, winner, retried>>

Allocate(p) ==
    /\ pc[p] = "Allocate" /\ p \notin allocated
    \* Allocate represents get_or_insert's reserved row ID; Existing represents
    \* insert_existing called with two distinct proposed IDs. Entry node p is
    \* private until CAS succeeds and is reused across retries.
    /\ proposed' = [proposed EXCEPT ![p] = IF Api = "Allocate" THEN nextRowId ELSE p]
    /\ nextRowId' = IF Api = "Allocate" THEN nextRowId + 1 ELSE nextRowId
    /\ allocated' = allocated \cup {p} /\ pc' = [pc EXCEPT ![p] = "Prepare"]
    /\ UNCHANGED <<head, link, searchedHead, expectedHead, published,
          result, winner, retried, foundAfterRetry>>

Prepare(p) ==
    /\ pc[p] = "Prepare"
    /\ LET expected == IF Mutation = "LateHead" THEN head ELSE searchedHead[p]
       IN /\ expectedHead' = [expectedHead EXCEPT ![p] = expected]
          /\ link' = [link EXCEPT ![p] = expected]
    /\ pc' = [pc EXCEPT ![p] = "CAS"]
    /\ UNCHANGED <<head, searchedHead, proposed, nextRowId, allocated,
          published, result, winner, retried, foundAfterRetry>>

CompareAndSwap(p) ==
    /\ pc[p] = "CAS"
    /\ IF head = expectedHead[p] THEN
          /\ head' = p /\ published' = published \cup {p}
          /\ result' = [result EXCEPT ![p] = proposed[p]]
          \* First successful publication fixes the abstract key's row ID.
          /\ winner' = [winner EXCEPT ![Key(p)] = IF @ = 0 THEN proposed[p] ELSE @]
          /\ pc' = [pc EXCEPT ![p] = "Done"]
          /\ UNCHANGED <<retried, searchedHead>>
       ELSE
          \* Failed CAS returns the observed head; that exact chain is searched
          \* before another publication attempt (no second unvalidated load).
          /\ searchedHead' = [searchedHead EXCEPT ![p] = head]
          /\ pc' = [pc EXCEPT ![p] = "Search"]
          /\ retried' = retried \cup {p}
          /\ UNCHANGED <<head, published, result, winner>>
    /\ UNCHANGED <<link, expectedHead, proposed, nextRowId,
          allocated, foundAfterRetry>>

Next == (\E p \in Workers: Capture(p) \/ Search(p) \/ Allocate(p)
                           \/ Prepare(p) \/ CompareAndSwap(p))
        \/ ((\A p \in Workers: pc[p] = "Done") /\ UNCHANGED vars)

TypeOK == /\ pc \in [Workers -> {"Capture", "Search", "Allocate", "Prepare", "CAS", "Done"}]
          /\ head \in 0..2 /\ link \in [Nodes -> 0..2]
          /\ searchedHead \in [Workers -> 0..2] /\ expectedHead \in [Workers -> 0..2]
          /\ proposed \in [Workers -> 0..2] /\ nextRowId \in 1..3
          /\ allocated \subseteq Workers /\ published \subseteq allocated
          /\ result \in [Workers -> 0..2] /\ winner \in [Keys -> 0..2]
          /\ retried \subseteq Workers /\ foundAfterRetry \subseteq retried
UniqueKeys == \A p, q \in published: p = q \/ Key(p) # Key(q)
ReturnedWinner == \A p \in Workers: pc[p] = "Done" =>
                      result[p] # 0 /\ result[p] = winner[Key(p)]
Safety == /\ UniqueKeys /\ ReturnedWinner /\ published = Chain(head)
          /\ (\A n \in published: link[n] # n)
Reached == CASE Witness = "retry_finds_winner" ->
                  Scenario = "SameKey" /\ foundAfterRetry # {}
                  /\ (\A p \in Workers: pc[p] = "Done")
                  /\ result[1] = result[2] /\ Cardinality(published) = 1
             [] Witness = "colliding_keys_commit" ->
                  Scenario = "CollidingKeys" /\ published = Workers
                  /\ result[1] # result[2]
             [] OTHER -> FALSE
WitnessNotReached == ~Reached
=============================================================================
