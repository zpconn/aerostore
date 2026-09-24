import AerostoreProofs.Contracts

namespace AerostoreProofs

/-- The data-bearing validation contract rejects either a changed stamp or a
    stamp that is too recent. These obligations are independent. -/
theorem dependency_rejects_changed : DependencyRejectsChanged := by
  intro recorded current start changed accepted
  exact changed accepted.1

theorem dependency_rejects_recent : DependencyRejectsRecent := by
  intro recorded current start recent accepted
  exact Nat.not_lt_of_ge recent accepted.2

theorem predicate_history_monotone : PredicateHistoryMonotone := by
  intro before history
  induction history generalizing before with
  | nil => intro _ bucket; exact Nat.le_refl _
  | cons event rest ih =>
    intro fresh bucket
    have tail := ih (publishPredicateStamp before event) fresh.2 bucket
    apply Nat.le_trans _ tail
    unfold publishPredicateStamp
    split_ifs
    · exact Nat.le_of_lt (fresh.1 bucket)
    · exact Nat.le_refl _

theorem predicate_history_covers_events : PredicateHistoryCoversEvents := by
  intro before history
  induction history generalizing before with
  | nil => intro _ event member; simp at member
  | cons first rest ih =>
    intro fresh event member bucket affected
    rcases List.mem_cons.mp member with equal | member
    · subst event
      have tail := predicate_history_monotone (publishPredicateStamp before first) rest fresh.2 bucket
      simpa [publishPredicateStamp, affected, replayPredicatePublications] using tail
    · exact ih (publishPredicateStamp before first) fresh.2 event member bucket affected

theorem accepted_read_excludes_later_publication : AcceptedReadExcludesLaterPublication := by
  intro before recorded history reads start fresh accepted event member later
  apply Set.disjoint_left.mpr
  intro bucket read affected
  have covered := predicate_history_covers_events before history fresh event member bucket affected
  have older := (accepted bucket read).2
  omega

/-- Own inserts, moves into/out of a predicate, and deletes use the same overlay.
    Completeness of the stable snapshot's raw candidates is an explicit premise;
    this theorem does not establish the skiplist/MVCC premise. -/
theorem own_write_predicate_complete : OwnWritePredicateComplete := by
  intro snapshot own predicate candidates complete
  ext row
  constructor
  · intro result; exact result.2
  · intro matching
    constructor
    · by_cases untouched : own row = none
      · left
        apply complete row
        simpa [rowMatches, overlayOwnWrites, untouched] using matching
      · exact Or.inr untouched
    · exact matching

/-- This concrete instance prevents a local insertion from being accidentally
    erased by weakening the abstract candidate-completeness precondition. -/
theorem own_insert_is_visible :
    7 ∈ materializePredicate (fun _ => none) (fun row => if row = 7 then some (some 9) else none)
      (fun key => key = 9) ∅ := by
  simp [materializePredicate, rowMatches, overlayOwnWrites]

/-- An older writer's identifier cannot substitute for its publication stamp. -/
theorem older_writer_requires_fresh_stamp :
    dependencyAccepted 1 1 2 ∧ ¬ dependencyAccepted 3 3 2 := by
  simp [dependencyAccepted]

/-- Empty reads still carry a bucket dependency: the post-start creation stamp
    rejects the read although no row had been materialized. -/
theorem empty_creation_is_rejected :
    ¬ readDependenciesAccepted {5} (fun _ => 0)
      (replayPredicatePublications (fun _ => 0) [⟨{5}, 3⟩]) 2 := by
  intro accepted
  have checked := (accepted 5 (by simp)).2
  simp [replayPredicatePublications, publishPredicateStamp] at checked

/-- Even a different key in the same bucket forces a conservative retry. -/
theorem bucket_collision_is_conservative :
    ¬ dependencyAccepted 0 3 4 := by
  simp [dependencyAccepted]

end AerostoreProofs
