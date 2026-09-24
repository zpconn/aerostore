import AerostoreProofs.Predicate

namespace AerostoreProofs

/-- Logical transaction identities are never reused; physical ProcArray slots may
    be reused for distinct identities. `clock` is the next reservation value. -/
structure LifecycleState where
  clock : Nat
  starts : Nat → Option Nat
  active : Set Nat
  finished : Set Nat
  observed : Set (Nat × Nat)
  /-- Label assigned at the publication fetch_add, before later bucket stores. -/
  publication : Nat → Option Nat

def lifecycleInitial (clock : Nat) : LifecycleState :=
  ⟨clock, fun _ => none, ∅, ∅, ∅, fun _ => none⟩

/-- Atomic metadata steps under the lifecycle mutex. Clock reservations represent
    fetch_add's modification order, including arbitrary intervening reservations.
    This relation is a mathematical protocol, not extracted native atomic code.
    It overapproximates scheduling of reserve/register: snapshot completeness
    across their critical section is checked separately by the lifecycle model
    and remains a native refinement obligation. -/
inductive LifecycleStep : LifecycleState → LifecycleState → Prop
  | reserve (s : LifecycleState) (tx : Nat) (unused : s.starts tx = none) :
      LifecycleStep s {s with clock := s.clock + 1, starts := Function.update s.starts tx (some s.clock)}
  | register (s : LifecycleState) (tx start : Nat) (reserved : s.starts tx = some start)
      (notFinished : tx ∉ s.finished) (notPublished : s.publication tx = none) :
      LifecycleStep s {s with active := insert tx s.active}
  | snapshot (s : LifecycleState) (reader start : Nat) (reserved : s.starts reader = some start)
      (registered : reader ∈ s.active) :
      LifecycleStep s {s with observed := s.observed ∪ {pair | pair.1 = reader ∧ pair.2 ∈ s.active}}
  | finish (s : LifecycleState) (tx : Nat) (registered : tx ∈ s.active) :
      LifecycleStep s {s with active := s.active \ {tx}, finished := insert tx s.finished}
  | publish (s : LifecycleState) (tx : Nat) (ended : tx ∈ s.finished)
      (notPublished : s.publication tx = none) :
      LifecycleStep s {s with clock := s.clock + 1, publication := Function.update s.publication tx (some s.clock)}

/-- Reachability includes arbitrary finite numbers of transactions and steps. -/
inductive LifecycleReachable (initialClock : Nat) : LifecycleState → Prop
  | initial : LifecycleReachable initialClock (lifecycleInitial initialClock)
  | step {before after : LifecycleState} :
      LifecycleReachable initialClock before → LifecycleStep before after →
      LifecycleReachable initialClock after

def LifecycleInvariant (s : LifecycleState) : Prop :=
  (∀ tx start, s.starts tx = some start → start < s.clock) ∧
  (∀ reader writer, (reader, writer) ∈ s.observed → ∃ start, s.starts reader = some start) ∧
  (∀ tx, tx ∈ s.active → s.publication tx = none) ∧
  Disjoint s.active s.finished ∧
  (∀ reader writer start stamp, (reader, writer) ∈ s.observed →
    s.starts reader = some start → s.publication writer = some stamp → start < stamp)

def LifecycleInductive : Prop :=
  ∀ before after, LifecycleInvariant before → LifecycleStep before after → LifecycleInvariant after

def LifecycleLatePublication : Prop :=
  ∀ initialClock s, LifecycleReachable initialClock s →
  ∀ reader writer start stamp, (reader, writer) ∈ s.observed →
    s.starts reader = some start → s.publication writer = some stamp → start < stamp

/-- Native u64 increment agrees with this nonwrapping model only below MAX.
    Exhaustion handling is a separate implementation obligation. -/
def LifecycleMachineReservation : Prop :=
  ∀ clock : Nat, clock < 2^64 - 1 →
    (clock + 1) % (2^64) = clock + 1 ∧ clock < clock + 1

theorem lifecycle_initial_invariant (clock : Nat) : LifecycleInvariant (lifecycleInitial clock) := by
  simp [LifecycleInvariant, lifecycleInitial]

theorem lifecycle_step_preserves : LifecycleInductive := by
  intro before after valid step
  rcases valid with ⟨clockBound, observedKnown, activeUnpublished, disjoint, late⟩
  cases step with
  | reserve tx unused =>
    refine ⟨?_, ?_, activeUnpublished, disjoint, ?_⟩
    · intro other start value
      by_cases same : other = tx
      · subst other
        simp at value
        change start < before.clock + 1
        omega
      · simp only [Function.update_of_ne same] at value
        exact Nat.lt_trans (clockBound other start value) (Nat.lt_succ_self _)
    · intro reader writer observed
      obtain ⟨start, existing⟩ := observedKnown reader writer observed
      have different : reader ≠ tx := by
        intro same; subst reader; rw [unused] at existing; contradiction
      exact ⟨start, by simpa [different] using existing⟩
    · intro reader writer start stamp observed reserved published
      have different : reader ≠ tx := by
        obtain ⟨oldStart, existing⟩ := observedKnown reader writer observed
        intro same; subst reader; rw [unused] at existing; contradiction
      simp only [Function.update_of_ne different] at reserved
      exact late reader writer start stamp observed reserved published
  | register tx start reserved notFinished notPublished =>
    refine ⟨clockBound, observedKnown, ?_, ?_, late⟩
    · intro other active
      rcases Set.mem_insert_iff.mp active with same | old
      · simpa [same] using notPublished
      · exact activeUnpublished other old
    · exact Set.disjoint_left.mpr (by
        intro other active finished
        rcases Set.mem_insert_iff.mp active with same | old
        · subst other; exact notFinished finished
        · exact Set.disjoint_left.mp disjoint old finished)
  | snapshot reader start reserved registered =>
    refine ⟨clockBound, ?_, activeUnpublished, disjoint, ?_⟩
    · intro other writer observed
      rcases observed with old | added
      · exact observedKnown other writer old
      · have same : other = reader := added.1
        subst other
        exact ⟨start, reserved⟩
    · intro other writer otherStart stamp observed started published
      rcases observed with old | added
      · exact late other writer otherStart stamp old started published
      · have absent := activeUnpublished writer added.2
        rw [absent] at published
        contradiction
  | finish tx registered =>
    refine ⟨clockBound, observedKnown, ?_, ?_, late⟩
    · intro other active; exact activeUnpublished other active.1
    · apply Set.disjoint_left.mpr
      intro other active finished
      rcases Set.mem_insert_iff.mp finished with same | old
      · exact active.2 (by simpa using same)
      · exact Set.disjoint_left.mp disjoint active.1 old
  | publish tx ended notPublished =>
    refine ⟨?_, observedKnown, ?_, disjoint, ?_⟩
    · intro other start reserved
      exact Nat.lt_trans (clockBound other start reserved) (Nat.lt_succ_self _)
    · intro other active
      have different : other ≠ tx := by
        intro same; subst other
        exact Set.disjoint_left.mp disjoint active ended
      simpa [different] using activeUnpublished other active
    · intro reader writer start stamp observed reserved published
      by_cases same : writer = tx
      · subst writer
        simp at published
        subst stamp
        exact clockBound reader start reserved
      · simp only [Function.update_of_ne same] at published
        exact late reader writer start stamp observed reserved published

theorem lifecycle_reachable_invariant {clock : Nat} {s : LifecycleState}
    (reachable : LifecycleReachable clock s) : LifecycleInvariant s := by
  induction reachable with
  | initial => exact lifecycle_initial_invariant _
  | step _ step ih => exact lifecycle_step_preserves _ _ ih step

def LifecyclePublicationChronology : Prop :=
  ∀ initialClock before after, LifecycleReachable initialClock before → LifecycleStep before after →
  ∀ reader writer start stamp, before.starts reader = some start → before.publication writer = none →
    after.publication writer = some stamp → start < stamp

/-- Covers readers that reserve after writer deregistration but before its publication clock reservation,
    as well as readers that captured the writer active in their snapshot. -/
theorem lifecycle_publication_chronology : LifecyclePublicationChronology := by
  intro initialClock before after reachable step reader writer start stamp reserved absent published
  have bound := (lifecycle_reachable_invariant reachable).1 reader start reserved
  cases step with
  | reserve tx unused =>
    change before.publication writer = some stamp at published
    rw [absent] at published
    contradiction
  | register tx txStart known notFinished notPublished =>
    change before.publication writer = some stamp at published
    rw [absent] at published
    contradiction
  | snapshot reader readerStart known registered =>
    change before.publication writer = some stamp at published
    rw [absent] at published
    contradiction
  | finish tx registered =>
    change before.publication writer = some stamp at published
    rw [absent] at published
    contradiction
  | publish tx ended notPublished =>
    by_cases same : writer = tx
    · subst writer
      simp at published
      subst stamp
      exact bound
    · simp only [Function.update_of_ne same] at published
      rw [absent] at published
      contradiction

/-- An active writer observed by a snapshot must reserve its publication label
    after the reader's reservation. No premise assumes the desired timestamp inequality. -/
theorem lifecycle_late_publication : LifecycleLatePublication := by
  intro initialClock s reachable
  exact (lifecycle_reachable_invariant reachable).2.2.2.2

theorem lifecycle_machine_reservation : LifecycleMachineReservation := by
  intro clock below
  constructor
  · apply Nat.mod_eq_of_lt
    omega
  · omega

def LifecyclePublicationRejectsObservedRead : Prop :=
  ∀ initialClock s, LifecycleReachable initialClock s →
  ∀ reader writer start stamp recorded, (reader, writer) ∈ s.observed →
    s.starts reader = some start → s.publication writer = some stamp →
    ¬ dependencyAccepted recorded stamp start

/-- Connect the derived chronology to the existing scalar read contract. -/
theorem lifecycle_observed_publication_rejected : LifecyclePublicationRejectsObservedRead := by
  intro initialClock s reachable reader writer start stamp recorded observed reserved published
  have later := lifecycle_late_publication initialClock s reachable reader writer start stamp observed reserved published
  exact dependency_rejects_recent recorded stamp start (Nat.le_of_lt later)

def LifecycleOlderWriterWitness : Prop :=
  ∃ s, LifecycleReachable 1 s ∧ s.starts 0 = some 1 ∧ s.starts 1 = some 2 ∧
    (1, 0) ∈ s.observed ∧ s.publication 0 = some 3

set_option linter.unusedSimpArgs false in
/-- A concrete older writer, younger reader, captured active dependency, then
    deregistration and fresh publication. This witnesses reachable premises. -/
theorem lifecycle_older_writer_witness : LifecycleOlderWriterWitness := by
  let a := lifecycleInitial 1
  let b : LifecycleState := {a with clock := 2, starts := Function.update a.starts 0 (some 1)}
  let c : LifecycleState := {b with active := insert 0 b.active}
  let d : LifecycleState := {c with clock := 3, starts := Function.update c.starts 1 (some 2)}
  let e : LifecycleState := {d with active := insert 1 d.active}
  let f : LifecycleState := {e with observed := e.observed ∪ {pair | pair.1 = 1 ∧ pair.2 ∈ e.active}}
  let g : LifecycleState := {f with active := f.active \ {0}, finished := insert 0 f.finished}
  let h : LifecycleState := {g with clock := 4, publication := Function.update g.publication 0 (some 3)}
  have rb : LifecycleReachable 1 b := .step .initial (.reserve a 0 rfl)
  have rc : LifecycleReachable 1 c := .step rb (.register b 0 1 (by simp [a, b, c, d, e, f, g, h, lifecycleInitial]) (by simp [a, b, c, d, e, f, g, h, lifecycleInitial]) (by simp [a, b, c, d, e, f, g, h, lifecycleInitial]))
  have rd : LifecycleReachable 1 d := .step rc (.reserve c 1 (by simp [a, b, c, d, e, f, g, h, lifecycleInitial]))
  have re : LifecycleReachable 1 e := .step rd (.register d 1 2 (by simp [a, b, c, d, e, f, g, h, lifecycleInitial]) (by simp [a, b, c, d, e, f, g, h, lifecycleInitial]) (by simp [a, b, c, d, e, f, g, h, lifecycleInitial]))
  have rf : LifecycleReachable 1 f := .step re (.snapshot e 1 2 (by simp [a, b, c, d, e, f, g, h, lifecycleInitial]) (by simp [a, b, c, d, e, f, g, h, lifecycleInitial]))
  have rg : LifecycleReachable 1 g := .step rf (.finish f 0 (by simp [a, b, c, d, e, f, g, h, lifecycleInitial]))
  have rh : LifecycleReachable 1 h := .step rg (.publish g 0 (by simp [a, b, c, d, e, f, g, h, lifecycleInitial]) (by simp [a, b, c, d, e, f, g, h, lifecycleInitial]))
  exact ⟨h, rh, by simp [a, b, c, d, e, f, g, h, lifecycleInitial], by simp [a, b, c, d, e, f, g, h, lifecycleInitial], by simp [a, b, c, d, e, f, g, h, lifecycleInitial], by simp [a, b, c, d, e, f, g, h, lifecycleInitial]⟩

end AerostoreProofs
