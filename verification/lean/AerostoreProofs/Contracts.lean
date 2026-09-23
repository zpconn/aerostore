import Mathlib.Data.List.Sort

namespace AerostoreProofs

/-- Abstract target only: completeness and canonical ordering. -/
def Canonical (input output : List Nat) : Prop :=
  (∀ key, key ∈ output ↔ key ∈ input) ∧ output.Pairwise (· < ·)

/-- Mathematical finite-set enumeration, not a hand-written extracted Rust body. -/
def bitmapSpecification (input : List Nat) (bucketCount : Nat) : List Nat :=
  (List.range bucketCount).filter (fun key => key ∈ input)

def BucketSpecificationCorrect : Prop :=
  ∀ (input : List Nat) (bucketCount : Nat),
    (∀ key ∈ input, key < bucketCount) →
    Canonical input (bitmapSpecification input bucketCount)

def CanonicalUnique : Prop :=
  ∀ input first second : List Nat,
    Canonical input first → Canonical input second → first = second

/-- Mathematical clock contract, independently of the implementation. -/
def accepted (stamp transactionStart : Nat) : Prop := stamp < transactionStart

def RejectionPersists : Prop :=
  ∀ oldStamp newStamp transactionStart : Nat,
    oldStamp ≤ newStamp → ¬ accepted oldStamp transactionStart →
    ¬ accepted newStamp transactionStart

/-- A dependency is checked even when the captured candidate set was empty. -/
def dependencyAccepted (recorded current transactionStart : Nat) : Prop :=
  current = recorded ∧ current < transactionStart

def DependencyRejectsChanged : Prop :=
  ∀ recorded current start : Nat, current ≠ recorded → ¬ dependencyAccepted recorded current start

def DependencyRejectsRecent : Prop :=
  ∀ recorded current start : Nat, start ≤ current → ¬ dependencyAccepted recorded current start

/-- The exact mathematical read check; scalar stamps do not express lock ownership. -/
def readDependenciesAccepted (reads : Set Nat) (recorded current : Nat → Nat)
    (transactionStart : Nat) : Prop :=
  ∀ bucket ∈ reads, dependencyAccepted (recorded bucket) (current bucket) transactionStart

structure PredicatePublication where
  buckets : Set Nat
  stamp : Nat

/-- Overwrite, as in the native stamp stores; this is not a maximum operation. -/
noncomputable def publishPredicateStamp (before : Nat → Nat) (event : PredicatePublication) : Nat → Nat :=
  fun bucket => @ite Nat (bucket ∈ event.buckets) (Classical.propDecidable _) event.stamp (before bucket)

noncomputable def replayPredicatePublications (before : Nat → Nat)
    (history : List PredicatePublication) : Nat → Nat :=
  match history with
  | [] => before
  | event :: rest => replayPredicatePublications (publishPredicateStamp before event) rest

/-- Fresh global reservations dominate all existing stamps. Machine wraparound and
    correspondence to the real atomic clock remain implementation obligations. -/
def FreshPredicateHistory (before : Nat → Nat) (history : List PredicatePublication) : Prop :=
  match history with
  | [] => True
  | event :: rest => (∀ bucket, before bucket < event.stamp) ∧
      FreshPredicateHistory (publishPredicateStamp before event) rest

def PredicateHistoryMonotone : Prop :=
  ∀ (before : Nat → Nat) (history : List PredicatePublication),
    FreshPredicateHistory before history →
    ∀ bucket, before bucket ≤ replayPredicatePublications before history bucket

def PredicateHistoryCoversEvents : Prop :=
  ∀ (before : Nat → Nat) (history : List PredicatePublication),
    FreshPredicateHistory before history →
    ∀ event ∈ history, ∀ bucket ∈ event.buckets,
      event.stamp ≤ replayPredicatePublications before history bucket

def AcceptedReadExcludesLaterPublication : Prop :=
  ∀ (before recorded : Nat → Nat) (history : List PredicatePublication)
    (reads : Set Nat) (transactionStart : Nat),
    FreshPredicateHistory before history →
    readDependenciesAccepted reads recorded (replayPredicatePublications before history)
      transactionStart →
    ∀ event ∈ history, transactionStart ≤ event.stamp → Disjoint reads event.buckets

/-- `some none` is a staged deletion; `none` means no local write. -/
def overlayOwnWrites (snapshot : Nat → Option Nat)
    (own : Nat → Option (Option Nat)) : Nat → Option Nat :=
  fun row => match own row with
    | none => snapshot row
    | some value => value

def rowMatches (rows : Nat → Option Nat) (predicate : Nat → Prop) (row : Nat) : Prop :=
  ∃ key, rows row = some key ∧ predicate key

def materializePredicate (snapshot : Nat → Option Nat) (own : Nat → Option (Option Nat))
    (predicate : Nat → Prop) (candidates : Set Nat) : Set Nat :=
  {row | (row ∈ candidates ∨ own row ≠ none) ∧
    rowMatches (overlayOwnWrites snapshot own) predicate row}

def OwnWritePredicateComplete : Prop :=
  ∀ (snapshot : Nat → Option Nat) (own : Nat → Option (Option Nat))
    (predicate : Nat → Prop) (candidates : Set Nat),
    (∀ row, rowMatches snapshot predicate row → row ∈ candidates) →
    materializePredicate snapshot own predicate candidates =
      {row | rowMatches (overlayOwnWrites snapshot own) predicate row}

end AerostoreProofs
