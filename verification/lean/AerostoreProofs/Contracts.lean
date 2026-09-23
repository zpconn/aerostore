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

end AerostoreProofs
