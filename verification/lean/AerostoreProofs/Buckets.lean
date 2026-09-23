import AerostoreProofs.Contracts

namespace AerostoreProofs

theorem bitmap_specification_correct : BucketSpecificationCorrect := by
  intro input count bounded
  constructor
  · intro key
    simp only [bitmapSpecification, List.mem_filter, List.mem_range, decide_eq_true_eq]
    exact ⟨fun h => h.2, fun h => ⟨bounded key h, h⟩⟩
  · exact List.pairwise_lt_range.filter _

theorem canonical_unique : CanonicalUnique := by
  intro input first second hfirst hsecond
  apply hfirst.2.eq_of_mem_iff hsecond.2
  intro key
  exact (hfirst.1 key).trans (hsecond.1 key).symm

theorem canonical_membership (input output : List Nat)
    (h : Canonical input output) (key : Nat) : key ∈ output ↔ key ∈ input := h.1 key

theorem canonical_no_duplicates (input output : List Nat)
    (h : Canonical input output) : output.Nodup := by
  exact List.Pairwise.imp Nat.ne_of_lt h.2

theorem canonical_preserves_bound (input output : List Nat) (bucketCount : Nat)
    (h : Canonical input output) (bounded : ∀ key ∈ input, key < bucketCount) :
    ∀ key ∈ output, key < bucketCount := by
  intro key member
  exact bounded key ((h.1 key).mp member)

end AerostoreProofs
