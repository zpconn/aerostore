import AerostoreProofs.Contracts

namespace AerostoreProofs

theorem reject_publication_after_start (stamp transactionStart : Nat)
    (h : transactionStart ≤ stamp) : ¬ accepted stamp transactionStart := by
  exact Nat.not_lt_of_ge h

/-- A later stamp cannot make a rejected historical snapshot acceptable. -/
theorem rejection_persists : RejectionPersists := by
  intro oldStamp newStamp transactionStart changed rejected
  simp only [accepted] at *
  omega

/-- Counterexample to substituting writer start time for fresh publication time. -/
theorem writer_start_is_not_publication_stamp :
    accepted 1 2 ∧ ¬ accepted 3 2 := by unfold accepted; decide

end AerostoreProofs
