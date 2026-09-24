import AerostoreProofs.Buckets
import AerostoreProofs.Bitmap
import AerostoreProofs.Sort

open Aeneas Aeneas.Std

namespace AerostoreProofs

theorem rust_bucket_implementations_equivalent : RustBucketImplementationsEquivalent := by
  intro input count
  obtain ⟨result, sortComputed, sortOutcome⟩ := rust_sort_total input count
  cases result with
  | Ok output =>
    obtain ⟨valid, sortCanonical, _⟩ := sortOutcome
    obtain ⟨bitmapOutput, bitmapComputed, bitmapCanonical, _⟩ :=
      rust_bitmap_refinement input count valid
    have mappedEqual := canonical_unique (input.val.map UScalar.val)
      (output.val.map UScalar.val) (bitmapOutput.val.map UScalar.val)
      sortCanonical bitmapCanonical
    have outputEqual : output = bitmapOutput := by
      apply alloc.vec.Vec.ext
      exact (List.map_inj_right (fun _ _ h => UScalar.eq_of_val_eq h)).mp mappedEqual
    rw [sortComputed, bitmapComputed, outputEqual]
  | Err badKey =>
    obtain ⟨badIndex, inBounds, keyEq, invalid, validPrefix⟩ := sortOutcome
    have bitmapComputed := rust_bitmap_rejects_first_invalid input count badIndex
      inBounds (by simpa only [keyEq] using invalid) validPrefix
    rw [sortComputed, bitmapComputed, keyEq]

end AerostoreProofs
