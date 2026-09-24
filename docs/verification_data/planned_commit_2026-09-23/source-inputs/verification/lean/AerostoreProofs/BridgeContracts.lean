import AerostoreProofs.AerostoreVerified
import AerostoreProofs.Contracts

open Aeneas Aeneas.Std

namespace AerostoreProofs

/-- Required refinement of the actual extracted Rust scalar helper. -/
def RustStampRefinement : Prop :=
  ∀ stamp transactionStart : U64,
    AerostoreExtracted.stamp_precedes_snapshot stamp transactionStart =
      Result.ok (decide (stamp.val < transactionStart.val))

/-- A partial, actual-code loop obligation: the bitmap marking loop preserves
    the exact set represented by presence flags after processing one more key.
    This is separate from total correctness of either full Rust bucket kernel. -/
def RustBitmapStepPreservesMembership : Prop :=
  ∀ (input : Slice Usize) (i : Usize) (present : alloc.vec.Vec Bool),
    i.val < input.val.length →
    (∀ k ∈ input.val, k.val < present.val.length) →
    (∀ b : Usize, b.val < present.val.length →
      (present.val[b.val]! = true ↔ b ∈ input.val.take i.val)) →
    ∃ nextI nextPresent,
      AerostoreExtracted.canonical_buckets_bitmap_loop0_loop1.body input i present =
        Result.ok (ControlFlow.cont (nextI, nextPresent)) ∧
      nextI.val = i.val + 1 ∧
      nextPresent.val.length = present.val.length ∧
      (∀ b : Usize, b.val < nextPresent.val.length →
        (nextPresent.val[b.val]! = true ↔ b ∈ input.val.take nextI.val))

/-- The complete extracted bitmap kernel terminates successfully for every
    in-range input and returns its exact set in strict ascending order.
    Allocator behavior is the pinned Aeneas Vec model, not a physical RAM bound. -/
def RustBitmapRefinement : Prop :=
  ∀ (input : Slice Usize) (bucketCount : Usize),
    (∀ key ∈ input.val, key.val < bucketCount.val) →
    ∃ output : alloc.vec.Vec Usize,
      AerostoreExtracted.canonical_buckets_bitmap input bucketCount =
        Result.ok (core.result.Result.Ok output) ∧
      Canonical (input.val.map UScalar.val) (output.val.map UScalar.val) ∧
      (∀ key ∈ output.val, key.val < bucketCount.val)

def RustSortRefinement : Prop :=
  ∀ (input : Slice Usize) (bucketCount : Usize),
    (∀ key ∈ input.val, key.val < bucketCount.val) →
    ∃ output : alloc.vec.Vec Usize,
      AerostoreExtracted.canonical_buckets_sort input bucketCount =
        Result.ok (core.result.Result.Ok output) ∧
      Canonical (input.val.map UScalar.val) (output.val.map UScalar.val) ∧
      (∀ key ∈ output.val, key.val < bucketCount.val)

def RustBitmapRejectsFirstInvalid : Prop :=
  ∀ (input : Slice Usize) (bucketCount : Usize) (bad : Nat)
    (inBounds : bad < input.val.length),
    bucketCount.val ≤ input.val[bad].val →
    (∀ key ∈ input.val.take bad, key.val < bucketCount.val) →
    AerostoreExtracted.canonical_buckets_bitmap input bucketCount =
      Result.ok (core.result.Result.Err input.val[bad])

/-- Full function specification in the Aeneas Vec model, including the first
    invalid input on failure. Arbitrary counts do not imply that Rust's byte
    capacity checks or the physical allocator always succeed. -/
def RustBitmapTotal : Prop :=
  ∀ (input : Slice Usize) (bucketCount : Usize),
    ∃ result,
      AerostoreExtracted.canonical_buckets_bitmap input bucketCount = Result.ok result ∧
      match result with
      | .Ok output =>
        (∀ key ∈ input.val, key.val < bucketCount.val) ∧
        Canonical (input.val.map UScalar.val) (output.val.map UScalar.val) ∧
        (∀ key ∈ output.val, key.val < bucketCount.val)
      | .Err badKey =>
        ∃ (badIndex : Nat) (inBounds : badIndex < input.val.length),
          badKey = input.val[badIndex] ∧ bucketCount.val ≤ badKey.val ∧
          (∀ key ∈ input.val.take badIndex, key.val < bucketCount.val)

/-- The production caller's fixed 4096-bucket instance, for every input,
    including invalid keys. Successful output has at most 4096 usize elements.
    This logical bound fits Rust's x86-64 Vec byte-capacity limit; successful
    physical allocation remains an assumption of the extraction models. -/
def RustBitmapProduction : Prop :=
  ∀ input : Slice Usize,
    ∃ result,
      AerostoreExtracted.canonical_buckets_bitmap input 4096#usize = Result.ok result ∧
      match result with
      | .Ok output =>
        (∀ key ∈ input.val, key.val < 4096) ∧
        Canonical (input.val.map UScalar.val) (output.val.map UScalar.val) ∧
        (∀ key ∈ output.val, key.val < 4096) ∧
        output.val.length ≤ 4096
      | .Err badKey =>
        ∃ (badIndex : Nat) (inBounds : badIndex < input.val.length),
          badKey = input.val[badIndex] ∧ 4096 ≤ badKey.val ∧
          (∀ key ∈ input.val.take badIndex, key.val < 4096)

def RustSortRejectsFirstInvalid : Prop :=
  ∀ (input : Slice Usize) (bucketCount : Usize) (bad : Nat)
    (inBounds : bad < input.val.length),
    bucketCount.val ≤ input.val[bad].val →
    (∀ key ∈ input.val.take bad, key.val < bucketCount.val) →
    AerostoreExtracted.canonical_buckets_sort input bucketCount =
      Result.ok (core.result.Result.Err input.val[bad])

/-- Sort result semantics in the pinned logical Vec model, with exact first
    invalid input. Physical allocation and byte-capacity behavior are external. -/
def RustSortTotal : Prop :=
  ∀ (input : Slice Usize) (bucketCount : Usize),
    ∃ result,
      AerostoreExtracted.canonical_buckets_sort input bucketCount = Result.ok result ∧
      match result with
      | .Ok output =>
        (∀ key ∈ input.val, key.val < bucketCount.val) ∧
        Canonical (input.val.map UScalar.val) (output.val.map UScalar.val) ∧
        (∀ key ∈ output.val, key.val < bucketCount.val)
      | .Err badKey =>
        ∃ (badIndex : Nat) (inBounds : badIndex < input.val.length),
          badKey = input.val[badIndex] ∧ bucketCount.val ≤ badKey.val ∧
          (∀ key ∈ input.val.take badIndex, key.val < bucketCount.val)

def RustSortProduction : Prop :=
  ∀ input : Slice Usize,
    ∃ result,
      AerostoreExtracted.canonical_buckets_sort input 4096#usize = Result.ok result ∧
      match result with
      | .Ok output =>
        (∀ key ∈ input.val, key.val < 4096) ∧
        Canonical (input.val.map UScalar.val) (output.val.map UScalar.val) ∧
        (∀ key ∈ output.val, key.val < 4096) ∧
        output.val.length ≤ 4096
      | .Err badKey =>
        ∃ (badIndex : Nat) (inBounds : badIndex < input.val.length),
          badKey = input.val[badIndex] ∧ 4096 ≤ badKey.val ∧
          (∀ key ∈ input.val.take badIndex, key.val < 4096)

/-- Exact equality of both extracted implementations' complete results, for all
    inputs and bucket counts in the pinned logical Vec model. -/
def RustBucketImplementationsEquivalent : Prop :=
  ∀ (input : Slice Usize) (bucketCount : Usize),
    AerostoreExtracted.canonical_buckets_sort input bucketCount =
      AerostoreExtracted.canonical_buckets_bitmap input bucketCount

end AerostoreProofs
