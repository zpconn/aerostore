import AerostoreProofs.BridgeContracts
import Mathlib.Data.Nat.Find

open Aeneas Aeneas.Std Aeneas.Std.WP Result ControlFlow

namespace AerostoreProofs

theorem rust_bitmap_initialization_loop
    (count : Usize) (present : alloc.vec.Vec Bool) (bucket : Usize)
    (hb : bucket.val ≤ count.val) (length : present.val.length = bucket.val)
    (empty : ∀ flag ∈ present.val, flag = false) :
    AerostoreExtracted.canonical_buckets_bitmap_loop0_loop0 count present bucket
      ⦃ result => result.val.length = count.val ∧ ∀ flag ∈ result.val, flag = false ⦄ := by
  unfold AerostoreExtracted.canonical_buckets_bitmap_loop0_loop0
  let inv := fun state : alloc.vec.Vec Bool × Usize =>
    state.2.val ≤ count.val ∧ state.1.val.length = state.2.val ∧
      ∀ flag ∈ state.1.val, flag = false
  apply loop.spec_decr_nat (fun state => count.val - state.2.val) inv
  · rintro ⟨flags, cursor⟩ ⟨cursorBound, flagsLength, flagsEmpty⟩
    dsimp only at cursorBound flagsLength flagsEmpty ⊢
    by_cases processing : cursor.val < count.val
    · obtain ⟨updated, hpush, updatedVal⟩ := spec_imp_exists
        (alloc.vec.Vec.push_spec flags false (by scalar_tac))
      obtain ⟨next, hnext, nextVal⟩ := spec_imp_exists
        (Usize.add_spec (x := cursor) (y := 1#usize) (by scalar_tac))
      have nv : next.val = cursor.val + 1 := by simpa using nextVal
      have computed :
          AerostoreExtracted.canonical_buckets_bitmap_loop0_loop0.body count flags cursor =
            ok (cont (updated, next)) := by
        simp [AerostoreExtracted.canonical_buckets_bitmap_loop0_loop0.body, processing, hpush, hnext]
      rw [computed]
      simp only [spec_ok]
      refine ⟨⟨?_, ?_, ?_⟩, ?_⟩
      · dsimp
        omega
      · dsimp
        simp [updatedVal, flagsLength, nv]
      · dsimp
        simpa [updatedVal] using flagsEmpty
      · omega
    · have doneAt : cursor.val = count.val := by omega
      have computed :
          AerostoreExtracted.canonical_buckets_bitmap_loop0_loop0.body count flags cursor =
            ok (done flags) := by
        simp [AerostoreExtracted.canonical_buckets_bitmap_loop0_loop0.body, processing]
      rw [computed]
      simp only [spec_ok]
      exact ⟨flagsLength.trans doneAt, flagsEmpty⟩
  · exact ⟨hb, length, empty⟩

theorem rust_bitmap_step_preserves_membership : RustBitmapStepPreservesMembership := by
  intro input i present hi bounded represented
  have indexBound := bounded input.val[i.val] (List.getElem_mem hi)
  obtain ⟨next, hnext, nextVal⟩ := spec_imp_exists
    (Usize.add_spec (x := i) (y := 1#usize) (by scalar_tac))
  refine ⟨next, present.set input.val[i.val] true, ?_, ?_, ?_, ?_⟩
  · simp [AerostoreExtracted.canonical_buckets_bitmap_loop0_loop1.body,
      hi, alloc.vec.Vec.index_mut_usize, Slice.index_usize,
      alloc.vec.Vec.index_usize, indexBound, hnext]
  · simpa using nextVal
  · simp
  · intro b hb
    simp only [alloc.vec.Vec.set_val_eq, List.length_set] at hb ⊢
    have nv : next.val = i.val + 1 := by simpa using nextVal
    rw [nv]
    rw [List.take_succ_eq_append_getElem hi]
    simp only [List.mem_append, List.mem_singleton]
    by_cases heq : b = input.val[i.val]
    · subst b
      simp [indexBound]
    · have hne : b.val ≠ input.val[i.val].val := fun h => heq (UScalar.eq_of_val_eq h)
      simp only [heq, or_false]
      simpa only [List.getElem!_eq_getElem?_getD, List.getElem?_set_ne hne.symm]
        using represented b hb

theorem rust_bitmap_marking_loop
    (input : Slice Usize) (i : Usize) (present : alloc.vec.Vec Bool)
    (hi : i.val ≤ input.val.length)
    (bounded : ∀ k ∈ input.val, k.val < present.val.length)
    (represented : ∀ b : Usize, b.val < present.val.length →
      (present.val[b.val]! = true ↔ b ∈ input.val.take i.val)) :
    AerostoreExtracted.canonical_buckets_bitmap_loop0_loop1 input i present
      ⦃ result => result.val.length = present.val.length ∧
        ∀ b : Usize, b.val < result.val.length →
          (result.val[b.val]! = true ↔ b ∈ input.val) ⦄ := by
  unfold AerostoreExtracted.canonical_buckets_bitmap_loop0_loop1
  let inv := fun state : Usize × alloc.vec.Vec Bool =>
    state.1.val ≤ input.val.length ∧ state.2.val.length = present.val.length ∧
    ∀ b : Usize, b.val < state.2.val.length →
      (state.2.val[b.val]! = true ↔ b ∈ input.val.take state.1.val)
  apply loop.spec_decr_nat (fun state => input.val.length - state.1.val) inv
  · rintro ⟨cursor, flags⟩ ⟨cursorBound, flagsLength, flagsRepresented⟩
    dsimp only at cursorBound flagsLength flagsRepresented ⊢
    by_cases processing : cursor.val < input.val.length
    · obtain ⟨next, updated, computed, nextVal, updatedLength, updatedRepresented⟩ :=
        rust_bitmap_step_preserves_membership input cursor flags processing
          (by simpa only [flagsLength] using bounded) flagsRepresented
      rw [computed]
      simp only [spec_ok]
      refine ⟨⟨?_, updatedLength.trans flagsLength, updatedRepresented⟩, ?_⟩
      · dsimp
        omega
      · omega
    · have atEnd : cursor.val = input.val.length := by omega
      have computed : AerostoreExtracted.canonical_buckets_bitmap_loop0_loop1.body input cursor flags =
          ok (done flags) := by
        simp [AerostoreExtracted.canonical_buckets_bitmap_loop0_loop1.body, processing]
      rw [computed]
      simp only [spec_ok]
      refine ⟨flagsLength, ?_⟩
      simpa only [atEnd, List.take_length] using flagsRepresented
  · exact ⟨hi, rfl, represented⟩

private theorem enumeration_step
    (flags : List Bool) (cursor : Usize) (output : List Usize)
    (ordered : output.Pairwise (fun a b => a.val < b.val))
    (earlier : ∀ b ∈ output, b.val < cursor.val)
    (represented : ∀ b : Usize, b ∈ output ↔ b.val < cursor.val ∧ flags[b.val]! = true) :
    let updated := if flags[cursor.val]! then output ++ [cursor] else output
    updated.Pairwise (fun a b => a.val < b.val) ∧
    (∀ b ∈ updated, b.val < cursor.val + 1) ∧
    (∀ b : Usize, b ∈ updated ↔ b.val < cursor.val + 1 ∧ flags[b.val]! = true) := by
  dsimp only
  split
  next present =>
    refine ⟨?_, ?_, ?_⟩
    · simpa only [List.pairwise_append, List.pairwise_singleton,
        List.mem_singleton, forall_eq] using And.intro ordered (And.intro trivial earlier)
    · intro b member
      simp only [List.mem_append, List.mem_singleton] at member
      rcases member with member | rfl
      · have := earlier b member
        omega
      · omega
    · intro b
      simp only [List.mem_append, List.mem_singleton, represented]
      by_cases same : b = cursor
      · subst b
        simp [present]
      · have different : b.val ≠ cursor.val := fun h => same (UScalar.eq_of_val_eq h)
        simp only [same, or_false]
        constructor <;> rintro ⟨h, flag⟩ <;> exact ⟨by omega, flag⟩
  next absent =>
    refine ⟨ordered, ?_, ?_⟩
    · intro b member
      have := earlier b member
      omega
    · intro b
      rw [represented]
      constructor
      · rintro ⟨h, flag⟩
        exact ⟨by omega, flag⟩
      · rintro ⟨h, flag⟩
        have different : b.val ≠ cursor.val := by
          intro same
          exact absent (same ▸ flag)
        exact ⟨by omega, flag⟩

theorem rust_bitmap_enumeration_loop
    (count : Usize) (flags : alloc.vec.Vec Bool) (bucket : Usize)
    (output : alloc.vec.Vec Usize)
    (hb : bucket.val ≤ count.val) (flagsLength : flags.val.length = count.val)
    (outLength : output.val.length ≤ bucket.val)
    (ordered : output.val.Pairwise (fun a b => a.val < b.val))
    (earlier : ∀ b ∈ output.val, b.val < bucket.val)
    (represented : ∀ b : Usize, b ∈ output.val ↔
      b.val < bucket.val ∧ flags.val[b.val]! = true) :
    AerostoreExtracted.canonical_buckets_bitmap_loop0_loop2 count flags bucket output
      ⦃ result => result.val.Pairwise (fun a b => a.val < b.val) ∧
        ∀ b : Usize, b ∈ result.val ↔ b.val < count.val ∧ flags.val[b.val]! = true ⦄ := by
  unfold AerostoreExtracted.canonical_buckets_bitmap_loop0_loop2
  let inv := fun state : Usize × alloc.vec.Vec Usize =>
    state.1.val ≤ count.val ∧ state.2.val.length ≤ state.1.val ∧
    state.2.val.Pairwise (fun a b => a.val < b.val) ∧
    (∀ b ∈ state.2.val, b.val < state.1.val) ∧
    ∀ b : Usize, b ∈ state.2.val ↔ b.val < state.1.val ∧ flags.val[b.val]! = true
  apply loop.spec_decr_nat (fun state => count.val - state.1.val) inv
  · rintro ⟨cursor, out⟩ ⟨cursorBound, lengthBound, outOrdered, outEarlier, outRepresented⟩
    dsimp only at cursorBound lengthBound outOrdered outEarlier outRepresented ⊢
    by_cases processing : cursor.val < count.val
    · have indexBound : cursor.val < flags.val.length := by omega
      obtain ⟨next, hnext, nextVal⟩ := spec_imp_exists
        (Usize.add_spec (x := cursor) (y := 1#usize) (by scalar_tac))
      have nv : next.val = cursor.val + 1 := by simpa using nextVal
      have invariantStep := enumeration_step flags.val cursor out.val outOrdered outEarlier outRepresented
      have hindex : alloc.vec.Vec.index (core.slice.index.SliceIndexUsizeSlice Bool) flags cursor =
          ok flags.val[cursor.val]! := by
        simp [alloc.vec.Vec.index_usize, indexBound]
      by_cases present : flags.val[cursor.val]! = true
      · obtain ⟨updated, hpush, updatedVal⟩ := spec_imp_exists
          (alloc.vec.Vec.push_spec out cursor (by scalar_tac))
        have computed : AerostoreExtracted.canonical_buckets_bitmap_loop0_loop2.body count flags cursor out =
            ok (cont (next, updated)) := by
          simp [AerostoreExtracted.canonical_buckets_bitmap_loop0_loop2.body,
            processing, hindex, present, hpush, hnext]
        rw [computed]
        simp only [spec_ok]
        simp [present] at invariantStep
        refine ⟨⟨?_, ?_, ?_, ?_, ?_⟩, ?_⟩
        · dsimp
          omega
        · dsimp
          simp only [updatedVal, List.length_append, List.length_singleton]
          omega
        · simpa only [updatedVal] using invariantStep.1
        · simpa [updatedVal, nv] using invariantStep.2.1
        · simpa [updatedVal, nv] using invariantStep.2.2
        · omega
      · have computed : AerostoreExtracted.canonical_buckets_bitmap_loop0_loop2.body count flags cursor out =
            ok (cont (next, out)) := by
          simp [AerostoreExtracted.canonical_buckets_bitmap_loop0_loop2.body,
            processing, hindex, present, hnext]
        rw [computed]
        simp only [spec_ok]
        simp [present] at invariantStep
        refine ⟨⟨?_, ?_, invariantStep.1, ?_, ?_⟩, ?_⟩
        · dsimp
          omega
        · dsimp
          omega
        · simpa [nv] using invariantStep.2.1
        · simpa [nv] using invariantStep.2.2
        · omega
    · have doneAt : cursor.val = count.val := by omega
      have computed : AerostoreExtracted.canonical_buckets_bitmap_loop0_loop2.body count flags cursor out =
          ok (done out) := by
        simp [AerostoreExtracted.canonical_buckets_bitmap_loop0_loop2.body, processing]
      rw [computed]
      simp only [spec_ok]
      exact ⟨outOrdered, by simpa only [doneAt] using outRepresented⟩
  · exact ⟨hb, outLength, ordered, earlier, represented⟩

private def BitmapPost (input : Slice Usize) (count : Usize)
    (result : core.result.Result (alloc.vec.Vec Usize) Usize) : Prop :=
  ∃ output, result = core.result.Result.Ok output ∧
    Canonical (input.val.map UScalar.val) (output.val.map UScalar.val) ∧
    ∀ key ∈ output.val, key.val < count.val

private theorem rust_bitmap_finish
    (input : Slice Usize) (count cursor : Usize)
    (atEnd : cursor.val = input.val.length)
    (bounded : ∀ key ∈ input.val, key.val < count.val) :
    AerostoreExtracted.canonical_buckets_bitmap_loop0.body input count cursor
      ⦃ result => match result with
        | .cont _ => False
        | .done value => BitmapPost input count value ⦄ := by
  obtain ⟨flags, hinit, flagsLength, flagsEmpty⟩ := spec_imp_exists
    (rust_bitmap_initialization_loop count (alloc.vec.Vec.new Bool) 0#usize
      (by simp) (by simp) (by simp))
  have representedEmpty : ∀ b : Usize, b.val < flags.val.length →
      (flags.val[b.val]! = true ↔ b ∈ input.val.take (0#usize).val) := by
    intro b hb
    have hf := flagsEmpty flags.val[b.val] (List.getElem_mem hb)
    simpa [hb] using hf
  obtain ⟨marked, hmark, markedLength, markedRep⟩ := spec_imp_exists
    (rust_bitmap_marking_loop input 0#usize flags (by simp)
      (by simpa only [flagsLength] using bounded) representedEmpty)
  obtain ⟨output, henum, outputOrdered, outputRep⟩ := spec_imp_exists
    (rust_bitmap_enumeration_loop count marked 0#usize (alloc.vec.Vec.new Usize)
      (by simp) (markedLength.trans flagsLength) (by simp) (by simp) (by simp) (by simp))
  have member : ∀ b : Usize, b ∈ output.val ↔ b ∈ input.val := by
    intro b
    rw [outputRep]
    constructor
    · rintro ⟨hb, flag⟩
      exact (markedRep b (by omega)).mp flag
    · intro h
      have hb := bounded b h
      exact ⟨hb, (markedRep b (by omega)).mpr h⟩
  have canonical : Canonical (input.val.map UScalar.val) (output.val.map UScalar.val) := by
    constructor
    · intro key
      simp only [List.mem_map]
      constructor
      · rintro ⟨b, hb, eq⟩
        exact ⟨b, (member b).mp hb, eq⟩
      · rintro ⟨b, hb, eq⟩
        exact ⟨b, (member b).mpr hb, eq⟩
    · simpa only [List.pairwise_map] using outputOrdered
  have computed : AerostoreExtracted.canonical_buckets_bitmap_loop0.body input count cursor =
      ok (done (core.result.Result.Ok output)) := by
    simp [AerostoreExtracted.canonical_buckets_bitmap_loop0.body, atEnd, hinit, hmark, henum]
  rw [computed]
  simp only [spec_ok]
  exact ⟨output, rfl, canonical, fun b hb => bounded b ((member b).mp hb)⟩

theorem rust_bitmap_refinement : RustBitmapRefinement := by
  intro input count bounded
  have specification : AerostoreExtracted.canonical_buckets_bitmap input count
      ⦃ result => BitmapPost input count result ⦄ := by
    unfold AerostoreExtracted.canonical_buckets_bitmap
      AerostoreExtracted.canonical_buckets_bitmap_loop0
    apply loop.spec_decr_nat (fun cursor => input.val.length - cursor.val)
      (fun cursor => cursor.val ≤ input.val.length)
    · intro cursor cursorBound
      by_cases processing : cursor.val < input.val.length
      · have keyBound := bounded input.val[cursor.val] (List.getElem_mem processing)
        obtain ⟨next, hnext, nextVal⟩ := spec_imp_exists
          (Usize.add_spec (x := cursor) (y := 1#usize) (by scalar_tac))
        have nv : next.val = cursor.val + 1 := by simpa using nextVal
        have computed : AerostoreExtracted.canonical_buckets_bitmap_loop0.body input count cursor =
            ok (cont next) := by
          simp [AerostoreExtracted.canonical_buckets_bitmap_loop0.body, processing,
            Slice.index_usize, hnext, Nat.not_le.mpr keyBound]
        rw [computed]
        simp only [spec_ok]
        constructor <;> omega
      · have doneAt : cursor.val = input.val.length := by omega
        have finished := rust_bitmap_finish input count cursor doneAt bounded
        apply WP.spec_mono finished
        intro result post
        cases result with
        | cont _ => exact False.elim post
        | done _ => exact post
    · simp
  obtain ⟨result, computed, output, resultEq, canonical, boundedOut⟩ := spec_imp_exists specification
  exact ⟨output, resultEq ▸ computed, canonical, boundedOut⟩

theorem rust_bitmap_rejects_first_invalid : RustBitmapRejectsFirstInvalid := by
  intro input count bad inBounds invalid validPrefix
  have specification : AerostoreExtracted.canonical_buckets_bitmap input count
      ⦃ result => result = core.result.Result.Err input.val[bad] ⦄ := by
    unfold AerostoreExtracted.canonical_buckets_bitmap
      AerostoreExtracted.canonical_buckets_bitmap_loop0
    apply loop.spec_decr_nat (fun cursor => bad - cursor.val) (fun cursor => cursor.val ≤ bad)
    · intro cursor cursorBound
      have processing : cursor.val < input.val.length := by omega
      by_cases before : cursor.val < bad
      · have keyInPrefix : input.val[cursor.val] ∈ input.val.take bad := by
          apply List.mem_take_iff_getElem.mpr
          exact ⟨cursor.val, by omega, rfl⟩
        have keyBound := validPrefix input.val[cursor.val] keyInPrefix
        obtain ⟨next, hnext, nextVal⟩ := spec_imp_exists
          (Usize.add_spec (x := cursor) (y := 1#usize) (by scalar_tac))
        have nv : next.val = cursor.val + 1 := by simpa using nextVal
        have computed : AerostoreExtracted.canonical_buckets_bitmap_loop0.body input count cursor =
            ok (cont next) := by
          simp [AerostoreExtracted.canonical_buckets_bitmap_loop0.body, processing,
            Slice.index_usize, hnext, Nat.not_le.mpr keyBound]
        rw [computed]
        simp only [spec_ok]
        constructor <;> omega
      · have found : cursor.val = bad := by omega
        have computed : AerostoreExtracted.canonical_buckets_bitmap_loop0.body input count cursor =
            ok (done (core.result.Result.Err input.val[bad])) := by
          simp [AerostoreExtracted.canonical_buckets_bitmap_loop0.body,
            Slice.index_usize, found, inBounds, invalid]
        rw [computed]
        simp only [spec_ok]
    · simp
  obtain ⟨result, computed, resultEq⟩ := spec_imp_exists specification
  exact resultEq ▸ computed

theorem rust_bitmap_total : RustBitmapTotal := by
  classical
  intro input count
  by_cases valid : ∀ key ∈ input.val, key.val < count.val
  · obtain ⟨output, computed, canonical, bounded⟩ := rust_bitmap_refinement input count valid
    exact ⟨core.result.Result.Ok output, computed, valid, canonical, bounded⟩
  · have badExists : ∃ index : Nat, ∃ inBounds : index < input.val.length,
        count.val ≤ input.val[index].val := by
      push Not at valid
      obtain ⟨key, member, invalid⟩ := valid
      obtain ⟨index, inBounds, same⟩ := List.mem_iff_getElem.mp member
      refine ⟨index, inBounds, ?_⟩
      simpa only [same] using invalid
    let bad := Nat.find badExists
    obtain ⟨inBounds, invalid⟩ := Nat.find_spec badExists
    have validPrefix : ∀ key ∈ input.val.take bad, key.val < count.val := by
      intro key member
      obtain ⟨index, indexBound, same⟩ := List.mem_take_iff_getElem.mp member
      by_contra notValid
      have noEarlier := Nat.find_min badExists (m := index) (by omega)
      apply noEarlier
      exact ⟨by omega, by simpa only [same] using Nat.le_of_not_gt notValid⟩
    have computed := rust_bitmap_rejects_first_invalid input count bad inBounds invalid validPrefix
    exact ⟨core.result.Result.Err input.val[bad], computed, bad, inBounds, rfl, invalid, validPrefix⟩

theorem rust_bitmap_production : RustBitmapProduction := by
  intro input
  obtain ⟨result, computed, outcome⟩ := rust_bitmap_total input 4096#usize
  refine ⟨result, computed, ?_⟩
  cases result with
  | Err badKey => exact outcome
  | Ok output =>
    obtain ⟨valid, canonical, bounded⟩ := outcome
    refine ⟨valid, canonical, bounded, ?_⟩
    have distinct : (output.val.map UScalar.val).Nodup := canonical.2.nodup
    have subset : output.val.map UScalar.val ⊆ List.range 4096 := by
      intro key mappedMember
      obtain ⟨value, valueMember, same⟩ := List.mem_map.mp mappedMember
      rw [← same]
      exact List.mem_range.mpr (bounded value valueMember)
    have size := (List.subperm_of_subset distinct subset).length_le
    simpa only [List.length_map, List.length_range] using size

end AerostoreProofs
