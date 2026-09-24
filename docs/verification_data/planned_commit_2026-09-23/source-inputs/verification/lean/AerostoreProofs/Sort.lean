import AerostoreProofs.BridgeContracts

open Aeneas Aeneas.Std Aeneas.Std.WP Result ControlFlow

namespace AerostoreProofs

theorem rust_sort_search_loop
    (output : alloc.vec.Vec Usize) (bucket position : Usize)
    (positionBound : position.val ≤ output.val.length)
    (before : ∀ j < position.val, output.val[j]!.val < bucket.val) :
    AerostoreExtracted.canonical_buckets_sort_loop0_loop0 output bucket position
      ⦃ found => found.val ≤ output.val.length ∧
        (∀ j < found.val, output.val[j]!.val < bucket.val) ∧
        (found.val < output.val.length → bucket.val ≤ output.val[found.val]!.val) ⦄ := by
  unfold AerostoreExtracted.canonical_buckets_sort_loop0_loop0
  apply loop.spec_decr_nat (fun cursor => output.val.length - cursor.val)
    (fun cursor => cursor.val ≤ output.val.length ∧
      ∀ j < cursor.val, output.val[j]!.val < bucket.val)
  · intro cursor ⟨cursorBound, earlier⟩
    by_cases within : cursor.val < output.val.length
    · have hindex : alloc.vec.Vec.index (core.slice.index.SliceIndexUsizeSlice Usize)
          output cursor = ok output.val[cursor.val]! := by
        simp [alloc.vec.Vec.index_usize, within]
      by_cases smaller : output.val[cursor.val]!.val < bucket.val
      · obtain ⟨next, hnext, nextVal⟩ := spec_imp_exists
          (Usize.add_spec (x := cursor) (y := 1#usize) (by scalar_tac))
        have nv : next.val = cursor.val + 1 := by simpa using nextVal
        have smallerIndex := smaller
        simp [within] at smallerIndex
        have computed :
            AerostoreExtracted.canonical_buckets_sort_loop0_loop0.body output bucket cursor =
              ok (cont next) := by
          simp [AerostoreExtracted.canonical_buckets_sort_loop0_loop0.body,
            within, hindex, smallerIndex, hnext]
        rw [computed]
        simp only [spec_ok]
        refine ⟨⟨by omega, ?_⟩, by omega⟩
        intro j hj
        by_cases old : j < cursor.val
        · exact earlier j old
        · have same : j = cursor.val := by omega
          simpa [same] using smaller
      · have smallerIndex := smaller
        simp [within] at smallerIndex
        have computed :
            AerostoreExtracted.canonical_buckets_sort_loop0_loop0.body output bucket cursor =
              ok (done cursor) := by
          simp [AerostoreExtracted.canonical_buckets_sort_loop0_loop0.body,
            within, hindex, smallerIndex]
        rw [computed]
        simp only [spec_ok]
        exact ⟨cursorBound, earlier, fun _ => by omega⟩
    · have computed :
          AerostoreExtracted.canonical_buckets_sort_loop0_loop0.body output bucket cursor =
            ok (done cursor) := by
        simp [AerostoreExtracted.canonical_buckets_sort_loop0_loop0.body, within]
      rw [computed]
      simp only [spec_ok]
      exact ⟨cursorBound, earlier, fun h => False.elim (within h)⟩
  · exact ⟨positionBound, before⟩

private def ShiftInvariant (original : List Usize) (position : Usize)
    (state : alloc.vec.Vec Usize × Usize) : Prop :=
  position.val ≤ state.2.val ∧ state.2.val ≤ original.length ∧
  state.1.val.length = original.length + 1 ∧
  (∀ j < state.2.val, state.1.val[j]! = original[j]!) ∧
  ∀ j, state.2.val < j → j < state.1.val.length →
    state.1.val[j]! = original[j - 1]!

private theorem shift_step
    (original : List Usize) (position shift : Usize) (output : alloc.vec.Vec Usize)
    (invariant : ShiftInvariant original position (output, shift))
    (processing : position.val < shift.val) :
    ∃ updated next,
      AerostoreExtracted.canonical_buckets_sort_loop0_loop1.body position output shift =
        ok (cont (updated, next)) ∧
      ShiftInvariant original position (updated, next) ∧ next.val < shift.val := by
  rcases invariant with ⟨lower, upper, length, prefixEq, suffix⟩
  dsimp only at lower upper length prefixEq suffix
  obtain ⟨next, hnext, nextVal⟩ := spec_imp_exists
    (Usize.sub_spec (x := shift) (y := 1#usize) (by scalar_tac))
  have nv : next.val = shift.val - 1 := by simpa using nextVal.1
  have nextBound : next.val < output.val.length := by omega
  have shiftBound : shift.val < output.val.length := by omega
  let updated := output.set shift output.val[next.val]!
  refine ⟨updated, next, ?_, ?_, by omega⟩
  · simp [AerostoreExtracted.canonical_buckets_sort_loop0_loop1.body,
      processing, hnext, alloc.vec.Vec.index_usize,
      alloc.vec.Vec.index_mut_usize, nextBound, shiftBound, updated]
  · refine ⟨by dsimp; omega, by dsimp; omega, ?_, ?_, ?_⟩
    · simpa [updated] using length
    · intro j hj
      dsimp only at hj ⊢
      have different : shift.val ≠ j := by omega
      simp only [updated, alloc.vec.Vec.set_val_eq, List.getElem!_eq_getElem?_getD,
        List.getElem?_set_ne different]
      simpa only [List.getElem!_eq_getElem?_getD] using prefixEq j (by omega)
    · intro j hj hjBound
      dsimp only at hj hjBound ⊢
      by_cases same : j = shift.val
      · subst j
        simpa [updated, shiftBound, nv] using prefixEq next.val (by omega)
      · have old : shift.val < j := by omega
        simpa only [updated, alloc.vec.Vec.set_val_eq,
          List.getElem!_eq_getElem?_getD, List.getElem?_set_ne (Ne.symm same)]
          using suffix j old (by simpa [updated] using hjBound)

theorem rust_sort_shift_loop
    (original : List Usize) (position shift : Usize) (output : alloc.vec.Vec Usize)
    (invariant : ShiftInvariant original position (output, shift)) :
    AerostoreExtracted.canonical_buckets_sort_loop0_loop1 output position shift
      ⦃ result => ShiftInvariant original position (result, position) ⦄ := by
  unfold AerostoreExtracted.canonical_buckets_sort_loop0_loop1
  apply loop.spec_decr_nat (fun state => state.2.val) (ShiftInvariant original position)
  · rintro ⟨out, cursor⟩ inv
    dsimp only
    by_cases processing : position.val < cursor.val
    · obtain ⟨updated, next, computed, nextInv, decreases⟩ :=
        shift_step original position cursor out inv processing
      rw [computed]
      simp only [spec_ok]
      exact ⟨nextInv, decreases⟩
    · have same : cursor = position := by
        apply UScalar.eq_of_val_eq
        have lower := inv.1
        dsimp at lower
        omega
      have computed :
          AerostoreExtracted.canonical_buckets_sort_loop0_loop1.body position out cursor =
            ok (done out) := by
        simp [AerostoreExtracted.canonical_buckets_sort_loop0_loop1.body, processing]
      rw [computed]
      simp only [spec_ok]
      simpa [same] using inv
  · exact invariant

private theorem shift_finish
    (original : List Usize) (position bucket : Usize) (output : alloc.vec.Vec Usize)
    (invariant : ShiftInvariant original position (output, position)) :
    (output.set position bucket).val = original.insertIdx position.val bucket := by
  rcases invariant with ⟨_, upper, length, prefixEq, suffix⟩
  dsimp only at upper length prefixEq suffix
  simp only [alloc.vec.Vec.set_val_eq]
  apply List.ext_getElem
  · simp [length, List.length_insertIdx, upper]
  · intro j hj hj'
    by_cases before : j < position.val
    · rw [List.getElem_insertIdx_of_lt before]
      have different : position.val ≠ j := by omega
      rw [List.getElem_set_ne different]
      simpa [show j < output.val.length by simpa using hj,
        show j < original.length by omega] using prefixEq j before
    · by_cases same : j = position.val
      · subst j
        rw [List.getElem_insertIdx_self, List.getElem_set_self]
      · have after : position.val < j := by omega
        rw [List.getElem_insertIdx_of_gt after, List.getElem_set_ne (Ne.symm same)]
        have bound : j < output.val.length := by simpa using hj
        simpa [bound, show j - 1 < original.length by omega] using suffix j after bound

private theorem ordered_insert
    (original : List Usize) (bucket : Usize) (position : Nat)
    (upper : position ≤ original.length)
    (ordered : original.Pairwise (fun a b => a.val < b.val))
    (earlier : ∀ j < position, original[j]!.val < bucket.val)
    (later : ∀ j, position ≤ j → j < original.length → bucket.val < original[j]!.val) :
    (original.insertIdx position bucket).Pairwise (fun a b => a.val < b.val) := by
  induction original generalizing position with
  | nil =>
      have zero : position = 0 := by simpa using upper
      simp [zero]
  | cons head tail ih =>
      have orderedTail := (List.pairwise_cons.mp ordered).2
      have headBefore := (List.pairwise_cons.mp ordered).1
      cases position with
      | zero =>
          rw [List.insertIdx_zero]
          apply List.pairwise_cons.mpr
          refine ⟨?_, ordered⟩
          intro value member
          obtain ⟨j, hj, valueEq⟩ := List.mem_iff_getElem.mp member
          subst value
          simpa only [_root_.getElem!_pos (head :: tail) j hj] using later j (by omega) hj
      | succ position =>
          simp only [List.insertIdx_succ_cons, List.pairwise_cons]
          refine ⟨?_, ih position (by simpa using upper) orderedTail ?_ ?_⟩
          · intro value member
            rw [List.mem_insertIdx (by simpa using upper)] at member
            rcases member with rfl | member
            · simpa using earlier 0 (by omega)
            · exact headBefore value member
          · intro j hj
            simpa using earlier (j + 1) (by omega)
          · intro j hj hjBound
            simpa using later (j + 1) (by omega) (by simpa using Nat.succ_lt_succ hjBound)

private theorem ordered_later
    (original : List Usize) (bucket : Usize) (position : Nat)
    (ordered : original.Pairwise (fun a b => a.val < b.val))
    (current : position < original.length → bucket.val < original[position]!.val) :
    ∀ j, position ≤ j → j < original.length → bucket.val < original[j]!.val := by
  intro j lower upper
  have positionBound : position < original.length := by omega
  by_cases same : position = j
  · simpa [same] using current positionBound
  · have step := List.pairwise_iff_getElem.mp ordered position j positionBound upper (by omega)
    have atPosition := current positionBound
    simp [positionBound] at atPosition
    simpa [upper] using Nat.lt_trans atPosition step

private theorem rust_sort_insertion
    (output : alloc.vec.Vec Usize) (position bucket : Usize)
    (upper : position.val ≤ output.val.length)
    (capacity : output.val.length < Usize.max) :
    ∃ pushed shift shifted,
      alloc.vec.Vec.push output bucket = ok pushed ∧
      alloc.vec.Vec.len pushed - 1#usize = ok shift ∧
      AerostoreExtracted.canonical_buckets_sort_loop0_loop1 pushed position shift = ok shifted ∧
      AerostoreExtracted.canonical_buckets_sort_loop0_loop2 pushed position shift = ok shifted ∧
      position.val < shifted.val.length ∧
      (shifted.set position bucket).val = output.val.insertIdx position.val bucket := by
  obtain ⟨pushed, hpush, pushedVal⟩ := spec_imp_exists
    (alloc.vec.Vec.push_spec output bucket capacity)
  obtain ⟨shift, hshift, shiftVal⟩ := spec_imp_exists
    (Usize.sub_spec (x := alloc.vec.Vec.len pushed) (y := 1#usize) (by
      simp [pushedVal]))
  have sv : shift.val = output.val.length := by
    simpa [pushedVal] using shiftVal.1
  have invariant : ShiftInvariant output.val position (pushed, shift) := by
    refine ⟨by dsimp; omega, by dsimp; omega, ?_, ?_, ?_⟩
    · simp [pushedVal]
    · intro j hj
      dsimp only at hj ⊢
      simp only [pushedVal]
      have bound : j < output.val.length := by omega
      simp [List.getElem!_eq_getElem?_getD, List.getElem?_append_left bound]
    · intro j hj hjBound
      dsimp only at hj hjBound ⊢
      simp only [pushedVal, List.length_append, List.length_singleton] at hjBound
      omega
  obtain ⟨shifted, hloop, finalInv⟩ := spec_imp_exists
    (rust_sort_shift_loop output.val position shift pushed invariant)
  have hloop2 : AerostoreExtracted.canonical_buckets_sort_loop0_loop2 pushed position shift =
      ok shifted := by
    simpa only [AerostoreExtracted.canonical_buckets_sort_loop0_loop1,
      AerostoreExtracted.canonical_buckets_sort_loop0_loop2,
      AerostoreExtracted.canonical_buckets_sort_loop0_loop1.body,
      AerostoreExtracted.canonical_buckets_sort_loop0_loop2.body] using hloop
  refine ⟨pushed, shift, shifted, hpush, hshift, hloop, hloop2, ?_,
    shift_finish output.val position bucket shifted finalInv⟩
  have finalLength := finalInv.2.2.1
  dsimp only at finalLength
  omega

private def SortInvariant (input : Slice Usize) (state : alloc.vec.Vec Usize × Usize) : Prop :=
  state.2.val ≤ input.val.length ∧ state.1.val.length ≤ state.2.val ∧
  state.1.val.Pairwise (fun a b => a.val < b.val) ∧
  ∀ b : Usize, b ∈ state.1.val ↔ b ∈ input.val.take state.2.val

private theorem sort_insert_invariant
    (input : Slice Usize) (output updated : alloc.vec.Vec Usize) (i next position : Usize)
    (invariant : SortInvariant input (output, i))
    (processing : i.val < input.val.length)
    (nextVal : next.val = i.val + 1)
    (upper : position.val ≤ output.val.length)
    (earlier : ∀ j < position.val, output.val[j]!.val < input.val[i.val].val)
    (later : ∀ j, position.val ≤ j → j < output.val.length →
      input.val[i.val].val < output.val[j]!.val)
    (updatedVal : updated.val = output.val.insertIdx position.val input.val[i.val]) :
    SortInvariant input (updated, next) := by
  rcases invariant with ⟨cursorBound, lengthBound, ordered, represented⟩
  dsimp only at cursorBound lengthBound ordered represented
  refine ⟨by dsimp; omega, ?_, ?_, ?_⟩
  · dsimp only
    rw [updatedVal, List.length_insertIdx_of_le_length upper]
    omega
  · dsimp only
    rw [updatedVal]
    exact ordered_insert output.val input.val[i.val] position.val upper ordered earlier later
  · intro b
    dsimp only
    rw [updatedVal, nextVal, List.take_succ_eq_append_getElem processing]
    simp only [List.mem_insertIdx upper, List.mem_append, List.mem_singleton, represented]
    exact or_comm

private theorem rust_sort_step
    (input : Slice Usize) (count i : Usize) (output : alloc.vec.Vec Usize)
    (invariant : SortInvariant input (output, i))
    (processing : i.val < input.val.length)
    (bucketBound : input.val[i.val].val < count.val) :
    ∃ updated next,
      AerostoreExtracted.canonical_buckets_sort_loop0.body input count output i =
        ok (cont (updated, next)) ∧
      SortInvariant input (updated, next) ∧ next.val = i.val + 1 := by
  have ⟨cursorBound, lengthBound, ordered, represented⟩ := invariant
  dsimp only at cursorBound lengthBound ordered represented
  obtain ⟨next, hnext, nextVal⟩ := spec_imp_exists
    (Usize.add_spec (x := i) (y := 1#usize) (by scalar_tac))
  have nv : next.val = i.val + 1 := by simpa using nextVal
  obtain ⟨position, hsearch, upper, earlier, current⟩ := spec_imp_exists
    (rust_sort_search_loop output input.val[i.val] 0#usize (by simp) (by simp))
  obtain ⟨pushed, shift, shifted, hpush, hshift, hloop1, hloop2, positionBound, inserted⟩ :=
    rust_sort_insertion output position input.val[i.val] upper (by scalar_tac)
  let updated := shifted.set position input.val[i.val]
  by_cases atEnd : position.val = output.val.length
  · have same : position = alloc.vec.Vec.len output := by
      apply UScalar.eq_of_val_eq
      simpa using atEnd
    have computed : AerostoreExtracted.canonical_buckets_sort_loop0.body input count output i =
        ok (cont (updated, next)) := by
      simp only [AerostoreExtracted.canonical_buckets_sort_loop0.body]
      simp [processing, Slice.index_usize, Nat.not_le.mpr bucketBound,
        hsearch, same, hpush, hshift, alloc.vec.Vec.index_mut_usize, hnext, updated]
      simp only [← same, hloop1, bind_tc_ok]
      simp [alloc.vec.Vec.index_usize, positionBound]
    refine ⟨updated, next, computed, ?_, nv⟩
    apply sort_insert_invariant input output updated i next position invariant processing nv
      upper earlier _ inserted
    intro j hj hjBound
    omega
  · have positionInBounds : position.val < output.val.length := by omega
    have differentPosition : position ≠ alloc.vec.Vec.len output := by
      intro same
      apply atEnd
      simpa using congrArg UScalar.val same
    have hindex : alloc.vec.Vec.index (core.slice.index.SliceIndexUsizeSlice Usize)
        output position = ok output.val[position.val]! := by
      simp [alloc.vec.Vec.index_usize, positionInBounds]
    by_cases sameBucket : output.val[position.val]! = input.val[i.val]
    · have member : input.val[i.val] ∈ output.val := by
        rw [← sameBucket]
        simp [positionInBounds]
      have computed : AerostoreExtracted.canonical_buckets_sort_loop0.body input count output i =
          ok (cont (output, next)) := by
        have sameIndexed : output.val[position.val] = input.val[i.val] := by
          simpa [positionInBounds] using sameBucket
        simp [AerostoreExtracted.canonical_buckets_sort_loop0.body, processing,
          Slice.index_usize, Nat.not_le.mpr bucketBound, hsearch, differentPosition,
          alloc.vec.Vec.index_usize, positionInBounds, sameIndexed, hnext]
      refine ⟨output, next, computed, ?_, nv⟩
      refine ⟨by dsimp; omega, by dsimp; omega, ordered, ?_⟩
      intro b
      dsimp only
      rw [nv, List.take_succ_eq_append_getElem processing]
      simp only [List.mem_append, List.mem_singleton]
      constructor
      · intro h
        exact Or.inl ((represented b).mp h)
      · rintro (h | rfl)
        · exact (represented b).mpr h
        · exact member
    · have differentIndexed : output.val[position.val] ≠ input.val[i.val] := by
        simpa [positionInBounds] using sameBucket
      have differentVals : output.val[position.val].val ≠ input.val[i.val].val :=
        fun same => differentIndexed (UScalar.eq_of_val_eq same)
      have computed : AerostoreExtracted.canonical_buckets_sort_loop0.body input count output i =
          ok (cont (updated, next)) := by
        simp [AerostoreExtracted.canonical_buckets_sort_loop0.body, processing,
          Slice.index_usize, Nat.not_le.mpr bucketBound, hsearch, differentPosition,
          alloc.vec.Vec.index_usize, positionInBounds, differentVals, hpush, hshift, hloop2,
          alloc.vec.Vec.index_mut_usize, positionBound, hnext, updated]
      refine ⟨updated, next, computed, ?_, nv⟩
      apply sort_insert_invariant input output updated i next position invariant processing nv
        upper earlier _ inserted
      apply ordered_later output.val input.val[i.val] position.val ordered
      intro _
      have lower := current positionInBounds
      have unequal : output.val[position.val]!.val ≠ input.val[i.val].val := by
        intro same
        exact sameBucket (UScalar.eq_of_val_eq same)
      omega

private def SortPost (input : Slice Usize) (count : Usize)
    (result : core.result.Result (alloc.vec.Vec Usize) Usize) : Prop :=
  ∃ output, result = core.result.Result.Ok output ∧
    Canonical (input.val.map UScalar.val) (output.val.map UScalar.val) ∧
    ∀ key ∈ output.val, key.val < count.val

theorem rust_sort_refinement : RustSortRefinement := by
  intro input count bounded
  have specification : AerostoreExtracted.canonical_buckets_sort input count
      ⦃ result => SortPost input count result ⦄ := by
    unfold AerostoreExtracted.canonical_buckets_sort
      AerostoreExtracted.canonical_buckets_sort_loop0
    apply loop.spec_decr_nat (fun state => input.val.length - state.2.val)
      (SortInvariant input)
    · rintro ⟨output, cursor⟩ invariant
      dsimp only
      by_cases processing : cursor.val < input.val.length
      · obtain ⟨updated, next, computed, updatedInv, nextVal⟩ :=
          rust_sort_step input count cursor output invariant processing
            (bounded input.val[cursor.val] (List.getElem_mem processing))
        rw [computed]
        simp only [spec_ok]
        exact ⟨updatedInv, by omega⟩
      · rcases invariant with ⟨cursorBound, _, ordered, represented⟩
        dsimp only at cursorBound ordered represented
        have atEnd : cursor.val = input.val.length := by omega
        have computed : AerostoreExtracted.canonical_buckets_sort_loop0.body
            input count output cursor = ok (done (core.result.Result.Ok output)) := by
          simp [AerostoreExtracted.canonical_buckets_sort_loop0.body, processing]
        rw [computed]
        simp only [spec_ok]
        have membership : ∀ b : Usize, b ∈ output.val ↔ b ∈ input.val := by
          simpa only [atEnd, List.take_length] using represented
        refine ⟨output, rfl, ?_, ?_⟩
        · constructor
          · intro key
            simp only [List.mem_map]
            constructor
            · rintro ⟨b, hb, eq⟩
              exact ⟨b, (membership b).mp hb, eq⟩
            · rintro ⟨b, hb, eq⟩
              exact ⟨b, (membership b).mpr hb, eq⟩
          · simpa only [List.pairwise_map] using ordered
        · intro b hb
          exact bounded b ((membership b).mp hb)
    · simp [SortInvariant]
  obtain ⟨result, computed, output, resultEq, canonical, boundedOut⟩ := spec_imp_exists specification
  exact ⟨output, resultEq ▸ computed, canonical, boundedOut⟩

theorem rust_sort_rejects_first_invalid : RustSortRejectsFirstInvalid := by
  intro input count bad inBounds invalid validPrefix
  have specification : AerostoreExtracted.canonical_buckets_sort input count
      ⦃ result => result = core.result.Result.Err input.val[bad] ⦄ := by
    unfold AerostoreExtracted.canonical_buckets_sort
      AerostoreExtracted.canonical_buckets_sort_loop0
    apply loop.spec_decr_nat (fun state => bad - state.2.val)
      (fun state => state.2.val ≤ bad ∧ SortInvariant input state)
    · rintro ⟨output, cursor⟩ ⟨cursorBound, invariant⟩
      dsimp only at cursorBound ⊢
      have processing : cursor.val < input.val.length := by omega
      by_cases before : cursor.val < bad
      · have keyInPrefix : input.val[cursor.val] ∈ input.val.take bad := by
          apply List.mem_take_iff_getElem.mpr
          exact ⟨cursor.val, by omega, rfl⟩
        obtain ⟨updated, next, computed, nextInv, nextVal⟩ :=
          rust_sort_step input count cursor output invariant processing
            (validPrefix input.val[cursor.val] keyInPrefix)
        rw [computed]
        simp only [spec_ok]
        exact ⟨⟨by omega, nextInv⟩, by omega⟩
      · have found : cursor.val = bad := by omega
        have computed : AerostoreExtracted.canonical_buckets_sort_loop0.body
            input count output cursor = ok (done (core.result.Result.Err input.val[bad])) := by
          simp [AerostoreExtracted.canonical_buckets_sort_loop0.body,
            Slice.index_usize, found, inBounds, invalid]
        rw [computed]
        simp only [spec_ok]
    · simp [SortInvariant]
  obtain ⟨result, computed, resultEq⟩ := spec_imp_exists specification
  exact resultEq ▸ computed

theorem rust_sort_total : RustSortTotal := by
  classical
  intro input count
  by_cases valid : ∀ key ∈ input.val, key.val < count.val
  · obtain ⟨output, computed, canonical, bounded⟩ := rust_sort_refinement input count valid
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
    have computed := rust_sort_rejects_first_invalid input count bad inBounds invalid validPrefix
    exact ⟨core.result.Result.Err input.val[bad], computed, bad, inBounds, rfl, invalid, validPrefix⟩

theorem rust_sort_production : RustSortProduction := by
  intro input
  obtain ⟨result, computed, outcome⟩ := rust_sort_total input 4096#usize
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
