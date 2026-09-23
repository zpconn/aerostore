import AerostoreProofs.Lifecycle

namespace AerostoreProofs

/-- Snapshot fields have the same meaning as the native xmin/xmax/active set.
    The transaction's own pending versions are materialized separately. -/
structure QuerySnapshot where
  reader : Nat
  xmin : Nat
  xmax : Nat
  active : Set Nat

def queryCreatorVisible (s : QuerySnapshot) (creator : Nat) : Prop :=
  creator = s.reader ∨ creator < s.xmin ∨ (creator < s.xmax ∧ creator ∉ s.active)

/-- The actual native branch order, including the own-creator early return. -/
def queryVersionVisible (s : QuerySnapshot) (creator deleter : Nat) : Prop :=
  creator = s.reader ∨
    ((creator < s.xmin ∨ (creator < s.xmax ∧ creator ∉ s.active)) ∧
      (deleter = 0 ∨ (deleter ≠ s.reader ∧ (s.xmax ≤ deleter ∨ deleter ∈ s.active))))

/-- A keyless row is represented by `none`; this includes logical index deletion,
    without claiming the native table physically deletes the row. -/
structure QueryEvent where
  row : Nat
  beforeKey : Option Nat
  afterKey : Option Nat
  writer : Nat
  creator : Nat
  stamp : Nat

def queryKeyMatches (predicate : Nat → Prop) (value : Option Nat) : Prop :=
  ∃ key, value = some key ∧ predicate key

def queryEventTouches (predicate : Nat → Prop) (event : QueryEvent) : Prop :=
  queryKeyMatches predicate event.beforeKey ∨ queryKeyMatches predicate event.afterKey

def queryEventBuckets (bucket : Nat → Nat) (event : QueryEvent) : Set Nat :=
  {b | ∃ key, (event.beforeKey = some key ∨ event.afterKey = some key) ∧ bucket key = b}

def queryStampEvent (bucket : Nat → Nat) (event : QueryEvent) : PredicatePublication :=
  ⟨queryEventBuckets bucket event, event.stamp⟩

/-- Only overlapping buckets must serialize their reservations/stores. Multiple
    row changes from one transaction may share an equal label. This
    permits disjoint physical stores to occur out of global reservation order. -/
def GuardedStampHistory (before : Nat → Nat) (history : List PredicatePublication) : Prop :=
  match history with
  | [] => True
  | event :: rest => (∀ b ∈ event.buckets, before b ≤ event.stamp) ∧
      GuardedStampHistory (publishPredicateStamp before event) rest

theorem guarded_stamp_history_monotone (before : Nat → Nat) (history : List PredicatePublication)
    (valid : GuardedStampHistory before history) :
    ∀ b, before b ≤ replayPredicatePublications before history b := by
  induction history generalizing before with
  | nil => intro b; exact Nat.le_refl _
  | cons event rest ih =>
    intro b
    apply Nat.le_trans _ (ih _ valid.2 b)
    unfold publishPredicateStamp
    split_ifs with touched
    · exact valid.1 b touched
    · exact Nat.le_refl _

def GuardedStampHistoryCovers : Prop :=
  ∀ (before : Nat → Nat) (history : List PredicatePublication)
    (_valid : GuardedStampHistory before history),
∀ event ∈ history, ∀ b ∈ event.buckets,
      event.stamp ≤ replayPredicatePublications before history b

theorem guarded_stamp_history_covers : GuardedStampHistoryCovers := by
  intro before history valid
  induction history generalizing before with
  | nil => simp
  | cons first rest ih =>
    intro event member b touched
    rcases List.mem_cons.mp member with same | member
    · subst event
      have later := guarded_stamp_history_monotone (publishPredicateStamp before first) rest valid.2 b
      simpa [replayPredicatePublications, publishPredicateStamp, touched] using later
    · exact ih _ valid.2 event member b touched

theorem lifecycle_unreserved_is_unused {initialClock : Nat} {state : LifecycleState}
    (reachable : LifecycleReachable initialClock state) :
    ∀ tx, state.starts tx = none → tx ∉ state.active ∧ tx ∉ state.finished ∧ state.publication tx = none := by
  induction reachable with
  | initial => simp [lifecycleInitial]
  | @step before after reachable step ih =>
    intro tx unused
    cases step with
    | reserve who absent =>
      by_cases same : tx = who
      · subst tx; simp at unused
      · simp only [Function.update_of_ne same] at unused
        exact ih tx unused
    | register who start reserved notFinished notPublished =>
      have old := ih tx unused
      refine ⟨?_, old.2⟩
      intro active
      rcases Set.mem_insert_iff.mp active with same | active
      · subst tx; rw [unused] at reserved; contradiction
      · exact old.1 active
    | snapshot => exact ih tx unused
    | finish who registered =>
      have old := ih tx unused
      refine ⟨fun active => old.1 active.1, ?_, old.2.2⟩
      intro finished
      rcases Set.mem_insert_iff.mp finished with same | finished
      · subst tx; exact old.1 registered
      · exact old.2.1 finished
    | publish who ended notPublished =>
      have old := ih tx unused
      have different : tx ≠ who := by
        intro same; subst tx; exact old.2.1 ended
      exact ⟨old.1, old.2.1, by simpa [different] using old.2.2⟩

/-- A writer's publication label follows its own start, independently of whether
    any reader happened to observe that writer in flight. -/
def QueryPublicationFollowsWriter : Prop :=
  ∀ {initialClock : Nat} {state : LifecycleState}
    (_reachable : LifecycleReachable initialClock state),
∀ writer creator stamp, state.starts writer = some creator →
      state.publication writer = some stamp → creator < stamp

theorem lifecycle_publication_follows_writer : QueryPublicationFollowsWriter := by
  intro initialClock state reachable
  induction reachable with
  | initial => simp [lifecycleInitial]
  | @step before after reachable step ih =>
    intro writer creator stamp started published
    cases step with
    | reserve tx unused =>
      by_cases same : writer = tx
      · subst writer
        have absent := (lifecycle_unreserved_is_unused reachable tx unused).2.2
        simp [absent] at published
      · simp only [Function.update_of_ne same] at started
        exact ih writer creator stamp started published
    | register => exact ih writer creator stamp started published
    | snapshot => exact ih writer creator stamp started published
    | finish => exact ih writer creator stamp started published
    | publish tx ended notPublished =>
      by_cases same : writer = tx
      · subst writer
        simp at published
        subst stamp
        exact (lifecycle_reachable_invariant reachable).1 tx creator started
      · simp only [Function.update_of_ne same] at published
        exact ih writer creator stamp started published

def QueryLifecycleMapping (state : LifecycleState) (readerIdentity : Nat)
    (snapshot : QuerySnapshot) (event : QueryEvent) : Prop :=
  state.starts readerIdentity = some snapshot.reader ∧
  state.starts event.writer = some event.creator ∧
  state.publication event.writer = some event.stamp ∧
  (event.creator ∈ snapshot.active → (readerIdentity, event.writer) ∈ state.observed)

/-- Freshness comes from the reachable lifecycle history and the actual snapshot
    bounds/active-writer mapping, rather than a candidate-completeness premise. -/
def QuerySubreaderPublicationVisible : Prop :=
  ∀ {initialClock : Nat} {state : LifecycleState}
    (_reachable : LifecycleReachable initialClock state) (readerIdentity : Nat)
    (snapshot : QuerySnapshot) (event : QueryEvent)
    (_mapping : QueryLifecycleMapping state readerIdentity snapshot event)
    (_readerBound : snapshot.reader < snapshot.xmax) (_earlier : event.stamp < snapshot.reader),
queryCreatorVisible snapshot event.creator

theorem query_subreader_publication_visible : QuerySubreaderPublicationVisible := by
  intro initialClock state reachable readerIdentity snapshot event mapping readerBound earlier
  have creatorBefore := lifecycle_publication_follows_writer reachable event.writer event.creator event.stamp
    mapping.2.1 mapping.2.2.1
  have inactive : event.creator ∉ snapshot.active := by
    intro active
    have late := lifecycle_late_publication initialClock state reachable readerIdentity event.writer
      snapshot.reader event.stamp (mapping.2.2.2 active) mapping.1 mapping.2.2.1
    omega
  exact Or.inr (Or.inr ⟨by omega, inactive⟩)

def queryUpdate (rows : Nat → Option Nat) (event : QueryEvent) : Nat → Option Nat :=
  Function.update rows event.row event.afterKey

def queryLiveReplay (rows : Nat → Option Nat) (history : List QueryEvent) : Nat → Option Nat :=
  match history with
  | [] => rows
  | event :: rest => queryLiveReplay (queryUpdate rows event) rest

noncomputable def querySnapshotReplay (snapshot : QuerySnapshot) (rows : Nat → Option Nat)
    (history : List QueryEvent) : Nat → Option Nat :=
  match history with
  | [] => rows
  | event :: rest => querySnapshotReplay snapshot
      (@ite (Nat → Option Nat) (queryCreatorVisible snapshot event.creator)
        (Classical.propDecidable _) (queryUpdate rows event) rows) rest

def CoherentQueryHistory (rows : Nat → Option Nat) (history : List QueryEvent) : Prop :=
  match history with
  | [] => True
  | event :: rest => event.beforeKey = rows event.row ∧ CoherentQueryHistory (queryUpdate rows event) rest

def queryInitialPostings (rows : Nat → Option Nat) : Set (Nat × Nat) :=
  {pair | rows pair.2 = some pair.1}

/-- Exact old-key removal/new-key insertion, not an assumed complete result set. -/
def queryUpdatePostings (postings : Set (Nat × Nat)) (event : QueryEvent) : Set (Nat × Nat) :=
  (postings \ {pair | pair.2 = event.row ∧ event.beforeKey = some pair.1}) ∪
    {pair | pair.2 = event.row ∧ event.afterKey = some pair.1}

def queryPostingReplay (postings : Set (Nat × Nat)) (history : List QueryEvent) : Set (Nat × Nat) :=
  match history with
  | [] => postings
  | event :: rest => queryPostingReplay (queryUpdatePostings postings event) rest

theorem query_posting_step_exact (rows : Nat → Option Nat) (event : QueryEvent)
    (coherent : event.beforeKey = rows event.row) :
    queryUpdatePostings (queryInitialPostings rows) event = queryInitialPostings (queryUpdate rows event) := by
  ext pair
  by_cases same : pair.2 = event.row
  · simp [queryUpdatePostings, queryInitialPostings, queryUpdate, same, coherent]
  · simp [queryUpdatePostings, queryInitialPostings, queryUpdate, same]

def QueryPostingReplayExact : Prop :=
  ∀ (rows : Nat → Option Nat) (history : List QueryEvent)
    (_coherent : CoherentQueryHistory rows history),
queryPostingReplay (queryInitialPostings rows) history =
      queryInitialPostings (queryLiveReplay rows history)

theorem query_posting_replay_exact : QueryPostingReplayExact := by
  intro rows history coherent
  induction history generalizing rows with
  | nil => rfl
  | cons event rest ih =>
    simp only [queryPostingReplay, queryLiveReplay]
    rw [query_posting_step_exact rows event coherent.1]
    exact ih _ coherent.2

theorem query_replay_preserves_matching (snapshot : QuerySnapshot) (predicate : Nat → Prop)
    (live oldSnapshot : Nat → Option Nat) (history : List QueryEvent)
    (coherent : CoherentQueryHistory live history)
    (initial : ∀ row, queryKeyMatches predicate (live row) ↔ queryKeyMatches predicate (oldSnapshot row))
    (visible : ∀ event ∈ history, queryEventTouches predicate event → queryCreatorVisible snapshot event.creator) :
    ∀ row, queryKeyMatches predicate (queryLiveReplay live history row) ↔
      queryKeyMatches predicate (querySnapshotReplay snapshot oldSnapshot history row) := by
  classical
  induction history generalizing live oldSnapshot with
  | nil => exact initial
  | cons event rest ih =>
    apply ih _ _ coherent.2
    · intro row
      by_cases seen : queryCreatorVisible snapshot event.creator
      · simp only [seen, ↓reduceIte]
        by_cases same : row = event.row
        · simp [queryUpdate, same]
        · simpa [queryUpdate, same] using initial row
      · simp only [seen, ↓reduceIte]
        have irrelevant : ¬ queryEventTouches predicate event := fun touches => seen (visible event (by simp) touches)
        by_cases same : row = event.row
        · subst row
          have beforeFalse : ¬ queryKeyMatches predicate (live event.row) := by
            rw [← coherent.1]; exact fun before => irrelevant (Or.inl before)
          have afterFalse : ¬ queryKeyMatches predicate event.afterKey := fun after => irrelevant (Or.inr after)
          have snapshotFalse : ¬ queryKeyMatches predicate (oldSnapshot event.row) := fun old => beforeFalse ((initial event.row).mpr old)
          simp [queryUpdate, afterFalse, snapshotFalse]
        · simpa [queryUpdate, same] using initial row
    · intro next member; exact visible next (List.mem_cons_of_mem _ member)

def queryCandidates (predicate : Nat → Prop) (postings : Set (Nat × Nat)) : Set Nat :=
  {row | ∃ key, (key, row) ∈ postings ∧ predicate key}

def queryOwnCandidates (candidates : Set Nat) (own : Nat → Option (Option Nat)) : Set Nat :=
  candidates ∪ {row | own row ≠ none}

def queryMaterialize (snapshot : Nat → Option Nat) (own : Nat → Option (Option Nat))
    (predicate : Nat → Prop) (candidates : Set Nat) : Set Nat :=
  {row | row ∈ queryOwnCandidates candidates own ∧ rowMatches (overlayOwnWrites snapshot own) predicate row}

/-- A complete-or-retry result for coherent abstract publication histories.
    Native pointer retention and mapping concurrent executions into this history
    remain separate obligations. Raw lookup must enumerate the maintained postings,
    not be assumed to enumerate historical snapshot rows. -/
def QuerySuccessComplete : Prop :=
  ∀ (initialClock : Nat) (state : LifecycleState) (_reachable : LifecycleReachable initialClock state)
    (readerIdentity : Nat) (snapshot : QuerySnapshot) (_readerBound : snapshot.reader < snapshot.xmax)
    (initialRows : Nat → Option Nat) (history : List QueryEvent)
    (predicate : Nat → Prop) (bucket : Nat → Nat) (readBuckets : Set Nat) (initialStamps : Nat → Nat)
    (own : Nat → Option (Option Nat)) (extraCandidates : Set Nat)
    (_coherent : CoherentQueryHistory initialRows history)
    (_mapping : ∀ event ∈ history, QueryLifecycleMapping state readerIdentity snapshot event)
    (_bucketCoverage : ∀ key, predicate key → bucket key ∈ readBuckets)
    (_stampHistory : GuardedStampHistory initialStamps (history.map (queryStampEvent bucket)))
    (_accepted : ∀ b ∈ readBuckets,
      replayPredicatePublications initialStamps (history.map (queryStampEvent bucket)) b < snapshot.reader),
queryMaterialize (querySnapshotReplay snapshot initialRows history) own predicate
      (queryCandidates predicate (queryPostingReplay (queryInitialPostings initialRows) history) ∪ extraCandidates) =
    {row | rowMatches (overlayOwnWrites (querySnapshotReplay snapshot initialRows history) own) predicate row}

theorem query_success_complete : QuerySuccessComplete := by
  intro initialClock state reachable readerIdentity snapshot readerBound initialRows history predicate bucket readBuckets initialStamps own extraCandidates coherent mapping bucketCoverage stampHistory accepted
  have visible : ∀ event ∈ history, queryEventTouches predicate event → queryCreatorVisible snapshot event.creator := by
    intro event member touched
    obtain ⟨key, inEvent, keyMatches⟩ : ∃ key, (event.beforeKey = some key ∨ event.afterKey = some key) ∧ predicate key := by
      rcases touched with ⟨key, before, keyMatches⟩ | ⟨key, after, keyMatches⟩
      · exact ⟨key, Or.inl before, keyMatches⟩
      · exact ⟨key, Or.inr after, keyMatches⟩
    have covered := guarded_stamp_history_covers initialStamps _ stampHistory
      (queryStampEvent bucket event) (List.mem_map_of_mem member) (bucket key) ⟨key, inEvent, rfl⟩
    have beforeReader := accepted (bucket key) (bucketCoverage key keyMatches)
    exact query_subreader_publication_visible reachable readerIdentity snapshot event (mapping event member)
      readerBound (by change event.stamp ≤ _ at covered; omega)
  have equalMatches := query_replay_preserves_matching snapshot predicate initialRows initialRows history coherent
    (fun _ => Iff.rfl) visible
  change materializePredicate _ _ _ _ = _
  apply own_write_predicate_complete
  intro row matching
  rw [query_posting_replay_exact initialRows history coherent]
  exact Or.inl ((equalMatches row).mpr matching)

/-- Missing a matching historical row in current postings forces a captured
    bucket to reject. Candidate completeness is the conclusion, not a premise. -/
def QueryMissingCandidateRetries : Prop :=
  ∀ (initialClock : Nat) (state : LifecycleState) (_reachable : LifecycleReachable initialClock state)
    (readerIdentity : Nat) (snapshot : QuerySnapshot) (_readerBound : snapshot.reader < snapshot.xmax)
    (initialRows : Nat → Option Nat) (history : List QueryEvent)
    (predicate : Nat → Prop) (bucket : Nat → Nat) (readBuckets : Set Nat) (initialStamps : Nat → Nat)
    (_coherent : CoherentQueryHistory initialRows history)
    (_mapping : ∀ event ∈ history, QueryLifecycleMapping state readerIdentity snapshot event)
    (_bucketCoverage : ∀ key, predicate key → bucket key ∈ readBuckets)
    (_stampHistory : GuardedStampHistory initialStamps (history.map (queryStampEvent bucket)))
    (row : Nat) (_matching : rowMatches (querySnapshotReplay snapshot initialRows history) predicate row)
    (_missing : row ∉ queryCandidates predicate (queryPostingReplay (queryInitialPostings initialRows) history)),
∃ b ∈ readBuckets, snapshot.reader ≤
      replayPredicatePublications initialStamps (history.map (queryStampEvent bucket)) b

theorem query_missing_candidate_retries : QueryMissingCandidateRetries := by
  intro initialClock state reachable readerIdentity snapshot readerBound initialRows history predicate bucket readBuckets initialStamps coherent mapping bucketCoverage stampHistory row matching missing
  classical
  by_contra noRetry
  have accepted : ∀ b ∈ readBuckets,
      replayPredicatePublications initialStamps (history.map (queryStampEvent bucket)) b < snapshot.reader := by
    intro b member
    apply Nat.lt_of_not_ge
    intro newer
    exact noRetry ⟨b, member, newer⟩
  have complete := query_success_complete initialClock state reachable readerIdentity snapshot readerBound
    initialRows history predicate bucket readBuckets initialStamps (fun _ => none) ∅
    coherent mapping bucketCoverage stampHistory accepted
  have result : row ∈ queryMaterialize (querySnapshotReplay snapshot initialRows history)
      (fun _ => none) predicate
      (queryCandidates predicate (queryPostingReplay (queryInitialPostings initialRows) history) ∪ ∅) := by
    rw [complete]
    simpa [rowMatches, overlayOwnWrites] using matching
  exact missing (by simpa [queryMaterialize, queryOwnCandidates] using result.1)

/-- Exact two-version branch correspondence for the declared single-writer
    slice. It explains why an invisible new version leaves its old version
    readable after guards are released, provided that old version is retained. -/
def QueryTwoVersionSelection : Prop :=
  ∀ (snapshot : QuerySnapshot) (base writer : Nat)
    (_positive : 0 < writer) (_foreignWriter : writer ≠ snapshot.reader)
    (_foreignBase : base ≠ snapshot.reader) (_baseOld : base < snapshot.xmin)
    (_bounds : snapshot.xmin ≤ snapshot.xmax)
    (_activeBound : ∀ tx ∈ snapshot.active, snapshot.xmin ≤ tx),
(queryVersionVisible snapshot writer 0 ↔ queryCreatorVisible snapshot writer) ∧
    (queryVersionVisible snapshot base writer ↔ ¬ queryCreatorVisible snapshot writer)

theorem query_two_version_selection : QueryTwoVersionSelection := by
  intro snapshot base writer positive foreignWriter foreignBase baseOld bounds activeBound
  constructor
  · simp [queryVersionVisible, queryCreatorVisible, foreignWriter]
  · by_cases active : writer ∈ snapshot.active
    · have lower := activeBound writer active
      simp [queryVersionVisible, queryCreatorVisible, foreignBase, baseOld,
        foreignWriter, active, Nat.not_lt_of_ge lower]
    · by_cases below : writer < snapshot.xmin
      · have upper : writer < snapshot.xmax := Nat.lt_of_lt_of_le below bounds
        simp [queryVersionVisible, queryCreatorVisible, foreignBase, baseOld,
          foreignWriter, active, below, Nat.ne_of_gt positive, Nat.not_le_of_gt upper]
      · simp [queryVersionVisible, queryCreatorVisible, foreignBase, baseOld,
          foreignWriter, active, below, Nat.ne_of_gt positive]

/-- Local freshness allows valid disjoint store reordering, unlike a globally
    increasing physical-store assumption. A private keyless write is also
    filtered after candidate union. -/
def QueryContractHasLiveWitnesses : Prop :=
  GuardedStampHistory (fun _ => 0) [⟨{1}, 4⟩, ⟨{2}, 3⟩] ∧
    queryCandidates (fun key => key = 9)
      (queryPostingReplay (queryInitialPostings (fun row => if row = 7 then some 9 else none))
        [⟨7, some 9, some 10, 1, 1, 4⟩]) = ∅ ∧
    queryMaterialize (fun row => if row = 7 then some 9 else none)
      (fun row => if row = 7 then some none else none) (fun key => key = 9) {7} = ∅ ∧
    queryMaterialize (fun _ => none)
      (fun row => if row = 7 then some (some 9) else none) (fun key => key = 9) ∅ = {7}

theorem query_contract_has_live_witnesses : QueryContractHasLiveWitnesses := by
  classical
  constructor
  · simp [GuardedStampHistory, publishPredicateStamp]
  constructor
  · ext row
    simp [queryCandidates, queryPostingReplay, queryInitialPostings, queryUpdatePostings]
  · constructor
    · ext row
      by_cases same : row = 7
      · simp [queryMaterialize, queryOwnCandidates, rowMatches, overlayOwnWrites, same]
      · simp [queryMaterialize, queryOwnCandidates, rowMatches, overlayOwnWrites, same]
    · ext row
      by_cases same : row = 7
      · simp [queryMaterialize, queryOwnCandidates, rowMatches, overlayOwnWrites, same]
      · simp [queryMaterialize, queryOwnCandidates, rowMatches, overlayOwnWrites, same]

end AerostoreProofs
