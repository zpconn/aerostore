// Generated from native unlink_node; see generate.py and README.md.
// Conditional native unlink_node detachment-loop proof. These primitive
// contracts are obligations, not proofs of native pointers, atomics or epochs.
use vstd::prelude::*;
verus! {

#[derive(PartialEq, Eq, Debug)]
pub enum Error { InvalidOffset }
pub ghost struct View {
    pub height: usize,
    pub attached: Set<usize>,
    pub window: Set<usize>,
    // Target key, payload-posting chain and successor lanes visible to an epoch
    // pin; marking flags and retired-list metadata are intentionally excluded.
    pub pinned_contents: Seq<u32>,
    pub guarded_and_pinned: bool,
    pub retired: bool,
}
pub open spec fn valid(v: View) -> bool {
    0 < v.height <= 32 && v.guarded_and_pinned && !v.retired
        && forall|lane: usize| v.attached.contains(lane) ==> lane < v.height
}
pub open spec fn detached(v: View) -> bool {
    forall|lane: usize| lane < v.height ==> !v.attached.contains(lane)
}
pub open spec fn frame(a: View, b: View) -> bool {
    b.height == a.height && b.guarded_and_pinned == a.guarded_and_pinned
        && b.pinned_contents == a.pinned_contents && b.retired == a.retired
        && b.attached.subset_of(a.attached)
}
pub open spec fn cas_step(a: View, b: View, lane: usize, success: bool) -> bool {
    frame(a, b) && b.window == a.window
        && (success ==> !b.attached.contains(lane))
        && (!success ==> b.attached == a.attached)
        && forall|other: usize| other != lane ==>
            b.attached.contains(other) == a.attached.contains(other)
}

pub trait GuardedLanes {
    spec fn view(&self) -> View;
    fn window_contains(&self, lane: usize) -> (r: bool)
        requires valid(self.view()), lane < self.view().height,
        ensures r == self.view().window.contains(lane);
    // Actual lane_ref_by_offset + node_next_offset + expected-target CAS.
    // Errors never retire, invalidate the epoch pin, or write target contents.
    fn detach_lane(&mut self, lane: usize) -> (r: Result<bool, Error>)
        requires valid(old(self).view()), lane < old(self).view().height,
            old(self).view().window.contains(lane),
        ensures valid(final(self).view()),
            frame(old(self).view(), final(self).view()),
            final(self).view().window == old(self).view().window,
            r.is_ok() ==> cas_step(old(self).view(), final(self).view(), lane, r.unwrap()),
            r.is_err() ==> final(self).view().attached == old(self).view().attached;
    // Guarded find may help unlink marked lanes. On success its refreshed
    // window exactly identifies remaining target predecessors; no reattachment.
    fn refresh_window(&mut self) -> (r: Result<(), Error>)
        requires valid(old(self).view()),
        ensures valid(final(self).view()), frame(old(self).view(), final(self).view()),
            r.is_ok() ==> final(self).view().window == final(self).view().attached;
    fn retire_node(&mut self)
        requires valid(old(self).view()), detached(old(self).view()),
        ensures final(self).view().retired,
            final(self).view().attached == old(self).view().attached,
            final(self).view().height == old(self).view().height,
            final(self).view().guarded_and_pinned,
            final(self).view().pinned_contents == old(self).view().pinned_contents;
}

pub fn cached_detach<E: GuardedLanes>(engine: &mut E, height: usize)
    -> (r: Result<bool, Error>)
    requires valid(old(engine).view()), height == old(engine).view().height,
    ensures valid(final(engine).view()), frame(old(engine).view(), final(engine).view()),
        r == Ok(true) ==> detached(final(engine).view()),
{
    let mut detached_all = true;
    let mut level = height;
    while level > 0
        invariant
            level <= height,
            valid(engine.view()), height == engine.view().height,
            frame(old(engine).view(), engine.view()),
            
            detached_all ==> forall|lane: usize| level <= lane < height ==>
                !engine.view().attached.contains(lane),
        decreases level,
    {
        level -= 1;
if ! engine . window_contains ( level ) {
    detached_all = false ;
    return Ok ( detached_all ) ;
}
if ! engine . detach_lane ( level ) ? {
    detached_all = false ;
    return Ok ( detached_all ) ;
}
    }
    Ok(detached_all)
}

pub fn refreshed_detach<E: GuardedLanes>(engine: &mut E, height: usize)
    -> (r: Result<bool, Error>)
    requires valid(old(engine).view()), height == old(engine).view().height,
        old(engine).view().attached.subset_of(old(engine).view().window),
    ensures valid(final(engine).view()), frame(old(engine).view(), final(engine).view()),
        r == Ok(true) ==> detached(final(engine).view()),
{
    let mut detached_all = true;
    let mut level = height;
    while level > 0
        invariant
            level <= height,
            valid(engine.view()), height == engine.view().height,
            frame(old(engine).view(), engine.view()),
            engine.view().attached.subset_of(engine.view().window),
            detached_all ==> forall|lane: usize| level <= lane < height ==>
                !engine.view().attached.contains(lane),
        decreases level,
    {
        level -= 1;
if ! engine . window_contains ( level ) {
    continue ;
}
detached_all = false ;
let _ = engine . detach_lane ( level ) ? ;
    }
    Ok(detached_all)
}

#[verifier::exec_allows_no_decreases_clause]
pub fn detach_before_retire<E: GuardedLanes>(engine: &mut E, height: usize)
    -> (r: Result<(), Error>)
    requires valid(old(engine).view()), height == old(engine).view().height,
    ensures final(engine).view().pinned_contents == old(engine).view().pinned_contents,
        final(engine).view().height == height, final(engine).view().guarded_and_pinned,
        r.is_ok() ==> final(engine).view().retired && detached(final(engine).view()),
        r.is_err() ==> !final(engine).view().retired,
{
    let mut detached_all = cached_detach(engine, height)?;
    while ! detached_all
        invariant valid(engine.view()), height == engine.view().height,
            frame(old(engine).view(), engine.view()),
            detached_all ==> detached(engine.view()),
        ensures detached(engine.view()),
            valid(engine.view()), height == engine.view().height,
            frame(old(engine).view(), engine.view()),
    {
        engine.refresh_window()?;
        detached_all = refreshed_detach(engine, height)?;
if true {
    break ;
}
    }
    engine.retire_node();
    Ok(())
}

// Concrete witnesses show these contracts permit both cached-CAS success and
// failure followed by find helping. This is contract consistency, not native
// heap refinement or a liveness theorem for the retry loop.
pub proof fn primitive_contracts_have_success_and_retry_witnesses(contents: Seq<u32>) {
    let a = View { height: 2, attached: set![0usize, 1usize], window: set![0usize, 1usize],
        pinned_contents: contents, guarded_and_pinned: true, retired: false };
    let b = View { attached: set![0usize], ..a };
    let c = View { attached: Set::empty(), ..a };
    let found = View { window: Set::empty(), ..c };
    assert(valid(a));
    assert(0usize < a.height && a.attached.contains(0usize));
    assert(!detached(a));
    assert(cas_step(a, b, 1, true));
    assert(cas_step(b, c, 0, true));
    assert(cas_step(a, a, 1, false));
    assert(frame(a, found));
    assert(found.window == found.attached);
    assert(detached(c));
    assert(detached(found));
}
}
