use crate::lifecycle;
verus! {
// This narrow interface records which horizon the actual native caller passes
// to its source-bound internal kernel. It assumes no pruning or history result.
pub trait VacuumDispatch: lifecycle::LifecyclePrimitives {
    spec fn dispatched_horizon(&self)->Option<u64>;
    fn reclaim_before(&mut self,horizon:u64)->(result:Result<Vec<Reclaimed>,Error>)
        requires !old(self).state().lifecycle_held,
        ensures final(self).dispatched_horizon()==Some(horizon);
}
pub enum VacuumError { Occ(Error) }
pub fn min_horizon(requested:u64,retained:u64)->(result:u64)
    ensures result<=requested,result<=retained,
        result==if requested<retained {requested} else {retained},
{
    if requested<retained {requested} else {retained}
}
pub fn compute_global_xmin<D:lifecycle::LifecyclePrimitives>(driver:&mut D)->(result:u64)
    requires lifecycle::well_formed(old(driver).state()),!old(driver).state().lifecycle_held,
    ensures lifecycle::well_formed(final(driver).state()),!final(driver).state().lifecycle_held,
        final(driver).state().slots==old(driver).state().slots,
        result==lifecycle::minimum_retention(final(driver).state().slots,lifecycle::PROCARRAY_SLOTS as int,final(driver).state().sampled_clock),
        forall|i:int| 0<=i<final(driver).state().slots.len() && final(driver).state().slots[i].txid!=0 ==>
            result<=final(driver).state().slots[i].snapshot_xmin,
{
    /* NATIVE_COMPUTE_HORIZON */
}
pub fn public_vacuum_reclaim_once<D:VacuumDispatch>(driver:&mut D,requested_xmin:u64)->(result:Result<Vec<Reclaimed>,Error>)
    requires lifecycle::well_formed(old(driver).state()),!old(driver).state().lifecycle_held,
    ensures final(driver).dispatched_horizon().is_some(),
        final(driver).dispatched_horizon().unwrap()<=requested_xmin,
        forall|i:int| 0<=i<old(driver).state().slots.len() && old(driver).state().slots[i].txid!=0 ==>
            final(driver).dispatched_horizon().unwrap()<=old(driver).state().slots[i].snapshot_xmin,
{
    /* NATIVE_PUBLIC_VACUUM */
}
pub fn run_vacuum_pass<D:VacuumDispatch>(driver:&mut D)
    ->(result:Result<Vec<Reclaimed>,VacuumError>)
    requires lifecycle::well_formed(old(driver).state()),!old(driver).state().lifecycle_held,
    ensures final(driver).dispatched_horizon().is_some(),
        forall|i:int| 0<=i<old(driver).state().slots.len() && old(driver).state().slots[i].txid!=0 ==>
            final(driver).dispatched_horizon().unwrap()<=old(driver).state().slots[i].snapshot_xmin,
{
    /* NATIVE_COLLECTOR_DISPATCH */
}
}
