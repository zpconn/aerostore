use std::sync::Arc;
use aerostore_core::{compute_global_xmin,run_vacuum_pass,OccTable,ShmArena};
fn scenario(raw:bool) {
    let arena=Arc::new(ShmArena::new(4<<20).unwrap());
    let table=OccTable::<u64>::new(Arc::clone(&arena),1).unwrap();
    table.seed_row(0,10).unwrap();
    let mut reader=table.begin_transaction().unwrap();
    let mut writer=table.begin_transaction().unwrap();
    table.write(&mut writer,0,20).unwrap();
    table.commit(&mut writer).unwrap();
    let correct_horizon=compute_global_xmin(arena.as_ref());
    let safe_count=run_vacuum_pass(&table).unwrap().len();
    let raw_count=if raw {table.vacuum_reclaim_once(u64::MAX).unwrap().len()} else {0};
    // No allocation or row reuse follows reclamation. This is ordinary safe Rust.
    let observed=table.read(&mut reader,0).unwrap();
    let commit=table.commit(&mut reader);
    println!("raw={raw} correct_horizon={correct_horizon} safe_reclaimed={safe_count} raw_reclaimed={raw_count} observed={observed:?} reader_commit={commit:?}");
    if raw { assert_eq!(raw_count,1); assert_eq!(observed,None); assert_eq!(commit,Ok(0)); }
    else {assert_eq!(safe_count,0);assert_eq!(observed,Some(10));}
}
fn main(){scenario(false);scenario(true);}
