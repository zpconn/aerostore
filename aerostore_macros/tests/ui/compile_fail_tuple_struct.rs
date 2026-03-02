use aerostore_macros::speedtable;

#[speedtable]
#[repr(C)]
struct BadTupleRow(u64, i32);

fn main() {}
