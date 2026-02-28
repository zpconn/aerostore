use aerostore_macros::speedtable;

#[speedtable]
#[repr(C)]
struct BadReservedNullBitmask {
    _null_bitmask: u64,
    altitude: i32,
}

fn main() {}

