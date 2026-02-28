use aerostore_macros::speedtable;

#[speedtable]
#[repr(C)]
struct BadReservedXmin {
    _xmin: u64,
    altitude: i32,
}

fn main() {}

