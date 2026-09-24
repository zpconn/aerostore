use aerostore_macros::speedtable;

#[speedtable]
#[repr(C)]
struct BadReservedXmax {
    _xmax: u64,
    altitude: i32,
}

fn main() {}

