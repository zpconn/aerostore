use aerostore_macros::speedtable;

#[speedtable]
#[repr(C)]
struct BadReservedLegacyNullmask {
    _nullmask: u64,
    altitude: i32,
}

fn main() {}

