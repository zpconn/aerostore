use aerostore_macros::speedtable;

#[speedtable]
#[repr(C)]
struct GenericRow<T: Copy> {
    key: T,
    seq: u64,
}

fn main() {
    let mut row = GenericRow::new(7_u32, 99);
    let bit = GenericRow::<u32>::__null_index_for_field("seq").expect("seq bit");
    row.__set_null_by_index(bit, true);
    assert!(row.__is_null_by_index(bit));
    assert_eq!(row.key, 7);
    assert_eq!(row.seq, 99);
}
