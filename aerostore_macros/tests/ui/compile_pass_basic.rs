use aerostore_macros::speedtable;

#[speedtable]
#[repr(C)]
struct FlightRow {
    ident: u64,
    altitude: i32,
    gs: u16,
}

fn main() {
    let mut row = FlightRow::new(42, 30_000, 450);
    let ident_bit = FlightRow::__null_index_for_field("ident").expect("ident bit");
    let alt_bit = FlightRow::__null_index_for_field("altitude").expect("altitude bit");
    assert_eq!(ident_bit, 0);
    assert_eq!(alt_bit, 1);

    row.__set_null_by_index(ident_bit, true);
    row.__set_null_by_index(alt_bit, false);
    assert!(row.__is_null_field("ident"));
    assert!(!row.__is_null_field("altitude"));
    assert_eq!(row.__null_bitmask(), 1_u64 << ident_bit);
    assert_eq!(row.__xmin(), 0);
    assert_eq!(row.__xmax(), 0);
}
