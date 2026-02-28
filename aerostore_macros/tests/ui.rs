#[test]
fn speedtable_compile_fail_contracts() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/compile_fail_*.rs");
}
