#[test]
fn speedtable_compile_contracts() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/compile_pass_*.rs");
    t.compile_fail("tests/ui/compile_fail_*.rs");
}
