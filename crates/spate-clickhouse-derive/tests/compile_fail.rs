//! `#[derive(ClickHouseRow)]` rejections. Each fixture pins one rejection
//! this derive makes at compile time; the matching `.stderr` is the pinned
//! message.

#[test]
fn compile_fail() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
}
