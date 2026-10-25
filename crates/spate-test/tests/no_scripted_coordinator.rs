//! The scripted coordinator is gone from `spate-test`.

use std::path::Path;

#[test]
fn no_scripted_coordinator() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    assert!(
        !src.join("coordination.rs").exists(),
        "src/coordination.rs exists"
    );
    let lib = std::fs::read_to_string(src.join("lib.rs")).expect("read lib.rs");
    assert!(
        !lib.contains("scripted_coordinator"),
        "lib.rs exports scripted_coordinator"
    );
}
