//! The coordination seam is gone from `spate-core`: no module, no declaration
//! of one, and no metric family under the `spate_coordination_` prefix.

use std::path::Path;

fn src(rel: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src").join(rel)
}

#[test]
fn no_coordination_module() {
    assert!(!src("coordination").exists(), "src/coordination exists");
    assert!(
        !src("coordination.rs").exists(),
        "src/coordination.rs exists"
    );
    let lib = std::fs::read_to_string(src("lib.rs")).expect("read lib.rs");
    assert!(
        !lib.lines().any(|l| l.contains("mod coordination")),
        "lib.rs declares a coordination module"
    );
    let names = std::fs::read_to_string(src("metrics/names.rs")).expect("read names.rs");
    assert!(
        !names.contains("\"spate_coordination_"),
        "names.rs holds a spate_coordination_ family"
    );
}
