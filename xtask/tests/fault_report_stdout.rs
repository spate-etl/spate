//! `cargo xtask fault-test report` prints each route on its own line, and
//! each line names the `<route>.title` and `<route>.md` it wrote.

use std::process::Command;

#[test]
fn report_prints_the_stem_of_each_file_it_writes() {
    let dir = std::env::temp_dir().join(format!("xtask-fault-report-cli-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let out = dir.join("out");
    let run = Command::new(env!("CARGO_BIN_EXE_spate-xtask"))
        .args([
            "fault-test",
            "report",
            "--run-url",
            "https://example.invalid/runs/1",
        ])
        .arg("--summary")
        .arg(dir.join("absent/summary.json"))
        .arg("--out-dir")
        .arg(&out)
        .output()
        .unwrap();
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let stdout = String::from_utf8(run.stdout).unwrap();
    let routes: Vec<&str> = stdout.lines().collect();
    assert_eq!(routes, ["harness"]);
    for route in routes {
        assert!(
            out.join(format!("{route}.title")).is_file(),
            "{route}.title"
        );
        assert!(out.join(format!("{route}.md")).is_file(), "{route}.md");
    }
    std::fs::remove_dir_all(&dir).unwrap();
}
