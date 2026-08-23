//! Pipeline configuration loading from arbitrary YAML text.
//!
//! `PipelineConfig::from_str` runs the `${VAR}` interpolator over the raw text
//! before the YAML parse, so the target drives both stages with one input. A
//! configuration it returns is one the loaders promise to have validated, so
//! the target re-runs `validate` and asserts it still passes.
//!
//! The interpolator resolves names against the process environment. The target
//! replaces that environment on its first run with a fixed one, `A=value` and
//! `B=` and nothing else, so a corpus entry frames the same way on any machine
//! and reaches the set, empty and unset branches with two-character names.

#![no_main]

use libfuzzer_sys::fuzz_target;
use spate_core::config::PipelineConfig;
use std::sync::Once;

static ENVIRONMENT: Once = Once::new();

fn pin_environment() {
    let names: Vec<String> = std::env::vars().map(|(name, _)| name).collect();
    // SAFETY: `Once` runs this on the fuzzing thread before the first parse,
    // while the harness is single-threaded.
    unsafe {
        for name in names {
            std::env::remove_var(name);
        }
        std::env::set_var("A", "value");
        std::env::set_var("B", "");
    }
}

fuzz_target!(|text: &str| {
    ENVIRONMENT.call_once(pin_environment);

    if let Ok(config) = PipelineConfig::from_str(text) {
        config
            .validate()
            .expect("from_str returns a validated configuration");
    }
});
