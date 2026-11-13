//! The SIMD features the process can use, logged once when the first pipeline
//! starts.
//!
//! In a default x86_64 build, each dependency that uses a feature in
//! [`SIMD_FEATURES`] checks for it at run time, so the host decides which code
//! runs. On aarch64, NEON is in the target's baseline, so the build compiles
//! the NEON code in. `crc` is checked at run time on aarch64 Linux and is in
//! the baseline on macOS. A virtual machine's CPU model can hide features the
//! physical CPU has.
//!
//! Both lists cover only the rows of [`SIMD_FEATURES`]. `compiled` lists the
//! rows the build compiles in, the target's defaults included, so a raised
//! `-C target-cpu` or `-C target-feature` shows there when it adds a row.
//! `detected` comes from std's detection macros, which read a compiled-in
//! feature as present without asking the CPU, so every name in `compiled` is
//! also in `detected`, whatever the host.

use std::env::consts::ARCH;
use std::sync::Once;

/// One row of [`SIMD_FEATURES`].
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct SimdFeature {
    /// The name std's detection macro takes.
    pub name: &'static str,
    /// Whether the build compiles the feature in, the target's defaults
    /// included.
    pub compiled: bool,
    /// The run-time probe.
    pub detect: fn() -> bool,
}

/// Builds the table for one architecture's detection macro, which takes only
/// a literal.
macro_rules! simd_features {
    ($detect:ident; $($name:tt),+ $(,)?) => {
        &[$(SimdFeature {
            name: $name,
            compiled: cfg!(target_feature = $name),
            detect: || $detect!($name),
        }),+]
    };
}

/// The SIMD features the process reports, in the order it lists them.
///
/// The features the decode and decompression paths dispatch on at run time:
/// `memchr` and `simdutf8` on `avx2`, `simd-json` on `avx2` with `pclmulqdq`
/// and then `sse4.2`, `simdutf8` on `sse4.2`, `zlib-rs` on `avx2` with `bmi1`
/// and `bmi2`, and on `pclmulqdq` with `sse4.1`, and libzstd on `bmi1` with
/// `bmi2`. Then the wider AVX-512 features a later path may take, so a host
/// that hides part of AVX-512 still records it.
#[doc(hidden)]
#[cfg(target_arch = "x86_64")]
pub const SIMD_FEATURES: &[SimdFeature] = simd_features!(
    is_x86_feature_detected;
    "sse4.1", "sse4.2", "pclmulqdq", "avx2", "bmi1", "bmi2",
    "avx512f", "avx512bw", "avx512vl", "avx512vbmi", "vpclmulqdq",
);

/// The SIMD features the process reports, in the order it lists them.
///
/// `neon` is in the target's baseline, and `memchr`, `simdutf8`, `simd-json`,
/// `zlib-rs` and libzstd compile their NEON code in. `zlib-rs` takes `crc`
/// for the gzip checksum, and checks for it at run time where the baseline
/// lacks it, as on Linux. Then the SVE features a later path may take.
#[doc(hidden)]
#[cfg(target_arch = "aarch64")]
pub const SIMD_FEATURES: &[SimdFeature] =
    simd_features!(is_aarch64_feature_detected; "neon", "crc", "sve", "sve2");

#[cfg(target_arch = "aarch64")]
use std::arch::is_aarch64_feature_detected;

/// No SIMD feature is reported on another architecture.
#[doc(hidden)]
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub const SIMD_FEATURES: &[SimdFeature] = &[];

/// The features in [`SIMD_FEATURES`] this process detects, in table order.
#[doc(hidden)]
#[must_use]
pub fn detected_simd() -> Vec<String> {
    detected(SIMD_FEATURES)
}

/// The features in [`SIMD_FEATURES`] the build compiles in, in table order.
#[doc(hidden)]
#[must_use]
pub fn compiled_simd() -> Vec<String> {
    compiled(SIMD_FEATURES)
}

fn detected(table: &[SimdFeature]) -> Vec<String> {
    table
        .iter()
        .filter(|f| (f.detect)())
        .map(|f| f.name.to_owned())
        .collect()
}

fn compiled(table: &[SimdFeature]) -> Vec<String> {
    table
        .iter()
        .filter(|f| f.compiled)
        .map(|f| f.name.to_owned())
        .collect()
}

/// Log this process's SIMD features, on the first call only.
#[doc(hidden)]
pub fn log_once() {
    static LOGGED: Once = Once::new();
    LOGGED.call_once(|| {
        log_features(ARCH, &compiled_simd(), &detected_simd());
    });
}

fn log_features(arch: &str, compiled: &[String], detected: &[String]) {
    tracing::info!(
        arch = %arch,
        compiled = %compiled.join(","),
        detected = %detected.join(","),
        "cpu features"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::LogBuf;

    fn feature(name: &'static str, compiled: bool, detected: bool) -> SimdFeature {
        SimdFeature {
            name,
            compiled,
            detect: if detected { || true } else { || false },
        }
    }

    /// Pins that only the features whose probe reports them are listed, in
    /// the table's order.
    #[test]
    fn only_the_detected_features_are_listed_in_table_order() {
        let table = [
            feature("a", false, true),
            feature("b", true, false),
            feature("c", false, true),
        ];
        assert_eq!(detected(&table), vec!["a".to_owned(), "c".to_owned()]);
    }

    /// Only the features the build compiles in are listed, in the table's
    /// order, whatever their probe says.
    #[test]
    fn only_the_compiled_features_are_listed_in_table_order() {
        let table = [
            feature("a", true, false),
            feature("b", false, true),
            feature("c", true, true),
        ];
        assert_eq!(compiled(&table), vec!["a".to_owned(), "c".to_owned()]);
    }

    /// Pins the std behaviour the module documentation states: a feature the
    /// build compiles in reads as detected.
    #[test]
    fn every_compiled_feature_reads_as_detected() {
        let detected = detected_simd();
        for name in compiled_simd() {
            assert!(detected.contains(&name), "{name} not in {detected:?}");
        }
    }

    /// Every aarch64 target Rust supports compiles NEON in.
    #[cfg(target_arch = "aarch64")]
    #[test]
    fn an_aarch64_build_compiles_neon_in() {
        assert!(
            compiled_simd().iter().any(|f| f == "neon"),
            "{:?}",
            compiled_simd()
        );
    }

    /// The line is one `INFO` event carrying the architecture and both lists,
    /// comma-joined in the order given.
    #[test]
    fn the_line_carries_the_architecture_and_both_lists() {
        let buf = LogBuf::default();
        let writer = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_target(false)
            .without_time()
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            log_features(
                "x86_64",
                &["avx2".to_owned()],
                &["sse4.2".to_owned(), "avx2".to_owned()],
            );
        });
        let lines: Vec<serde_json::Value> = buf
            .text()
            .lines()
            .map(|l| serde_json::from_str(l).expect("a JSON line"))
            .collect();
        assert_eq!(
            lines,
            vec![serde_json::json!({
                "level": "INFO",
                "message": "cpu features",
                "arch": "x86_64",
                "compiled": "avx2",
                "detected": "sse4.2,avx2",
            })]
        );
    }
}
