//! What a record says about the build and the machine that produced it.
//!
//! Two records are only comparable if they came from the same toolchain, the
//! same target, the same profile and the same resolved feature set. The
//! comparator refuses on a mismatch rather than rendering the difference, so
//! these fields are the ones that decide whether a report exists at all.
//!
//! # Who fills them in
//!
//! The driver, not the bench binary. A bench target compiled by cargo cannot
//! see its own toolchain version, its host triple, or which features cargo
//! resolved for it — and it must never resolve a git ref, because a `run`
//! against a worktree would then report the ref rather than the checkout. The
//! driver knows all of it per leg, serialises it once, and passes it down the
//! environment as [`FINGERPRINT_ENV`].
//!
//! A binary run by hand — `cargo bench --bench chain_wall -- --run …` — finds
//! the variable unset and stamps [`BuildFingerprint::local`], whose unknown
//! fields are absent rather than guessed. Two such records compare fine with
//! each other and are refused against a driver-produced leg, which is the
//! correct answer in both cases.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The environment variable carrying the driver's build fingerprint, as compact
/// JSON, into every bench process it starts.
pub const FINGERPRINT_ENV: &str = "SPATE_BENCH_FINGERPRINT";

/// The leg name a hand-run binary stamps, distinct from the driver's `base` and
/// `head` so a stray record cannot be mistaken for either.
pub const LOCAL_LEG: &str = "local";

/// The leg name for the reference's checkout.
pub const BASE_LEG: &str = "base";

/// The leg name for the working tree.
pub const HEAD_LEG: &str = "head";

/// Provenance for one leg of a comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildFingerprint {
    /// Runner protocol the binary speaks — [`crate::protocol::PROTOCOL_VERSION`].
    pub protocol: u32,
    /// Which side of the comparison this is: `base`, `head`, or `local`.
    pub leg: String,
    /// `rustc --version` output for the toolchain that built the leg.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rustc: Option<String>,
    /// The `host:` line of `rustc -vV`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_triple: Option<String>,
    /// Cargo profile the binary was built with — `bench` for anything the
    /// driver builds. The *name* only; `codegen` covers the settings behind it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    /// Digest of the leg's profile tables, `.cargo/config.toml` and rustflags.
    ///
    /// The guard that the profile name cannot be: both legs are built with
    /// `cargo bench`, so the name agrees by construction, while the settings
    /// behind it are what a performance change is most likely to touch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codegen: Option<String>,
    /// Features cargo resolved for the benched package, sorted.
    ///
    /// The resolved set rather than the flags: `--all-features` and an explicit
    /// list can name the same set, and two legs whose manifests differ can
    /// resolve differently from identical flags. It is the resolved set that
    /// decides whether the same code was compiled.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub features: Vec<String>,
    /// The feature flags as they were written on the command line, verbatim.
    ///
    /// Kept beside the resolved set so a report can say what was asked for as
    /// well as what it meant.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub feature_args: Vec<String>,
    /// `git describe --always --dirty` for the leg's checkout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_describe: Option<String>,
    /// Whether the leg's checkout had uncommitted changes.
    ///
    /// Normal on the head leg — that is the change being measured — and a
    /// warning on the base leg, which the driver checks out clean.
    pub dirty: bool,
}

impl BuildFingerprint {
    /// The fingerprint a binary stamps when no driver set [`FINGERPRINT_ENV`].
    ///
    /// Everything the process cannot observe about its own build is left
    /// absent. `profile` is the one exception: `debug_assertions` distinguishes
    /// a `cargo bench` build from a `cargo test` one, and getting that wrong is
    /// the difference between a number and a misprint.
    #[must_use]
    pub fn local() -> Self {
        Self {
            protocol: crate::protocol::PROTOCOL_VERSION,
            leg: LOCAL_LEG.to_owned(),
            rustc: None,
            host_triple: None,
            profile: Some(
                if cfg!(debug_assertions) {
                    "dev"
                } else {
                    "bench"
                }
                .to_owned(),
            ),
            codegen: None,
            features: Vec::new(),
            feature_args: Vec::new(),
            git_describe: None,
            dirty: false,
        }
    }

    /// Reads [`FINGERPRINT_ENV`], falling back to [`BuildFingerprint::local`].
    ///
    /// # Errors
    ///
    /// When the variable is set but does not parse. A malformed fingerprint is
    /// a driver bug, and silently falling back would produce records that pair
    /// with nothing and say nothing about why.
    pub fn from_env() -> Result<Self, String> {
        Self::from_raw(std::env::var(FINGERPRINT_ENV).ok().as_deref())
    }

    /// The decision [`BuildFingerprint::from_env`] makes, without reading the
    /// environment.
    ///
    /// Split out so it can be tested: `cargo test` runs a binary's tests in one
    /// process, so a test that set the variable would decide the answer for
    /// whatever else was running at the time.
    ///
    /// # Errors
    ///
    /// As [`BuildFingerprint::from_env`].
    pub fn from_raw(raw: Option<&str>) -> Result<Self, String> {
        match raw {
            None => Ok(Self::local()),
            Some(text) => serde_json::from_str(text)
                .map_err(|e| format!("{FINGERPRINT_ENV} is set but does not parse: {e}")),
        }
    }

    /// The fields whose disagreement makes two legs incomparable, as a map the
    /// comparator can diff by name.
    ///
    /// `leg` is deliberately absent: it differs by construction, and a guard
    /// that flagged it would fire on every comparison. `git_describe` and
    /// `dirty` are absent for the opposite reason — they differ on every
    /// *useful* comparison, since the whole point is two different trees.
    #[must_use]
    pub fn guarded_fields(&self) -> BTreeMap<&'static str, String> {
        let mut fields = BTreeMap::new();
        fields.insert("protocol", self.protocol.to_string());
        fields.insert("rustc", self.rustc.clone().unwrap_or_default());
        fields.insert("host_triple", self.host_triple.clone().unwrap_or_default());
        fields.insert("profile", self.profile.clone().unwrap_or_default());
        fields.insert("codegen", self.codegen.clone().unwrap_or_default());
        fields.insert("features", self.features.join(","));
        fields
    }
}

/// The machine a record was produced on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Host {
    /// `os/arch`, e.g. `macos/aarch64`.
    pub os: String,
    /// CPU brand string, or `unknown` where the platform does not offer one.
    pub cpu: String,
    /// Cores visible to the process.
    pub cores: usize,
    /// A label for the machine: `SPATE_BENCH_HOST`, or `local` when unset.
    pub label: String,
}

impl Host {
    /// Detects the machine this process is running on.
    #[must_use]
    pub fn detect() -> Self {
        Self {
            os: format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH),
            cpu: cpu_brand(),
            cores: std::thread::available_parallelism().map_or(0, std::num::NonZeroUsize::get),
            label: std::env::var("SPATE_BENCH_HOST").unwrap_or_else(|_| "local".to_owned()),
        }
    }

    /// The fields whose disagreement makes two legs incomparable.
    ///
    /// A comparison across two machines is not a comparison, whatever the
    /// numbers look like.
    #[must_use]
    pub fn guarded_fields(&self) -> BTreeMap<&'static str, String> {
        let mut fields = BTreeMap::new();
        fields.insert("host_os", self.os.clone());
        fields.insert("host_cpu", self.cpu.clone());
        fields.insert("host_cores", self.cores.to_string());
        // The label too. Two runners of one instance type agree on os, cpu and
        // core count exactly — which is the shape a dedicated benchmark host
        // takes — so without this the one field whose whole purpose is naming
        // the machine would be the one field that could not tell two apart.
        fields.insert("host_label", self.label.clone());
        fields
    }
}

/// The CPU brand, read from whichever interface the platform offers.
///
/// Named platforms with an explicit fall-through, rather than one path and a
/// guess: an unknown platform reports `unknown`, which the guard treats as
/// equal to another `unknown` on the same machine and unequal to a real brand.
fn cpu_brand() -> String {
    #[cfg(target_vendor = "apple")]
    {
        sysctl_string("machdep.cpu.brand_string").unwrap_or_else(|| "unknown".to_owned())
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        std::fs::read_to_string("/proc/cpuinfo")
            .ok()
            .and_then(|text| {
                text.lines()
                    .find_map(|line| line.strip_prefix("model name"))
                    .and_then(|rest| rest.split_once(':'))
                    .map(|(_, value)| value.trim().to_owned())
            })
            .unwrap_or_else(|| "unknown".to_owned())
    }
    #[cfg(not(any(target_vendor = "apple", target_os = "linux", target_os = "android")))]
    {
        "unknown".to_owned()
    }
}

#[cfg(target_vendor = "apple")]
fn sysctl_string(name: &str) -> Option<String> {
    use std::ffi::CString;

    let key = CString::new(name).ok()?;
    let mut len: libc::size_t = 0;
    // SAFETY: `key` is a live NUL-terminated string for the duration of the
    // call. A null value pointer with a valid length pointer is the documented
    // way to ask `sysctlbyname` for the size it would write.
    if unsafe {
        libc::sysctlbyname(
            key.as_ptr(),
            std::ptr::null_mut(),
            &raw mut len,
            std::ptr::null_mut(),
            0,
        )
    } != 0
        || len == 0
    {
        return None;
    }
    let mut buf = vec![0u8; len];
    // SAFETY: `buf` is a live allocation of exactly `len` bytes and `len` is
    // the size the call above reported, so `sysctlbyname` writes within it.
    if unsafe {
        libc::sysctlbyname(
            key.as_ptr(),
            buf.as_mut_ptr().cast(),
            &raw mut len,
            std::ptr::null_mut(),
            0,
        )
    } != 0
    {
        return None;
    }
    buf.truncate(len);
    while buf.last() == Some(&0) {
        buf.pop();
    }
    String::from_utf8(buf).ok()
}

#[cfg(test)]
mod tests {
    use super::{BuildFingerprint, Host, LOCAL_LEG};

    #[test]
    fn a_local_fingerprint_leaves_the_unknowable_absent() {
        let local = BuildFingerprint::local();
        assert_eq!(local.leg, LOCAL_LEG);
        assert_eq!(local.protocol, crate::protocol::PROTOCOL_VERSION);
        assert!(local.rustc.is_none());
        assert!(local.host_triple.is_none());
        assert!(!local.dirty);

        // Absent fields must not serialise at all — a `null` would compare
        // unequal to a driver leg that simply omitted them.
        let json = serde_json::to_string(&local).expect("serialises");
        assert!(!json.contains("rustc"), "{json}");
        assert!(!json.contains("git_describe"), "{json}");
    }

    #[test]
    fn the_guard_ignores_the_fields_that_differ_by_construction() {
        let mut base = BuildFingerprint::local();
        base.leg = "base".to_owned();
        base.git_describe = Some("abc1234".to_owned());
        let mut head = BuildFingerprint::local();
        head.leg = "head".to_owned();
        head.git_describe = Some("def5678".to_owned());
        head.dirty = true;

        assert_eq!(
            base.guarded_fields(),
            head.guarded_fields(),
            "the guard must not fire on the leg name or the commit"
        );

        head.rustc = Some("rustc 1.94.0".to_owned());
        assert_ne!(base.guarded_fields(), head.guarded_fields());
    }

    /// Both branches of the real decision, through the seam that exists so a
    /// test does not have to set a process-global variable.
    #[test]
    fn a_malformed_fingerprint_is_an_error_rather_than_a_fallback() {
        assert_eq!(
            BuildFingerprint::from_raw(None).expect("falls back"),
            BuildFingerprint::local()
        );

        let err = BuildFingerprint::from_raw(Some("{\"leg\":")).expect_err("malformed");
        assert!(err.contains(super::FINGERPRINT_ENV), "{err}");

        let round_tripped = serde_json::to_string(&BuildFingerprint::local()).expect("serialises");
        assert_eq!(
            BuildFingerprint::from_raw(Some(&round_tripped)).expect("parses"),
            BuildFingerprint::local()
        );
    }

    #[test]
    fn the_host_reports_something_for_every_guarded_field() {
        let host = Host::detect();
        assert!(!host.os.is_empty());
        assert!(!host.cpu.is_empty());
        assert_eq!(host.guarded_fields().len(), 4);
        assert_eq!(host.guarded_fields(), Host::detect().guarded_fields());
    }
}
