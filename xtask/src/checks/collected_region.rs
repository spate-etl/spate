//! The guard over a counted-tier bench case's collected region.
//!
//! A callgrind toggle bounds the region, and a toggle flips collection, so work
//! the optimizer reshapes can fall outside it while the C runtime's teardown is
//! counted in its place. The bench builds, runs, exits 0 and reports a number in
//! the millions.
//!
//! The classification axis is the ELF object each instruction executed in, from
//! callgrind's `ob=` lines. Every crate and dependency is compiled into the
//! bench executable, so "the binary under measurement" is the whole
//! application. A case attributing less than [`MIN_APPLICATION_PCT`] of its
//! collected instructions there is refused, as is one whose collected region is
//! below [`MIN_COLLECTED_IR`].
//!
//! Every case is judged on its own, over the sum of its parts: callgrind writes
//! one output per thread, `<base>.t<thread>.p<part>.out`, and a thread that
//! never entered the collected region declares `summary: 0`.
//!
//! DEVELOPING.md states the bench-authoring rule this enforces and where the
//! thresholds come from.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use crate::run::{Error, Outcome};

/// The prefix on every line this check writes for itself.
const TOOL: &str = "collected-region";

/// The share of a case's collected instructions that must land in the binary
/// under measurement. The measured spread it sits below is in DEVELOPING.md.
const MIN_APPLICATION_PCT: i64 = 10;

/// The second signal. A lost region can also leave almost nothing: a handful of
/// instructions belonging to the toggled wrapper. That is application code, and
/// the composition rule passes it. The floor is 1,000 against a smallest real
/// case of 6,656.
const MIN_COLLECTED_IR: i64 = 1000;

/// Judges every case under `dir`, defaulting to the tree the benches write to.
///
/// `shard` prefixes every line with the (package, arm) being measured. A
/// relative `dir` resolves against the repository root, and the paths the
/// diagnostics name are built from it as given.
pub(crate) fn check(root: &Path, explain: bool, shard: Option<&str>, dir: Option<&str>) -> Outcome {
    let dir = dir.map_or_else(default_dir, |d| d.trim_end_matches('/').to_owned());
    if explain {
        println!("(reads {dir})");
        return Ok(());
    }
    check_dir(root, &dir, shard.unwrap_or_default())
}

/// Where the benches just wrote, when the caller does not say.
fn default_dir() -> String {
    target_tree(std::env::var("CARGO_TARGET_DIR").ok().as_deref())
}

/// The gungraun tree under a cargo target directory, where an empty variable
/// reads as an unset one.
fn target_tree(target: Option<&str>) -> String {
    format!(
        "{}/gungraun",
        target.filter(|t| !t.is_empty()).unwrap_or("target")
    )
}

/// Judges every case under `dir`, one directory per case.
///
/// Fails closed: a run that measured nothing and a run that measured well are
/// otherwise the same green job.
fn check_dir(root: &Path, dir: &str, shard: &str) -> Outcome {
    let base = root.join(dir);
    if !base.is_dir() {
        return Err(Error::msg(format!(
            "{dir} does not exist; there are no profiles to check"
        )));
    }
    // Joined here so one shard's message is greppable by the same pattern
    // whether or not the tier is fanned out.
    let shard = if shard.is_empty() {
        String::new()
    } else {
        format!("{shard} — ")
    };

    let mut failed = false;
    let mut checked = 0u64;
    for rel in case_dirs(&base)? {
        // `spate-s3/descriptor_gungraun/descriptor/decode.full_splits`,
        // relative to the tree the caller named. A profile sitting directly in
        // that tree has no relative path to strip to, so it is named for its
        // own directory.
        let (case_dir, case_id) = if rel.is_empty() {
            (dir.to_owned(), basename(dir).to_owned())
        } else {
            (format!("{dir}/{rel}"), rel.clone())
        };
        let parts = case_parts(&base.join(&rel), &case_dir);
        if parts.is_empty() {
            continue;
        }
        let where_ = if parts.len() == 1 {
            parts[0].0.clone()
        } else {
            format!("{case_dir} ({} parts)", parts.len())
        };
        let verdict = read_parts(&parts).map_or_else(
            |()| Verdict::Refused(Refusal::bare("unreadable")),
            |texts| read_case(&texts),
        );
        checked += 1;
        if report(&shard, &case_id, &where_, &verdict) {
            failed = true;
        }
    }

    if checked == 0 {
        println!(
            "::error::{shard}no callgrind profile under {dir}; the benches wrote no measurement to check."
        );
        return Err(Error::status(1));
    }
    if failed {
        return Err(Error::status(1));
    }
    println!(
        "{TOOL}: {shard}{checked} case(s) attribute at least {MIN_APPLICATION_PCT}% of their collected instructions to the binary under measurement"
    );
    Ok(())
}

/// Writes one case's verdict, answering whether it refuses the case.
fn report(shard: &str, case_id: &str, where_: &str, verdict: &Verdict) -> bool {
    let (hundredths, pct, app, total) = match verdict {
        Verdict::Refused(refusal) => {
            println!(
                "::error::{shard}{case_id}: its callgrind profile could not be read ({}).",
                refusal.fields()
            );
            println!("  {where_}");
            println!(
                "  The guard refuses to judge a profile it cannot account for; see xtask/src/checks/collected_region.rs."
            );
            return true;
        }
        Verdict::Ok {
            hundredths,
            pct,
            app,
            total,
        } => (*hundredths, pct, *app, *total),
    };
    // The magnitude corroborator first: a case that collected almost nothing
    // has lost its region whatever the surviving instructions belong to.
    if total < MIN_COLLECTED_IR {
        println!(
            "::error::{shard}{case_id}: the collected region is {total} Ir, below the {MIN_COLLECTED_IR} floor;"
        );
        println!(
            "  a bench case cannot do meaningful work in that many instructions, so the region was lost"
        );
        println!(
            "  rather than measured: the same defect as a runtime-dominated region, wearing the other face."
        );
        println!("  Profile: {where_}");
        println!(
            "  Move the measured work into a named #[inline(never)] function the benchmark calls,"
        );
        println!("  and see DEVELOPING.md.");
        return true;
    }
    // Integers only. The decimal beside it is for reading.
    if hundredths >= MIN_APPLICATION_PCT * 100 {
        println!("{shard}{case_id}: {pct}% of {total} Ir in the binary under measurement");
        return false;
    }
    println!(
        "::error::{shard}{case_id}: the collected region is {pct}% application code ({app} of {total} Ir);"
    );
    println!(
        "  the rest is the C runtime, so this case is measuring the allocator rather than the code it names."
    );
    println!("  Profile: {where_}");
    println!(
        "  The usual cause is the measured work being written inline in the #[library_benchmark]"
    );
    println!(
        "  function, where the optimizer may reshape it out of the collected region. Move it into a"
    );
    println!("  named #[inline(never)] function the benchmark calls. See DEVELOPING.md.");
    true
}

// ── Discovery ──────────────────────────────────────────────────────────

/// Whether a name is a head-leg callgrind profile.
///
/// A saved baseline lands as `callgrind.<case>.out.<label>@<label>`, and judging
/// it fails a pull request for a bench its author did not write. `@` cannot
/// appear in a bench, group or case name.
fn is_profile(name: &str) -> bool {
    name.len() >= "callgrind.".len() + ".out".len()
        && name.starts_with("callgrind.")
        && name.ends_with(".out")
        && !name.contains('@')
}

/// Every directory under `base` holding at least one profile, as a path
/// relative to `base` and in the bytewise order the cases are judged. The empty
/// string names `base`.
///
/// A threaded case's parts are only a measurement together, so the unit is the
/// directory and each is judged once.
fn case_dirs(base: &Path) -> Result<BTreeSet<String>, Error> {
    let mut out = BTreeSet::new();
    walk(base, "", &mut out)?;
    Ok(out)
}

/// Descends `dir`, recording it when it holds a profile.
///
/// Symlinked directories are recorded and never descended into, so a loop
/// cannot hold the walk.
fn walk(dir: &Path, rel: &str, out: &mut BTreeSet<String>) -> Result<(), Error> {
    let read = std::fs::read_dir(dir).map_err(|e| Error::msg(format!("{}: {e}", dir.display())))?;
    let mut holds = false;
    let mut subdirs = Vec::new();
    for entry in read {
        let entry = entry.map_err(|e| Error::msg(format!("{}: {e}", dir.display())))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if is_profile(&name) {
            holds = true;
        }
        if entry.file_type().is_ok_and(|t| t.is_dir()) {
            let child = if rel.is_empty() {
                name
            } else {
                format!("{rel}/{name}")
            };
            subdirs.push((entry.path(), child));
        }
    }
    if holds {
        out.insert(rel.to_owned());
    }
    for (path, child) in subdirs {
        walk(&path, &child, out)?;
    }
    Ok(())
}

/// One case's parts, as (displayed path, path on disk) sorted bytewise by the
/// displayed path.
fn case_parts(case: &Path, case_dir: &str) -> Vec<(String, PathBuf)> {
    let mut out = Vec::new();
    if is_profile(basename(case_dir)) {
        out.push((case_dir.to_owned(), case.to_path_buf()));
    }
    let Ok(read) = std::fs::read_dir(case) else {
        return out;
    };
    for entry in read.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if is_profile(&name) {
            out.push((format!("{case_dir}/{name}"), entry.path()));
        }
    }
    out.sort();
    out
}

/// Every part's contents, or nothing where one could not be read.
fn read_parts(parts: &[(String, PathBuf)]) -> Result<Vec<String>, ()> {
    parts
        .iter()
        .map(|(_, path)| std::fs::read_to_string(path).map_err(drop))
        .collect()
}

/// The last path component, as `basename` reports it.
fn basename(path: &str) -> &str {
    if path.is_empty() {
        return path;
    }
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return "/";
    }
    trimmed.rsplit('/').next().unwrap_or(trimmed)
}

// ── The verdict ────────────────────────────────────────────────────────

/// One case's verdict.
#[derive(PartialEq, Debug)]
enum Verdict {
    /// The share in hundredths of a percent, the same share rendered for
    /// reading, the application instructions and the collected total.
    Ok {
        hundredths: i64,
        pct: String,
        app: i64,
        total: i64,
    },
    Refused(Refusal),
}

/// A profile this module declines to account for.
#[derive(PartialEq, Debug)]
struct Refusal {
    reason: &'static str,
    detail: Option<(i64, i64)>,
}

impl Refusal {
    fn bare(reason: &'static str) -> Self {
        Self {
            reason,
            detail: None,
        }
    }

    /// The three blank-separated fields the diagnostic names, padded where a
    /// refusal carries no detail.
    fn fields(&self) -> String {
        match self.detail {
            Some((a, b)) => format!("{} {a} {b}", self.reason),
            None => format!("{}  ", self.reason),
        }
    }
}

fn refused(reason: &'static str) -> Verdict {
    Verdict::Refused(Refusal::bare(reason))
}

/// Header state, which belongs to one part.
struct Part {
    /// How many position columns precede the event columns.
    npos: usize,
    /// Which event column carries `Ir`, counted from the first.
    iri: usize,
    /// The object a cost line is charged to. A part whose first cost line
    /// precedes its own `ob=` would otherwise inherit whatever the last part
    /// left.
    ob: String,
    /// What this part says it collected.
    declared: f64,
    /// Whether it said so at all.
    declared_here: bool,
    /// Whether the last position line was a call or a jump, whose following
    /// cost line belongs to the callee or the branch. Callgrind excludes both
    /// from its totals.
    pending: bool,
    names: HashMap<String, String>,
}

impl Default for Part {
    /// Callgrind's own header defaults, re-read per part: `positions: line`,
    /// and `Ir` first among the events.
    fn default() -> Self {
        Self {
            npos: 1,
            iri: 1,
            ob: String::new(),
            declared: 0.0,
            declared_here: false,
            pending: false,
            names: HashMap::new(),
        }
    }
}

/// One case's verdict, over the contents of its parts in the order the caller
/// found them.
///
/// Cost belongs to the case, so the attribution, the total and the declared
/// total accumulate across parts. Judged one part at a time, a thread that
/// declared `summary: 0` reads as a region that collected nothing.
fn read_case(parts: &[String]) -> Verdict {
    let mut ir: BTreeMap<String, f64> = BTreeMap::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut total = 0.0;
    let mut cmd = String::new();
    let mut no_ir = false;
    let mut declared_total = 0.0;
    let mut declared_files = 0usize;
    let mut seen_files = 0usize;
    let mut any_record = false;
    let mut part = Part::default();

    for text in parts {
        let mut first = true;
        for line in records(text) {
            if first {
                // The part just finished contributes its declared total to the
                // sum the aggregate is compared against.
                if any_record {
                    declared_total += part.declared;
                    declared_files += usize::from(part.declared_here);
                    part = Part::default();
                }
                seen_files += 1;
                first = false;
            }
            any_record = true;
            read_line(
                line, &mut part, &mut cmd, &mut no_ir, &mut ir, &mut seen, &mut total,
            );
        }
    }
    // The last part never hit the boundary rule above.
    declared_total += part.declared;
    declared_files += usize::from(part.declared_here);

    if cmd.is_empty() {
        return refused("no-cmd");
    }
    // Any part missing the Ir column is enough. Left unnamed this surfaces as a
    // totals mismatch, from summing the position field.
    if no_ir {
        return refused("no-ir-column");
    }
    // Zero across *every* part. One part at zero is ordinary: a thread that
    // never entered the collected region.
    if total == 0.0 {
        return refused("no-cost");
    }
    // Every part has to have declared a total. Counted against the parts the
    // *caller* counted: an empty file has no records, so a truncated part
    // beside a healthy one would slip through every check here.
    if seen_files != parts.len() {
        return refused("unreadable-part");
    }
    if declared_files != parts.len() {
        return refused("partial-summary");
    }
    if declared_total != total {
        return Verdict::Refused(Refusal {
            reason: "totals-mismatch",
            detail: Some((total as i64, declared_total as i64)),
        });
    }
    let Some(binary) = binary_object(&cmd, &seen) else {
        return refused("no-binary");
    };
    let app = ir.get(binary).copied().unwrap_or(0.0);
    Verdict::Ok {
        // Truncated rather than rounded: 9.999% reads as 999 and fails, where
        // rounding would let it through as 10%.
        hundredths: (10000.0 * app / total) as i64,
        pct: format!("{:.2}", 100.0 * app / total),
        app: app as i64,
        total: total as i64,
    }
}

/// The object whose path the command line starts with, longest match first so
/// that a prefix of another path cannot claim it.
///
/// Compared against every object the profile named, including one that carried
/// no cost. An ignore-list of `libc.so`-shaped names would fail open on an
/// unknown platform.
fn binary_object<'a>(cmd: &str, seen: &'a BTreeSet<String>) -> Option<&'a str> {
    let mut binary: Option<&str> = None;
    for o in seen {
        if cmd.starts_with(o.as_str()) && o.len() > binary.map_or(0, str::len) {
            binary = Some(o);
        }
    }
    binary
}

/// Folds one line of a part into the case.
fn read_line(
    line: &str,
    part: &mut Part,
    cmd: &mut String,
    no_ir: &mut bool,
    ir: &mut BTreeMap<String, f64>,
    seen: &mut BTreeSet<String>,
    total: &mut f64,
) {
    let f = fields(line);
    if let Some(rest) = line.strip_prefix("cmd:") {
        // Identical across every part of one case; the first is as good as any.
        if cmd.is_empty() {
            *cmd = rest.trim_start_matches([' ', '\t']).to_owned();
        }
    } else if line.starts_with("positions:") {
        part.npos = f.len().saturating_sub(1);
    } else if line.starts_with("events:") {
        part.iri = 0;
        for (i, name) in f.iter().enumerate() {
            if *name == "Ir" {
                part.iri = i;
            }
        }
        if part.iri == 0 {
            *no_ir = true;
        }
    } else if line.starts_with("summary:") || line.starts_with("totals:") {
        // What this part says it collected. `totals:` is the same quantity
        // under another name; a part carrying both carries them equal, so the
        // last one read is the part total.
        part.declared = numeric(field(line, &f, 1 + part.iri));
        part.declared_here = true;
    } else if let Some(v) = line.strip_prefix("ob=") {
        part.ob = deref(&mut part.names, v);
        seen.insert(part.ob.clone());
    } else if let Some(v) = line.strip_prefix("cob=") {
        seen.insert(deref(&mut part.names, v));
    } else if starts_with_any(line, &["fl=", "fi=", "fe=", "cfi=", "cfl=", "fn=", "cfn="]) {
        // A file or function name. Ids are per name kind, so one introduced
        // here never resolves an object reference, and the line carries no cost
        // and leaves any call exclusion standing.
    } else if starts_with_any(line, &["calls=", "jump=", "jcnd="]) {
        part.pending = true;
    } else if line.starts_with(|c: char| c.is_ascii_digit() || c == '*' || c == '+' || c == '-') {
        // A cost line: position field(s) then one column per declared event,
        // with trailing zero columns omitted.
        if part.pending {
            part.pending = false;
        } else {
            let cost = numeric(field(line, &f, part.npos + part.iri));
            *ir.entry(part.ob.clone()).or_default() += cost;
            *total += cost;
        }
    } else {
        part.pending = false;
    }
}

/// Resolves callgrind's name compression for an object: a position line may
/// introduce an id (`ob=(1) /lib/libc.so.6`) and later refer to it (`ob=(1)`).
///
/// `cob=` shares the namespace of `ob=`, so a name introduced on the called side
/// is recorded too. An id no part of this file introduced resolves to the empty
/// name.
fn deref(names: &mut HashMap<String, String>, v: &str) -> String {
    if !v.starts_with('(') {
        return v.to_owned();
    }
    let Some(end) = v.find(')') else {
        return v.to_owned();
    };
    let id = v[1..end].to_owned();
    let name = v[end + 1..].trim_start_matches([' ', '\t']);
    if !name.is_empty() {
        names.insert(id.clone(), name.to_owned());
    }
    names.get(&id).cloned().unwrap_or_default()
}

// ── Record, field and number conventions ───────────────────────────────

/// The records a profile holds: its lines, with no empty record after a
/// trailing newline.
fn records(text: &str) -> Vec<&str> {
    if text.is_empty() {
        return Vec::new();
    }
    text.strip_suffix('\n')
        .unwrap_or(text)
        .split('\n')
        .collect()
}

/// A line's fields: runs of blanks separate, and leading and trailing blanks
/// are ignored.
fn fields(line: &str) -> Vec<&str> {
    line.split([' ', '\t']).filter(|f| !f.is_empty()).collect()
}

/// The `n`th field counted from one, where zero names the whole line and an
/// absent field is empty.
fn field<'a>(line: &'a str, f: &[&'a str], n: usize) -> &'a str {
    match n.checked_sub(1) {
        None => line,
        Some(i) => f.get(i).copied().unwrap_or(""),
    }
}

/// The number a field carries: its longest numeric prefix, and zero where it
/// has none.
fn numeric(field: &str) -> f64 {
    let bytes = field.as_bytes();
    let mut i = 0;
    while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
        i += 1;
    }
    let start = i;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        i += 1;
    }
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i < bytes.len() && bytes[i] == b'.' {
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
    }
    // An exponent joins the number only when digits follow it.
    if i < bytes.len() && (bytes[i] | 0x20) == b'e' {
        let mut j = i + 1;
        if j < bytes.len() && (bytes[j] == b'+' || bytes[j] == b'-') {
            j += 1;
        }
        let exponent = j;
        while j < bytes.len() && bytes[j].is_ascii_digit() {
            j += 1;
        }
        if j > exponent {
            i = j;
        }
    }
    field[start..i].parse().unwrap_or(0.0)
}

/// Whether the line opens with any of `prefixes`.
fn starts_with_any(line: &str, prefixes: &[&str]) -> bool {
    prefixes.iter().any(|p| line.starts_with(p))
}

#[cfg(test)]
mod tests;
