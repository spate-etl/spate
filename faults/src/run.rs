//! The fault-run harness: generates a seeded data set, starts the containers
//! and the worker processes, sweeps the store's final state, and judges the
//! run with the oracle.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use spate_coordination::store::nats::NatsStore;
use spate_coordination::store::{CoordinationStore as _, Keyspace};
use spate_test_support::container_image;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

use crate::journal;
use crate::oracle::{
    self, GeneratedObject, GeneratedRecord, Inputs, ProcessJournal, StoreKind, SweptEntry,
};
use crate::outcome::{self, Evidence, Kind, LostReplies, Outcome, Scenario, Stage, Violation};
use crate::seaweed::Gateway;
use crate::seed::{self, SplitMix64};
use crate::worker::{S3Config, StoreConfig, Tuning, WorkerConfig, nats_store};
use crate::workers::Workers;

/// Environment variable holding the run seed, in decimal or `0x` hex.
pub const SEED_VAR: &str = "SPATE_FAULT_SEED";
/// Environment variable naming the directory run directories go under.
pub const RUN_DIR_VAR: &str = "SPATE_FAULT_RUN_DIR";

const OBJECTS: u64 = 10;
const MIB: u64 = 1024 * 1024;
const MIN_OBJECT: u64 = 48 * 1024;
const MAX_OBJECT: u64 = 5 * MIB / 2;
const BUCKET: &str = "spate-faults";
const JOB: &str = "faults";
const NATS_CLIENT_PORT: u16 = 4222;
/// Cap on starting the containers and creating the store.
const SETUP_DEADLINE: Duration = Duration::from_secs(60);
/// Cap on one store call the harness makes directly.
const STORE_CALL: Duration = Duration::from_secs(10);
/// How long the workers may run before the harness kills them.
const RUN_DEADLINE: Duration = Duration::from_secs(300);

/// One scenario over NATS.
#[derive(Clone, Copy, Debug)]
pub struct Spec<'a> {
    /// Scenario name; also the test's name.
    pub name: &'a str,
    /// Worker processes running at once.
    pub instances: u32,
    /// The worker binary.
    pub worker: &'a Path,
    /// How long each sink write is held.
    pub sink_delay_ms: u64,
    /// The scenario injects no faults, so a record written twice fails its
    /// expectation.
    pub fault_free: bool,
}

/// Runs `spec` and returns its outcome when every check held.
///
/// The run directory, `<run root>/<scenario>-<seed>`, keeps each worker's
/// config, journal and stderr, and `outcome.json`. A passing run keeps only
/// `outcome.json`.
///
/// # Panics
///
/// Panics with the outcome kind's prefix, the message and the replay command
/// when the scenario fails, and when the journals cannot be judged.
pub fn run(spec: &Spec<'_>) -> Outcome {
    let seed = run_seed();
    let dir = run_root().join(format!("{}-{seed:016x}", spec.name));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("create {}: {e}", dir.display()));
    let run = Run { spec, seed, dir };
    let mut rng = SplitMix64::for_scenario(seed, spec.name);
    let (generated, objects) = generate(&mut rng);
    let tuning = Tuning::nats();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("harness runtime");

    let setup = catch_unwind(AssertUnwindSafe(|| setup(&rt, &tuning, objects)));
    let env = match setup {
        Ok(Ok(env)) => env,
        Ok(Err(failure)) => return run.harness(Stage::Setup, &failure),
        Err(panic) => return run.harness(Stage::Setup, &panic_text(&*panic)),
    };

    let mut workers = Workers::default();
    let mut processes = Vec::new();
    for i in 0..spec.instances {
        let instance = format!("w{i}");
        match run.spawn(&mut workers, &env, &instance, &tuning) {
            Ok(process) => processes.push(process),
            Err(failure) => return run.harness(Stage::Setup, &failure),
        }
    }
    let timed_out = workers
        .wait(RUN_DEADLINE)
        .unwrap_or_else(|e| panic!("read worker status: {e}"));
    let exits = workers.exits();
    drop(workers);

    let sweep = match sweep(&rt, &env.direct) {
        Ok(sweep) => sweep,
        Err(failure) => return run.harness(Stage::Running, &failure),
    };
    let journals: Vec<ProcessJournal> = processes
        .into_iter()
        .map(|(instance, pid, path)| ProcessJournal {
            // A worker that failed before opening its journal wrote nothing.
            lines: if path.exists() {
                journal::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
            } else {
                Vec::new()
            },
            instance,
            pid,
        })
        .collect();
    let violations = oracle::check(&Inputs {
        store: StoreKind::Nats,
        generated: &generated,
        processes: &journals,
        sweep: &sweep,
        faults: &[],
        timing: tuning.timing(),
    })
    .unwrap_or_else(|e| panic!("the oracle could not judge the run: {e}"));
    let expectations = if spec.fault_free {
        duplicates(&journals)
    } else {
        Vec::new()
    };
    let (kind, message) = outcome::classify(&Evidence {
        setup_failure: None,
        scenario: &Scenario::Ordinary,
        worker_exits: &exits,
        timed_out,
        violations: &violations,
        expectations: &expectations,
        lost_replies: &LostReplies::default(),
        health: &[],
    });
    run.finish(Stage::Oracle, kind, message, violations, expectations)
}

/// The containers a run needs, and the harness's own store handle, which
/// crosses no fault.
struct Env {
    gateway: Gateway,
    _nats: Container<GenericImage>,
    nats_port: u16,
    direct: NatsStore,
}

fn setup(
    rt: &tokio::runtime::Runtime,
    tuning: &Tuning,
    objects: Vec<(String, Vec<u8>)>,
) -> Result<Env, String> {
    let gateway = Gateway::start(BUCKET)?;
    gateway.put_all(rt, objects)?;
    let (image, tag) = container_image(&["--pull", "nats"]);
    let nats = GenericImage::new(image, tag)
        .with_exposed_port(NATS_CLIENT_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["-js"])
        .start()
        .map_err(|e| format!("start NATS: {e}"))?;
    let nats_port = nats
        .get_host_port_ipv4(NATS_CLIENT_PORT)
        .map_err(|e| format!("NATS port: {e}"))?;
    let direct = nats_store(&nats_config(nats_port), tuning)?;
    // The first call creates the buckets with the workers' parameters.
    let until = Instant::now() + SETUP_DEADLINE;
    loop {
        let ready = rt.block_on(async {
            tokio::time::timeout(STORE_CALL, direct.get(Keyspace::Durable, "plan")).await
        });
        match ready {
            Ok(Ok(_)) => break,
            failure if Instant::now() >= until => {
                return Err(format!(
                    "the store was not ready within a minute: {failure:?}"
                ));
            }
            _ => std::thread::sleep(Duration::from_millis(200)),
        }
    }
    Ok(Env {
        gateway,
        _nats: nats,
        nats_port,
        direct,
    })
}

fn nats_config(port: u16) -> StoreConfig {
    StoreConfig::Nats {
        server: format!("nats://127.0.0.1:{port}"),
        job: JOB.to_owned(),
    }
}

/// The durable `spec.`, `split.`, `plan` and `verdict` entries.
fn sweep(rt: &tokio::runtime::Runtime, store: &NatsStore) -> Result<Vec<SweptEntry>, String> {
    let mut sweep = Vec::new();
    for prefix in ["spec.", "split.", "plan", "verdict"] {
        let entries = rt
            .block_on(async {
                tokio::time::timeout(STORE_CALL, store.list(Keyspace::Durable, prefix)).await
            })
            .map_err(|_| format!("the sweep of {prefix} timed out"))?
            .map_err(|e| format!("the sweep of {prefix} failed: {e}"))?;
        sweep.extend(entries.into_iter().map(|e| SweptEntry {
            key: e.key,
            rev: e.revision.0,
            value: e.value,
        }));
    }
    Ok(sweep)
}

/// One run's identity and directory.
struct Run<'a> {
    spec: &'a Spec<'a>,
    seed: u64,
    dir: PathBuf,
}

impl Run<'_> {
    /// Writes a worker's config and starts it, returning its instance, pid
    /// and journal path.
    fn spawn(
        &self,
        workers: &mut Workers,
        env: &Env,
        instance: &str,
        tuning: &Tuning,
    ) -> Result<(String, u32, PathBuf), String> {
        let name = format!("{instance}-1");
        let journal = self.dir.join(format!("{name}.ndjson"));
        let config = WorkerConfig {
            instance: instance.to_owned(),
            journal: journal.clone(),
            store: nats_config(env.nats_port),
            s3: S3Config {
                endpoint: env.gateway.endpoint(),
                bucket: env.gateway.bucket.clone(),
            },
            tuning: *tuning,
            sink_delay_ms: self.spec.sink_delay_ms,
        };
        let config_path = self.dir.join(format!("{name}.json"));
        let json = serde_json::to_vec_pretty(&config).map_err(|e| e.to_string())?;
        fs::write(&config_path, json).map_err(|e| format!("{}: {e}", config_path.display()))?;
        let stderr_path = self.dir.join(format!("{name}.stderr"));
        let stderr =
            File::create(&stderr_path).map_err(|e| format!("{}: {e}", stderr_path.display()))?;
        let pid = workers
            .spawn(
                instance,
                self.spec.worker,
                &[OsStr::new(&config_path)],
                stderr,
            )
            .map_err(|e| format!("start {}: {e}", self.spec.worker.display()))?;
        Ok((instance.to_owned(), pid, journal))
    }

    fn harness(&self, stage: Stage, failure: &str) -> Outcome {
        self.finish(
            stage,
            Kind::Harness,
            failure.to_owned(),
            Vec::new(),
            Vec::new(),
        )
    }

    /// Writes `outcome.json`, and panics unless the kind is
    /// [`Kind::Pass`].
    fn finish(
        &self,
        stage: Stage,
        kind: Kind,
        message: String,
        violations: Vec<Violation>,
        expectations: Vec<String>,
    ) -> Outcome {
        let spec = self.spec;
        let replay = format!(
            "cargo xtask fault-test --seed 0x{:016x} {}",
            self.seed, spec.name
        );
        let outcome = Outcome {
            scenario: spec.name.to_owned(),
            store: "nats".to_owned(),
            instances: spec.instances,
            seed: self.seed,
            replay,
            stage,
            kind,
            message,
            violations,
            expectations,
            faults_fired: Vec::new(),
        };
        let path = self.dir.join("outcome.json");
        let json = serde_json::to_vec_pretty(&outcome).expect("outcome serializes");
        fs::write(&path, json).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        if kind == Kind::Pass {
            self.keep_only(&path);
            return outcome;
        }
        panic!(
            "{} {}\nscenario {} on {}, {} instances, seed 0x{:016x}\nreplay: {}\nrun directory: {}",
            kind.panic_prefix(),
            outcome.message,
            outcome.scenario,
            outcome.store,
            outcome.instances,
            outcome.seed,
            outcome.replay,
            self.dir.display()
        );
    }

    /// Removes every file in the run directory but `keep`.
    fn keep_only(&self, keep: &Path) {
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return;
        };
        for entry in entries.flatten() {
            if entry.path() != keep {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
}

fn run_seed() -> u64 {
    seed_from(|name| std::env::var(name).ok())
}

/// The run seed from [`SEED_VAR`], or one drawn from the clock and printed.
fn seed_from(var: impl Fn(&str) -> Option<String>) -> u64 {
    match var(SEED_VAR) {
        Some(text) => {
            seed::parse(&text).unwrap_or_else(|| panic!("{SEED_VAR}={text} is not a seed"))
        }
        None => {
            let seed = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos() as u64);
            eprintln!("fault-run seed 0x{seed:016x}");
            seed
        }
    }
}

fn run_root() -> PathBuf {
    std::env::var_os(RUN_DIR_VAR).map_or_else(
        || Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/fault-runs"),
        PathBuf::from,
    )
}

/// The data set for `rng`: objects under `data/` holding JSON records
/// `{"k": <id>, "pad": <letters>}`, one per line. The first object is below
/// 1 MiB and the second above it, so whole-object packing and byte-range
/// splits both occur.
fn generate(rng: &mut SplitMix64) -> (Vec<GeneratedObject>, Vec<(String, Vec<u8>)>) {
    let mut generated = Vec::new();
    let mut objects = Vec::new();
    for o in 0..OBJECTS {
        let key = format!("data/o{o:03}.ndjson");
        let target = match o {
            0 => rng.in_range(MIN_OBJECT, MIB - 510),
            1 => rng.in_range(MIB + 1, MAX_OBJECT),
            _ => rng.in_range(MIN_OBJECT, MAX_OBJECT),
        };
        let mut body = Vec::new();
        let mut records = Vec::new();
        let mut r = 0_u64;
        while (body.len() as u64) < target {
            let id = format!("o{o:03}-r{r:06}");
            let pad: String = (0..rng.in_range(32, 480))
                .map(|_| char::from(b'a' + (rng.next_u64() % 26) as u8))
                .collect();
            records.push(GeneratedRecord {
                id: id.clone(),
                offset: body.len() as u64,
            });
            body.extend_from_slice(format!(r#"{{"k":"{id}","pad":"{pad}"}}"#).as_bytes());
            body.push(b'\n');
            r += 1;
        }
        generated.push(GeneratedObject {
            key: key.clone(),
            records,
        });
        objects.push((key, body));
    }
    (generated, objects)
}

/// A failed expectation for each record any sink wrote more than once.
fn duplicates(journals: &[ProcessJournal]) -> Vec<String> {
    let mut counts: HashMap<&str, u32> = HashMap::new();
    for line in journals.iter().flat_map(|j| &j.lines) {
        if let journal::Event::Rows { ids } = &line.event {
            for id in ids {
                *counts.entry(id).or_default() += 1;
            }
        }
    }
    let mut twice: Vec<&str> = counts
        .into_iter()
        .filter_map(|(id, n)| (n > 1).then_some(id))
        .collect();
    twice.sort_unstable();
    match twice.first() {
        None => Vec::new(),
        Some(first) => vec![format!(
            "a fault-free run wrote {} records more than once, first {first}",
            twice.len()
        )],
    }
}

fn panic_text(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).to_owned()))
        .unwrap_or_else(|| "setup panicked".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{Event, Line};

    /// The data set is a function of the seed, its first object lies below
    /// 1 MiB and its second above, and each record's offset is where its line
    /// starts.
    #[test]
    fn data_set_replays_from_the_seed_and_spans_one_mib() {
        let (generated, objects) = generate(&mut SplitMix64::new(7));
        assert_eq!(objects, generate(&mut SplitMix64::new(7)).1);
        assert_ne!(objects, generate(&mut SplitMix64::new(8)).1);
        assert!((objects[0].1.len() as u64) < MIB);
        assert!((objects[1].1.len() as u64) > MIB);
        for (object, (key, body)) in generated.iter().zip(&objects) {
            assert_eq!(&object.key, key);
            for record in &object.records {
                let line = &body[usize::try_from(record.offset).unwrap()..];
                let prefix = format!(r#"{{"k":"{}","#, record.id);
                assert!(line.starts_with(prefix.as_bytes()), "{}", record.id);
            }
        }
    }

    /// For several seeds, the first object lies below 1 MiB and the second above it.
    #[test]
    fn data_set_spans_one_mib_across_seeds() {
        for seed in (0..8).chain([1399]) {
            let (_, objects) = generate(&mut SplitMix64::for_scenario(
                seed,
                "nats_no_faults_writes_no_duplicates",
            ));
            let sizes = (objects[0].1.len() as u64, objects[1].1.len() as u64);
            assert!(sizes.0 < MIB && sizes.1 > MIB, "seed {seed}: {sizes:?}");
        }
    }

    /// A passing run keeps only `outcome.json`, and its replay command passes
    /// the seed through `--seed`.
    #[test]
    fn a_passing_run_keeps_its_outcome_and_replays_its_seed() {
        let dir = std::env::temp_dir().join(format!("spate-faults-finish-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("w0-1.ndjson"), "").unwrap();
        let spec = Spec {
            name: "s",
            instances: 1,
            worker: Path::new("w"),
            sink_delay_ms: 0,
            fault_free: true,
        };
        let run = Run {
            spec: &spec,
            seed: 0xff,
            dir: dir.clone(),
        };
        let outcome = run.finish(
            Stage::Oracle,
            Kind::Pass,
            String::new(),
            Vec::new(),
            Vec::new(),
        );
        let left: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        fs::remove_dir_all(&dir).unwrap();
        assert_eq!(
            outcome.replay,
            "cargo xtask fault-test --seed 0x00000000000000ff s"
        );
        assert_eq!(left, ["outcome.json"]);
    }

    /// The seed variable is the one `cargo xtask fault-test` sets.
    #[test]
    fn seed_variable_matches_xtask() {
        assert_eq!(SEED_VAR, "SPATE_FAULT_SEED");
    }

    /// The run seed is read from `SPATE_FAULT_SEED`.
    #[test]
    fn run_seed_reads_spate_fault_seed() {
        let seed = seed_from(|name| (name == "SPATE_FAULT_SEED").then(|| "0xff".to_owned()));
        assert_eq!(seed, 0xff);
    }

    /// A record written twice, by one process or two, fails the fault-free
    /// expectation; records written once do not.
    #[test]
    fn fault_free_expectation_counts_records_written_twice() {
        let journal = |pid, ids: &[&str]| ProcessJournal {
            instance: format!("w{pid}"),
            pid,
            lines: vec![Line {
                t_ms: 1,
                event: Event::Rows {
                    ids: ids.iter().map(|s| (*s).to_owned()).collect(),
                },
            }],
        };
        assert!(duplicates(&[journal(1, &["a", "b"]), journal(2, &["c"])]).is_empty());
        assert_eq!(
            duplicates(&[journal(1, &["b", "a", "b"]), journal(2, &["a"])]),
            ["a fault-free run wrote 2 records more than once, first a"]
        );
    }
}
