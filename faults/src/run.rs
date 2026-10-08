//! The fault-run harness: generates a seeded data set, starts the containers
//! and the worker processes, kills and replaces workers on the seeded
//! schedule, replaces workers that abort on their in-process fault, sweeps the
//! store's final state, and judges the run with the oracle.

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::{self, File};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use spate_coordination::store::dynamodb::DynamoDbStore;
use spate_coordination::store::nats::NatsStore;
use spate_coordination::store::{CoordinationStore as _, Entry, Keyspace, StoreError};
use spate_test_support::container_image;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage, ImageExt};

use crate::expect;
use crate::health::{self, Target};
use crate::journal::{self, Event, Journal, Reply};
use crate::oracle::{
    self, GeneratedObject, GeneratedRecord, Inputs, ProcessJournal, StoreKind, SweptEntry,
};
use crate::outcome::{self, Evidence, FaultFired, Kind, Outcome, Scenario, Stage, Violation};
use crate::schedule::{Action, InProcess, Schedule, Step, Timeline};
use crate::seaweed::Gateway;
use crate::seed::{self, SplitMix64};
use crate::store::AbortMode;
use crate::worker::{S3Config, StoreConfig, Tuning, WorkerConfig, dynamodb_store, nats_store};
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
const TABLE: &str = "spate-faults";
const NATS_CLIENT_PORT: u16 = 4222;
const DYNAMODB_PORT: u16 = 8000;
/// Cap on starting the containers and creating the store.
const SETUP_DEADLINE: Duration = Duration::from_secs(60);
/// Cap on one store call the harness makes directly.
const STORE_CALL: Duration = Duration::from_secs(10);
/// How long the workers may run before the harness kills them.
const RUN_DEADLINE: Duration = Duration::from_secs(300);
/// How often the harness checks the workers between scheduled steps.
const POLL: Duration = Duration::from_millis(50);
/// How often each container's health is polled.
const HEALTH_INTERVAL: Duration = Duration::from_secs(1);
/// Cap on one health probe.
const PROBE: Duration = Duration::from_secs(2);

/// One scenario.
#[derive(Clone, Copy, Debug)]
pub struct Spec<'a> {
    /// Scenario name; also the test's name.
    pub name: &'a str,
    /// The coordination store.
    pub store: StoreKind,
    /// Worker processes running at once.
    pub instances: u32,
    /// The worker binary.
    pub worker: &'a Path,
    /// How long each sink write is held.
    pub sink_delay_ms: u64,
    /// The scenario injects no faults, so a record written twice fails its
    /// expectation. Otherwise the seed draws a kill schedule and in-process
    /// faults.
    pub fault_free: bool,
}

/// Runs `spec` and returns its outcome when every check held.
///
/// The run directory, `<run root>/<scenario>-<seed>`, keeps each worker's
/// config, journal and stderr, the harness's `faults.ndjson` and
/// `health.ndjson`, and `outcome.json`. A passing run keeps only
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
    let mut rng = SplitMix64::for_scenario(seed, spec.name);
    let (generated, objects) = generate(&mut rng);
    let tuning = match spec.store {
        StoreKind::Nats => Tuning::nats(),
        StoreKind::DynamoDb => Tuning::dynamodb(),
    };
    let schedule = if spec.fault_free {
        Schedule::default()
    } else {
        Schedule::draw(&mut rng, spec.instances, tuning.lease_ms)
    };
    eprintln!(
        "fault-run schedule for {}:\n{}",
        spec.name,
        schedule.render()
    );
    let run = Run {
        spec,
        seed,
        dir,
        schedule,
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("harness runtime");

    let setup = catch_unwind(AssertUnwindSafe(|| {
        setup(&rt, spec.store, &tuning, objects)
    }));
    let env = match setup {
        Ok(Ok(env)) => env,
        Ok(Err(failure)) => return run.harness(Stage::Setup, &failure),
        Err(panic) => return run.harness(Stage::Setup, &panic_text(&*panic)),
    };

    let mut workers = Workers::default();
    let mut processes = Vec::new();
    let faults_path = run.dir.join("faults.ndjson");
    let targets = env.health_targets(&rt);
    let (driven, polls) = std::thread::scope(|s| {
        // Inside the scope, so a panic while driving drops `stop` and the
        // poller ends instead of holding the scope open.
        let (stop, stopped) = mpsc::channel::<()>();
        let (targets, path) = (&targets, run.dir.join("health.ndjson"));
        let poller = s.spawn(move || health::watch(targets, &path, HEALTH_INTERVAL, &stopped));
        let driven = run.drive(&mut workers, &mut processes, &env, &tuning, &faults_path);
        drop(stop);
        (driven, poller.join().expect("the health poller panicked"))
    });
    let (timed_out, mut fired) = match driven {
        Ok(driven) => driven,
        Err(failure) => return run.harness(Stage::Running, &failure),
    };
    let polls = match polls {
        Ok(polls) => polls,
        Err(e) => return run.harness(Stage::Running, &format!("health.ndjson: {e}")),
    };
    let exits = workers.exits();
    drop(workers);

    let sweep = match sweep(&rt, &env.direct) {
        Ok(sweep) => sweep,
        Err(failure) => return run.harness(Stage::Running, &failure),
    };
    fired.extend(in_process_fired(&run.schedule, &processes));
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
    let faults = if faults_path.exists() {
        journal::read(&faults_path)
            .unwrap_or_else(|e| panic!("read {}: {e}", faults_path.display()))
    } else {
        Vec::new()
    };
    let violations = oracle::check(&Inputs {
        store: spec.store,
        generated: &generated,
        processes: &journals,
        sweep: &sweep,
        faults: &faults,
        timing: tuning.timing(),
    })
    .unwrap_or_else(|e| panic!("the oracle could not judge the run: {e}"));
    let mut expectations = if spec.fault_free {
        duplicates(&journals)
    } else {
        Vec::new()
    };
    if spec.instances > 1 && !claims_overlap(&journals) {
        expectations.push("no two processes held splits at overlapping times".to_owned());
    }
    if !timed_out {
        expectations.extend(unreplaced_aborts(&journals));
    }
    let lost_replies = expect::lost_replies(&journals, run.schedule.lost_reply().is_some());
    let (kind, message) = outcome::classify(&Evidence {
        setup_failure: None,
        scenario: &Scenario::Ordinary,
        worker_exits: &exits,
        timed_out,
        violations: &violations,
        expectations: &expectations,
        lost_replies: &lost_replies,
        health: &polls,
    });
    run.finish(
        Stage::Oracle,
        kind,
        message,
        violations,
        expectations,
        fired,
    )
}

/// The containers a run needs, the store config workers get, and the
/// harness's own store handle, which crosses no fault.
struct Env {
    gateway: Gateway,
    store: Container<GenericImage>,
    store_name: &'static str,
    store_config: StoreConfig,
    direct: Direct,
}

/// The harness's own handle on either store.
enum Direct {
    Nats(NatsStore),
    DynamoDb(DynamoDbStore),
}

impl Direct {
    async fn get(&self, key: &str) -> Result<Option<Entry>, StoreError> {
        match self {
            Direct::Nats(s) => s.get(Keyspace::Durable, key).await,
            Direct::DynamoDb(s) => s.get(Keyspace::Durable, key).await,
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        match self {
            Direct::Nats(s) => s.list(Keyspace::Durable, prefix).await,
            Direct::DynamoDb(s) => s.list(Keyspace::Durable, prefix).await,
        }
    }
}

impl Env {
    /// Each container, probed over the harness's own connections.
    fn health_targets<'a>(&'a self, rt: &'a tokio::runtime::Runtime) -> Vec<Target<'a>> {
        vec![
            Target {
                container: self.store_name,
                running: Box::new(|| self.store.is_running().map_err(|e| e.to_string())),
                reach: Box::new(move || {
                    match rt.block_on(async {
                        tokio::time::timeout(PROBE, self.direct.get("plan")).await
                    }) {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(e)) => Err(e.to_string()),
                        Err(_) => Err(format!("no answer within {PROBE:?}")),
                    }
                }),
            },
            Target {
                container: "seaweedfs",
                running: Box::new(|| self.gateway.is_running()),
                reach: Box::new(move || self.gateway.probe(rt, PROBE)),
            },
        ]
    }
}

fn setup(
    rt: &tokio::runtime::Runtime,
    kind: StoreKind,
    tuning: &Tuning,
    objects: Vec<(String, Vec<u8>)>,
) -> Result<Env, String> {
    let gateway = Gateway::start(BUCKET)?;
    gateway.put_all(rt, objects)?;
    let _runtime = rt.enter();
    let (store, store_name, store_config, direct) = match kind {
        StoreKind::Nats => {
            let (image, tag) = container_image(&["--pull", "nats"]);
            let nats = GenericImage::new(image, tag)
                .with_exposed_port(NATS_CLIENT_PORT.tcp())
                .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
                .with_cmd(["-js"])
                .start()
                .map_err(|e| format!("start NATS: {e}"))?;
            let port = nats
                .get_host_port_ipv4(NATS_CLIENT_PORT)
                .map_err(|e| format!("NATS port: {e}"))?;
            let config = StoreConfig::Nats {
                server: format!("nats://127.0.0.1:{port}"),
                job: JOB.to_owned(),
            };
            let direct = Direct::Nats(nats_store(&config, tuning)?);
            (nats, "nats", config, direct)
        }
        StoreKind::DynamoDb => {
            let (image, tag) = container_image(&["--pull", "dynamodb"]);
            let local = GenericImage::new(image, tag)
                .with_exposed_port(DYNAMODB_PORT.tcp())
                .with_wait_for(WaitFor::message_on_stdout("Initializing DynamoDB Local"))
                .with_cmd(["-jar", "DynamoDBLocal.jar", "-inMemory"])
                .start()
                .map_err(|e| format!("start DynamoDB Local: {e}"))?;
            let port = local
                .get_host_port_ipv4(DYNAMODB_PORT)
                .map_err(|e| format!("DynamoDB Local port: {e}"))?;
            let config = StoreConfig::DynamoDb {
                endpoint: format!("http://127.0.0.1:{port}"),
                table: TABLE.to_owned(),
                job: JOB.to_owned(),
            };
            let direct = Direct::DynamoDb(dynamodb_store(&config, tuning)?);
            (local, "dynamodb", config, direct)
        }
    };
    // The first call creates the NATS buckets or the DynamoDB table with the
    // workers' parameters.
    let until = Instant::now() + SETUP_DEADLINE;
    loop {
        let ready =
            rt.block_on(async { tokio::time::timeout(STORE_CALL, direct.get("plan")).await });
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
        store,
        store_name,
        store_config,
        direct,
    })
}

/// The durable `spec.`, `split.`, `plan` and `verdict` entries.
fn sweep(rt: &tokio::runtime::Runtime, store: &Direct) -> Result<Vec<SweptEntry>, String> {
    let mut sweep = Vec::new();
    for prefix in ["spec.", "split.", "plan", "verdict"] {
        let entries = rt
            .block_on(async { tokio::time::timeout(STORE_CALL, store.list(prefix)).await })
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
    schedule: Schedule,
}

impl Run<'_> {
    /// Starts the workers and applies the schedule on real time until every
    /// worker has exited with no replacement due, or [`RUN_DEADLINE`] from
    /// their start passes. A worker that aborts on its plan is replaced after
    /// the plan's delay, and a kill due on the `ErrAfterLand` process waits
    /// until its journal shows the lost reply recovered, or one lease past its
    /// `err_after_land` line. Returns whether one was still running at the
    /// deadline, and each kill the timeline handed out or left pending.
    fn drive(
        &self,
        workers: &mut Workers,
        processes: &mut Vec<(String, u32, PathBuf)>,
        env: &Env,
        tuning: &Tuning,
        faults_path: &Path,
    ) -> Result<(bool, Vec<FaultFired>), String> {
        let faults =
            Journal::open(faults_path).map_err(|e| format!("{}: {e}", faults_path.display()))?;
        let log = |event| {
            faults
                .append(event)
                .map_err(|e| format!("{}: {e}", faults_path.display()))
        };
        let instances = self.spec.instances as usize;
        let start = Instant::now();
        let until = start + RUN_DEADLINE;
        let mut incarnations = vec![1; instances];
        for i in 0..self.spec.instances {
            processes.push(self.spawn(workers, env, i, 1, tuning)?);
        }
        let status = |e: std::io::Error| format!("read worker status: {e}");
        let mut timeline = Timeline::new(&self.schedule);
        let mut fired = Vec::new();
        // Per instance: its current process's abort was judged, when its
        // `err_after_land` line was first seen, and its held kill was released.
        let mut judged = vec![false; instances];
        let mut line_at: Vec<Option<u64>> = vec![None; instances];
        let mut released = vec![false; instances];
        let timed_out = loop {
            let now = Instant::now();
            let now_ms = millis(now - start);
            let all_exited = workers.try_wait().map_err(status)?;
            for i in 0..self.spec.instances {
                let at = i as usize;
                if judged[at] {
                    continue;
                }
                let Some(plan) = self.schedule.plan_for(i, incarnations[at]) else {
                    continue;
                };
                let name = format!("w{i}");
                if workers.live(&name).map_err(status)?.is_some() {
                    continue;
                }
                judged[at] = true;
                let journal = self.journal_path(i, incarnations[at]);
                let journalled = || journal_holds(&journal, "abort");
                if let Some(at_ms) = abort_respawn(plan, workers.signal(&name), journalled, now_ms)
                {
                    workers.mark_scheduled(&name);
                    timeline.respawn(i, at_ms);
                }
            }
            if now >= until {
                break !workers.try_wait().map_err(status)?;
            }
            if all_exited && !timeline.awaiting_respawn() {
                break false;
            }
            while let Some(step) = timeline.next(now_ms, |i| {
                let at = i as usize;
                if !kill_held(&self.schedule, i, incarnations[at], released[at]) {
                    return false;
                }
                let path = self.journal_path(i, incarnations[at]);
                if line_at[at].is_none() && journal_holds(&path, "err_after_land") {
                    line_at[at] = Some(now_ms);
                }
                released[at] = hold_released(line_at[at], now_ms, tuning.lease_ms, || {
                    recovery_journalled(&path, &format!("w{i}"))
                });
                !released[at]
            }) {
                match step {
                    Step::Kill {
                        at_ms,
                        instance,
                        respawn_at_ms,
                    } => {
                        let name = format!("w{instance}");
                        let live = workers.live(&name).map_err(status)?;
                        if let Some(pid) = live {
                            log(Event::Kill {
                                instance: name.clone(),
                                pid,
                            })?;
                            workers
                                .kill(&name)
                                .map_err(|e| format!("kill {name}: {e}"))?;
                            timeline.respawn(instance, respawn_at_ms);
                        }
                        fired.push(FaultFired {
                            incarnation: format!("{name}-{}", incarnations[instance as usize]),
                            fault: format!("kill at {at_ms} ms"),
                            fired: live.is_some(),
                        });
                    }
                    Step::Respawn { instance, .. } => {
                        let at = instance as usize;
                        incarnations[at] += 1;
                        judged[at] = false;
                        let process =
                            self.spawn(workers, env, instance, incarnations[at], tuning)?;
                        log(Event::Respawn {
                            instance: process.0.clone(),
                            pid: process.1,
                        })?;
                        processes.push(process);
                    }
                }
            }
            std::thread::sleep(POLL);
        };
        for kill in timeline.drain_kills() {
            if let Action::Kill { at_ms, instance } = kill {
                fired.push(FaultFired {
                    incarnation: format!("w{instance}-{}", incarnations[instance as usize]),
                    fault: format!("kill at {at_ms} ms"),
                    fired: false,
                });
            }
        }
        Ok((timed_out, fired))
    }

    /// The journal of `instance`'s `incarnation`.
    fn journal_path(&self, instance: u32, incarnation: u32) -> PathBuf {
        self.dir.join(format!("w{instance}-{incarnation}.ndjson"))
    }

    /// Writes the config of `instance`'s `incarnation` and starts it,
    /// returning its instance id, pid and journal path.
    fn spawn(
        &self,
        workers: &mut Workers,
        env: &Env,
        instance: u32,
        incarnation: u32,
        tuning: &Tuning,
    ) -> Result<(String, u32, PathBuf), String> {
        let journal = self.journal_path(instance, incarnation);
        let abort = self
            .schedule
            .plan_for(instance, incarnation)
            .map(|p| p.plan);
        let instance = format!("w{instance}");
        let name = format!("{instance}-{incarnation}");
        let config = WorkerConfig {
            instance: instance.clone(),
            journal: journal.clone(),
            store: env.store_config.clone(),
            s3: S3Config {
                endpoint: env.gateway.endpoint(),
                bucket: env.gateway.bucket.clone(),
            },
            tuning: *tuning,
            sink_delay_ms: self.spec.sink_delay_ms,
            abort,
        };
        let config_path = self.dir.join(format!("{name}.json"));
        let json = serde_json::to_vec_pretty(&config).map_err(|e| e.to_string())?;
        fs::write(&config_path, json).map_err(|e| format!("{}: {e}", config_path.display()))?;
        let stderr_path = self.dir.join(format!("{name}.stderr"));
        let stderr =
            File::create(&stderr_path).map_err(|e| format!("{}: {e}", stderr_path.display()))?;
        let pid = workers
            .spawn(
                &instance,
                self.spec.worker,
                &[OsStr::new(&config_path)],
                stderr,
            )
            .map_err(|e| format!("start {}: {e}", self.spec.worker.display()))?;
        Ok((instance, pid, journal))
    }

    fn harness(&self, stage: Stage, failure: &str) -> Outcome {
        self.finish(
            stage,
            Kind::Harness,
            failure.to_owned(),
            Vec::new(),
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
        faults_fired: Vec<FaultFired>,
    ) -> Outcome {
        let spec = self.spec;
        let replay = format!(
            "cargo xtask fault-test --seed 0x{:016x} {}",
            self.seed, spec.name
        );
        let outcome = Outcome {
            scenario: spec.name.to_owned(),
            store: match spec.store {
                StoreKind::Nats => "nats",
                StoreKind::DynamoDb => "dynamodb",
            }
            .to_owned(),
            instances: spec.instances,
            seed: self.seed,
            replay,
            stage,
            kind,
            message,
            violations,
            expectations,
            faults_fired,
        };
        let path = self.dir.join("outcome.json");
        let json = serde_json::to_vec_pretty(&outcome).expect("outcome serializes");
        fs::write(&path, json).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        if kind == Kind::Pass {
            self.keep_only(&path);
            return outcome;
        }
        panic!(
            "{} {}\nscenario {} on {}, {} instances, seed 0x{:016x}\nreplay: {}\nschedule:\n{}run directory: {}",
            kind.panic_prefix(),
            outcome.message,
            outcome.scenario,
            outcome.store,
            outcome.instances,
            outcome.seed,
            outcome.replay,
            self.schedule.render(),
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

/// Whether two processes landed writes as a split's owner at overlapping
/// times, each process spanning its first to its last such write.
fn claims_overlap(journals: &[ProcessJournal]) -> bool {
    let spans: Vec<(u64, u64)> = journals
        .iter()
        .filter_map(|j| {
            let mut owned = HashSet::new();
            let mut span: Option<(u64, u64)> = None;
            for line in &j.lines {
                match &line.event {
                    Event::Send { call, value, .. }
                        if value.owner.as_deref() == Some(j.instance.as_str()) =>
                    {
                        owned.insert(*call);
                    }
                    Event::Done {
                        call,
                        reply: Reply::Won(_),
                        ..
                    } if owned.contains(call) => {
                        span = Some((span.map_or(line.t_ms, |s| s.0), line.t_ms));
                    }
                    _ => {}
                }
            }
            span
        })
        .collect();
    spans
        .iter()
        .enumerate()
        .any(|(i, a)| spans[i + 1..].iter().any(|b| a.0 <= b.1 && b.0 <= a.1))
}

/// A failed expectation for each process, of `journals` in start order, that
/// journalled an `abort` line and has no later process under its instance id.
fn unreplaced_aborts(journals: &[ProcessJournal]) -> Vec<String> {
    journals
        .iter()
        .enumerate()
        .filter(|(at, j)| {
            j.lines
                .iter()
                .any(|l| matches!(l.event, Event::Abort { .. }))
                && !journals[at + 1..].iter().any(|r| r.instance == j.instance)
        })
        .map(|(_, j)| {
            format!(
                "{} (pid {}) aborted on its plan and was never replaced",
                j.instance, j.pid
            )
        })
        .collect()
}

/// When an ended process that carried `plan` is replaced: one respawn delay
/// after `now_ms` when it ended on SIGABRT and `journalled` finds its `abort`
/// line, and never otherwise.
fn abort_respawn(
    plan: &InProcess,
    signal: Option<i32>,
    journalled: impl FnOnce() -> bool,
    now_ms: u64,
) -> Option<u64> {
    let aborts = matches!(plan.plan.mode, AbortMode::Before | AbortMode::After);
    (aborts && signal == Some(libc::SIGABRT) && journalled())
        .then(|| now_ms + plan.respawn_after_ms)
}

/// Whether the `instance`'s `incarnation` must not be killed yet: it carries
/// the `ErrAfterLand` plan and its hold has not been released.
fn kill_held(schedule: &Schedule, instance: u32, incarnation: u32, released: bool) -> bool {
    !released
        && schedule
            .plan_for(instance, incarnation)
            .is_some_and(|p| p.plan.mode == AbortMode::ErrAfterLand)
}

/// Whether a kill held for the lost-reply process may go: one lease has
/// passed since its `err_after_land` line was seen at `line_at_ms`, or
/// `recovered` says its journal shows the landed write recovered.
fn hold_released(
    line_at_ms: Option<u64>,
    now_ms: u64,
    lease_ms: u64,
    recovered: impl FnOnce() -> bool,
) -> bool {
    line_at_ms.is_some_and(|at| now_ms >= at.saturating_add(lease_ms) || recovered())
}

/// Whether the journal `instance` writes at `path` shows the landed write
/// recovered after each lost reply.
fn recovery_journalled(path: &Path, instance: &str) -> bool {
    journal::read(path).is_ok_and(|lines| {
        expect::recovery_shown(&ProcessJournal {
            instance: instance.to_owned(),
            pid: 0,
            lines,
        })
    })
}

/// Whether the journal at `path` holds a line of event `ev`.
fn journal_holds(path: &Path, ev: &str) -> bool {
    std::fs::read(path).is_ok_and(|bytes| {
        let needle = format!(r#""ev":"{ev}""#);
        bytes.windows(needle.len()).any(|w| w == needle.as_bytes())
    })
}

/// Each in-process fault the schedule drew, and whether its process
/// journalled it: an `abort` line for an abort, an `err_after_land` line for
/// a lost reply.
fn in_process_fired(schedule: &Schedule, processes: &[(String, u32, PathBuf)]) -> Vec<FaultFired> {
    schedule
        .in_process
        .iter()
        .map(|p| {
            let incarnation = format!("w{}-1", p.instance);
            let ev = match p.plan.mode {
                AbortMode::ErrAfterLand => "err_after_land",
                AbortMode::Before | AbortMode::After => "abort",
            };
            let fired = processes.iter().any(|(_, _, path)| {
                path.file_stem() == Some(OsStr::new(&incarnation)) && journal_holds(path, ev)
            });
            FaultFired {
                incarnation,
                fault: p.plan.to_string(),
                fired,
            }
        })
        .collect()
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
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
            store: StoreKind::Nats,
            instances: 1,
            worker: Path::new("w"),
            sink_delay_ms: 0,
            fault_free: true,
        };
        let run = Run {
            spec: &spec,
            seed: 0xff,
            dir: dir.clone(),
            schedule: Schedule::default(),
        };
        let outcome = run.finish(
            Stage::Oracle,
            Kind::Pass,
            String::new(),
            Vec::new(),
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

    /// Claims overlap when two processes each landed a write as owner between
    /// the other's first and last such write; lost writes and writes owned by
    /// another instance do not count.
    #[test]
    fn claims_overlap_needs_two_owners_landing_at_once() {
        use crate::journal::{Progress, Status, WriteOp};
        let write = |instance: &str, call, t_ms, owner: &str, reply| {
            let value = Progress {
                schema: journal::SCHEMA,
                epoch: 1,
                owner: Some(owner.to_owned()),
                watermark: None,
                completed: false,
                status: Status::Runnable,
                attempts: 0,
            };
            let key = format!("split.{instance}");
            [
                Line {
                    t_ms,
                    event: Event::Send {
                        call,
                        op: WriteOp::Update,
                        key: key.clone(),
                        expected: Some(1),
                        value,
                    },
                },
                Line {
                    t_ms,
                    event: Event::Done { call, key, reply },
                },
            ]
        };
        let process = |instance: &str, pid, writes: Vec<[Line; 2]>| ProcessJournal {
            instance: instance.to_owned(),
            pid,
            lines: writes.into_iter().flatten().collect(),
        };
        let a = process(
            "w0",
            1,
            vec![
                write("w0", 1, 10, "w0", Reply::Won(2)),
                write("w0", 2, 30, "w0", Reply::Won(3)),
            ],
        );
        let during = process("w1", 2, vec![write("w1", 1, 20, "w1", Reply::Won(2))]);
        let after = process("w1", 3, vec![write("w1", 1, 40, "w1", Reply::Won(2))]);
        let lost = process("w1", 4, vec![write("w1", 1, 20, "w1", Reply::Lost)]);
        let foreign = process("w1", 5, vec![write("w1", 1, 20, "w0", Reply::Won(2))]);
        assert!(claims_overlap(&[a.clone(), during]));
        assert!(!claims_overlap(&[a.clone(), after]));
        assert!(!claims_overlap(&[a.clone(), lost]));
        assert!(!claims_overlap(&[a, foreign]));
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

    /// A kill due on the process carrying the `ErrAfterLand` plan waits until
    /// its hold is released, and its replacement waits one respawn delay from
    /// then; kills on other processes fall on time.
    #[test]
    fn err_after_land_incarnation_is_killed_only_after_its_release() {
        use crate::schedule::Kill;
        use crate::store::AbortPlan;
        let lost = InProcess {
            instance: 0,
            plan: AbortPlan {
                kind: crate::classify::WriteKind::Commit,
                n: 1,
                mode: AbortMode::ErrAfterLand,
            },
            respawn_after_ms: 0,
        };
        let kill = |at_ms, instance| Kill {
            at_ms,
            instance,
            respawn_after_ms: 100,
        };
        let schedule = Schedule {
            kills: vec![kill(1_000, 0), kill(1_000, 1)],
            in_process: vec![lost],
        };
        assert!(kill_held(&schedule, 0, 1, false));
        assert!(!kill_held(&schedule, 0, 1, true), "the hold was released");
        assert!(!kill_held(&schedule, 0, 2, false), "a replacement");
        assert!(!kill_held(&schedule, 1, 1, false), "another instance");

        let mut timeline = Timeline::new(&schedule);
        let schedule = &schedule;
        let held = |released: bool| move |i| kill_held(schedule, i, 1, released);
        assert_eq!(
            timeline.next(1_000, held(false)),
            Some(Step::Kill {
                at_ms: 1_000,
                instance: 1,
                respawn_at_ms: 1_100
            })
        );
        assert_eq!(timeline.next(1_000, held(false)), None);
        assert_eq!(timeline.next(4_000, held(false)), None);
        assert_eq!(
            timeline.next(5_000, held(true)),
            Some(Step::Kill {
                at_ms: 1_000,
                instance: 0,
                respawn_at_ms: 5_100
            })
        );
    }

    /// A kill held for the lost-reply process stays held past its
    /// `err_after_land` line until its journal shows the landed write
    /// recovered, or one lease has passed since the line.
    #[test]
    fn held_kill_waits_for_the_recovery_or_one_lease() {
        assert!(!hold_released(None, 10_000, 2_000, || true), "no line yet");
        assert!(!hold_released(Some(1_000), 1_050, 2_000, || false));
        assert!(hold_released(Some(1_000), 1_050, 2_000, || true));
        assert!(hold_released(Some(1_000), 3_000, 2_000, || false));

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w0-1.ndjson");
        let journal = Journal::open(&path).unwrap();
        let value = |watermark| crate::journal::Progress {
            schema: crate::journal::SCHEMA,
            epoch: 2,
            owner: Some("w0".to_owned()),
            watermark,
            completed: false,
            status: crate::journal::Status::Runnable,
            attempts: 0,
        };
        let send = |call, expected, watermark| Event::Send {
            call,
            op: crate::journal::WriteOp::Update,
            key: "split.a".to_owned(),
            expected: Some(expected),
            value: value(watermark),
        };
        let done = |call, reply| Event::Done {
            call,
            key: "split.a".to_owned(),
            reply,
        };
        let seen = |rev, watermark, from| Event::Seen {
            key: "split.a".to_owned(),
            rev,
            value: value(watermark),
            from,
        };
        for event in [
            seen(6, None, crate::journal::Source::Get),
            send(1, 6, Some(10)),
            done(1, Reply::Won(7)),
            Event::ErrAfterLand {
                key: "split.a".to_owned(),
                rev: 7,
            },
            send(2, 6, Some(10)),
            done(2, Reply::Lost),
        ] {
            journal.append(event).unwrap();
        }
        assert!(!recovery_journalled(&path, "w0"), "no read-back yet");
        journal
            .append(seen(7, Some(10), crate::journal::Source::Get))
            .unwrap();
        assert!(recovery_journalled(&path, "w0"));
    }

    /// Two kills the schedule renders on the lost-reply process, both due
    /// before its hold is released, are each handed out or drained.
    #[test]
    fn held_kill_swallows_no_rendered_kill() {
        use crate::schedule::Kill;
        use crate::store::AbortPlan;
        let schedule = Schedule {
            kills: vec![
                Kill {
                    at_ms: 1_000,
                    instance: 0,
                    respawn_after_ms: 100,
                },
                Kill {
                    at_ms: 1_500,
                    instance: 0,
                    respawn_after_ms: 100,
                },
            ],
            in_process: vec![InProcess {
                instance: 0,
                plan: AbortPlan {
                    kind: crate::classify::WriteKind::Commit,
                    n: 1,
                    mode: AbortMode::ErrAfterLand,
                },
                respawn_after_ms: 0,
            }],
        };
        let rendered = schedule.render();
        assert!(rendered.contains("1500 ms: kill w0"));
        let mut timeline = Timeline::new(&schedule);
        let mut handed = Vec::new();
        // The hold is released at 5 s: until then both kills are held.
        for now in (0..=20_000).step_by(50) {
            let released = now >= 5_000;
            while let Some(step) = timeline.next(now, |i| kill_held(&schedule, i, 1, released)) {
                if let Step::Kill {
                    instance,
                    respawn_at_ms,
                    ..
                } = step
                {
                    timeline.respawn(instance, respawn_at_ms);
                }
                handed.push(step);
            }
        }
        let left = timeline.drain_kills();
        let kills = handed
            .iter()
            .filter(|s| matches!(s, Step::Kill { .. }))
            .count()
            + left.len();
        assert_eq!(kills, 2, "every rendered kill is handed out or drained");
    }

    /// A process with an `abort` line and no later process under its
    /// instance id fails the expectation; a replaced one does not, nor does a
    /// process with no `abort` line.
    #[test]
    fn an_aborted_process_without_a_replacement_fails_the_expectation() {
        use crate::classify::WriteKind;
        use crate::journal::AbortPoint;
        let journal = |instance: &str, pid, aborted: bool| ProcessJournal {
            instance: instance.to_owned(),
            pid,
            lines: if aborted {
                vec![Line {
                    t_ms: 1,
                    event: Event::Abort {
                        key: "split.a".to_owned(),
                        kind: WriteKind::Claim,
                        n: 1,
                        at: AbortPoint::Before,
                    },
                }]
            } else {
                Vec::new()
            },
        };
        let replaced = [
            journal("w0", 1, true),
            journal("w1", 2, false),
            journal("w0", 3, false),
        ];
        assert_eq!(unreplaced_aborts(&replaced), Vec::<String>::new());
        assert_eq!(
            unreplaced_aborts(&[
                journal("w0", 3, false),
                journal("w0", 1, true),
                journal("w1", 2, true)
            ]),
            [
                "w0 (pid 1) aborted on its plan and was never replaced",
                "w1 (pid 2) aborted on its plan and was never replaced",
            ]
        );
    }

    /// A process that ends on SIGABRT with its plan's `abort` line is replaced
    /// one respawn delay later; one killed, one with no line, and the
    /// `ErrAfterLand` process are not.
    #[test]
    fn every_abort_is_followed_by_a_respawn() {
        use crate::store::AbortPlan;
        let with = |mode| InProcess {
            instance: 1,
            plan: AbortPlan {
                kind: crate::classify::WriteKind::Claim,
                n: 2,
                mode,
            },
            respawn_after_ms: 700,
        };
        let abort = Some(libc::SIGABRT);
        for mode in [AbortMode::Before, AbortMode::After] {
            assert_eq!(abort_respawn(&with(mode), abort, || true, 50), Some(750));
            assert_eq!(abort_respawn(&with(mode), abort, || false, 50), None);
            assert_eq!(
                abort_respawn(&with(mode), Some(libc::SIGKILL), || true, 50),
                None
            );
            assert_eq!(abort_respawn(&with(mode), None, || true, 50), None);
        }
        assert_eq!(
            abort_respawn(&with(AbortMode::ErrAfterLand), abort, || true, 50),
            None
        );
    }
}
