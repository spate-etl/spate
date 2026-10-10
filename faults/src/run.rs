//! The fault-run harness: generates a seeded data set, starts the containers
//! and the worker processes, kills, stops and replaces workers on the seeded
//! schedule, replaces workers that abort on their in-process fault, sweeps the
//! store's final state, and judges the run with the oracle.

use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::{self, File};
use std::future::Future;
use std::net::SocketAddr;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, mpsc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use spate_coordination::store::dynamodb::DynamoDbStore;
use spate_coordination::store::nats::NatsStore;
use spate_coordination::store::{CoordinationStore as _, Entry, Keyspace, StoreError};
use spate_test_support::{DynamoDbFaultProxy, Toxiproxy, container_image};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, ContainerRequest, GenericImage, ImageExt};

use crate::expect;
use crate::health::{self, Target};
use crate::journal::{self, Event, Journal, LeaderAtKill, Line, Reply};
use crate::oracle::{
    self, GeneratedObject, GeneratedRecord, Inputs, ProcessJournal, StoreKind, SweptEntry,
};
use crate::outcome::{
    self, Check, Evidence, FaultFired, Kind, Outcome, Scenario, Stage, StopSeen, Violation,
};
use crate::schedule::{Action, InProcess, SLEEPER, Schedule, Step, Stop, Timeline};
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
/// Cap on a stopped worker reporting stopped.
const STOP_CONFIRM: Duration = Duration::from_secs(5);

mod leader;
mod link;
mod proxy;
mod stopped;

pub use proxy::drop_after_land_then_pass_wins;

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
    /// What the run injects.
    pub faults: Faults,
}

/// What a scenario injects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Faults {
    /// Nothing, so a record written twice fails its expectation.
    None,
    /// The seeded schedule of kills, stops and in-process faults.
    Schedule,
    /// Two workers. The second, started once the first leads, stops itself
    /// inside a seeded commit and is resumed after a lease, once the first
    /// has claimed the split. With `broken_fence` the resumed commit is
    /// re-sent after it loses its CAS, and the oracle must catch it.
    StoppedWriter {
        /// The stopped worker re-sends its lost commit.
        broken_fence: bool,
    },
    /// Every worker's first process carries a stop at one seeded stage of a
    /// leader's work, and only the first to reach it stops. That process is
    /// killed, and another instance must take the leader key within four leases.
    LeaderKilled,
    /// Three workers. The first process to reach the second to fourth seeded
    /// progress record while leading stops before sending it, and is resumed
    /// once another instance has claimed that split. With `broken_fence` the
    /// resumed create is re-sent as an update after it finds the key, and the
    /// oracle must catch it.
    DeposedLeader {
        /// The stopped leader overwrites the split it finds.
        broken_fence: bool,
    },
}

impl Faults {
    /// Whether a stopped worker re-sends its write after it loses.
    #[must_use]
    pub fn broken_fence(self) -> bool {
        match self {
            Faults::StoppedWriter { broken_fence } | Faults::DeposedLeader { broken_fence } => {
                broken_fence
            }
            Faults::None | Faults::Schedule | Faults::LeaderKilled => false,
        }
    }
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
    let mut tuning = match spec.store {
        StoreKind::Nats => Tuning::nats(),
        StoreKind::DynamoDb => Tuning::dynamodb(),
    };
    match spec.faults {
        Faults::StoppedWriter { .. } => tuning.max_in_flight = stopped::WORKING_SET,
        Faults::DeposedLeader { .. } => tuning.max_in_flight = leader::WORKING_SET,
        Faults::None | Faults::Schedule | Faults::LeaderKilled => {}
    }
    let schedule = match spec.faults {
        Faults::None => Schedule::default(),
        Faults::Schedule => Schedule::draw(&mut rng, spec.instances, tuning.lease_ms),
        Faults::StoppedWriter { .. } => Schedule::stopped_writer(&mut rng),
        Faults::LeaderKilled => Schedule::leader_killed(&mut rng, tuning.lease_ms),
        Faults::DeposedLeader { .. } => Schedule::deposed_leader(&mut rng),
    };
    let proxy_seed = (spec.store == StoreKind::DynamoDb && spec.faults == Faults::Schedule)
        .then(|| rng.next_u64());
    let links = link_windows(&mut rng, spec, &schedule, &tuning);
    let run = Run {
        spec,
        seed,
        dir,
        schedule,
        proxy_seed,
        links,
    };
    eprintln!("fault-run schedule for {}:\n{}", spec.name, run.render());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("harness runtime");

    let setup = catch_unwind(AssertUnwindSafe(|| {
        setup(&rt, spec.store, &tuning, objects, !run.links.is_empty())
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
        let driven = match spec.faults {
            Faults::StoppedWriter { .. } => run
                .drive_stopped(
                    &mut workers,
                    &mut processes,
                    &env,
                    &rt,
                    &tuning,
                    &faults_path,
                )
                .map(|(timed_out, fired, stopped)| (timed_out, fired, stopped, None)),
            Faults::LeaderKilled | Faults::DeposedLeader { .. } => run.drive_leader(
                &mut workers,
                &mut processes,
                &env,
                &rt,
                &tuning,
                &faults_path,
            ),
            Faults::None | Faults::Schedule => run
                .drive(
                    &mut workers,
                    &mut processes,
                    &env,
                    &rt,
                    &tuning,
                    &faults_path,
                )
                .map(|(timed_out, fired)| (timed_out, fired, None, None)),
        };
        drop(stop);
        (driven, poller.join().expect("the health poller panicked"))
    });
    let (timed_out, mut fired, stopped, killed) = match driven {
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
    let faults = if faults_path.exists() {
        journal::read(&faults_path)
            .unwrap_or_else(|e| panic!("read {}: {e}", faults_path.display()))
    } else {
        Vec::new()
    };
    fired.extend(proxy::fired(&faults, &processes));
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
    let mut violations = oracle::check(&Inputs {
        store: spec.store,
        generated: &generated,
        processes: &journals,
        sweep: &sweep,
        faults: &faults,
        timing: tuning.timing(),
    })
    .unwrap_or_else(|e| panic!("the oracle could not judge the run: {e}"));
    let scenario = match spec.faults {
        Faults::StoppedWriter { broken_fence } | Faults::DeposedLeader { broken_fence } => {
            violations.extend(stopped.as_ref().and_then(not_reassigned));
            Scenario::StoppedWriter {
                broken_fence,
                stop: stopped.map(|s| StopSeen {
                    resend_rev: resend_rev(&journals, &s),
                    key: s.key,
                    pid: s.pid,
                }),
            }
        }
        Faults::None | Faults::Schedule | Faults::LeaderKilled => Scenario::Ordinary,
    };
    let mut expectations = if spec.faults == Faults::None {
        duplicates(&journals)
    } else {
        Vec::new()
    };
    if let Some(killed) = killed {
        violations.extend(killed.violation);
        expectations.extend(killed.expectations);
    }
    if spec.instances > 1 && scenario == Scenario::Ordinary && !claims_overlap(&journals) {
        expectations.push("no two processes held splits at overlapping times".to_owned());
    }
    if !timed_out {
        expectations.extend(unreplaced_aborts(&journals));
    }
    expectations.extend(proxy::unexercised(run.proxy_seed.is_some(), &faults));
    expectations.extend(link::unexercised(!run.links.is_empty(), &faults));
    expectations.extend(unread_leader_kills(spec.faults, &faults));
    let lost_replies = expect::lost_replies(&journals, run.schedule.lost_reply().is_some());
    let (kind, message) = outcome::classify(&Evidence {
        setup_failure: None,
        scenario: &scenario,
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
    /// Toxiproxy and the store's address on the run's network, when workers
    /// reach the store through it.
    link: Option<(Toxiproxy, String)>,
    store: Container<GenericImage>,
    store_name: &'static str,
    store_config: StoreConfig,
    direct: Direct,
    /// DynamoDB Local's address, which a worker's fault proxy forwards to.
    dynamodb: Option<SocketAddr>,
}

/// The harness's own handle on either store.
enum Direct {
    Nats(NatsStore),
    DynamoDb(DynamoDbStore),
}

impl Direct {
    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        match self {
            Direct::Nats(s) => s.get(ks, key).await,
            Direct::DynamoDb(s) => s.get(ks, key).await,
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
        let mut targets = vec![
            Target {
                container: self.store_name,
                running: Box::new(|| self.store.is_running().map_err(|e| e.to_string())),
                reach: Box::new(move || {
                    match rt.block_on(async {
                        tokio::time::timeout(PROBE, self.direct.get(Keyspace::Durable, "plan"))
                            .await
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
        ];
        if let Some((toxiproxy, _)) = &self.link {
            targets.push(Target {
                container: "toxiproxy",
                running: Box::new(|| toxiproxy.is_running()),
                reach: Box::new(|| toxiproxy.probe()),
            });
        }
        targets
    }
}

/// Starts the containers and creates the store. With `linked`, the store
/// joins a network of the run's own, with Toxiproxy beside it.
fn setup(
    rt: &tokio::runtime::Runtime,
    kind: StoreKind,
    tuning: &Tuning,
    objects: Vec<(String, Vec<u8>)>,
    linked: bool,
) -> Result<Env, String> {
    let gateway = Gateway::start(BUCKET)?;
    gateway.put_all(rt, objects)?;
    let _runtime = rt.enter();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let network = format!("spate-faults-{}-{nanos}", std::process::id());
    let on = linked.then(|| (network.as_str(), format!("{network}-store")));
    let on = on.as_ref().map(|(n, c)| (*n, c.as_str()));
    let (store, store_name, store_config, direct, dynamodb) = match kind {
        StoreKind::Nats => {
            let (image, tag) = container_image(&["--pull", "nats"]);
            let nats = GenericImage::new(image, tag)
                .with_exposed_port(NATS_CLIENT_PORT.tcp())
                .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
                .with_cmd(["-js"]);
            let nats = joined(nats, on)
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
            (nats, "nats", config, direct, None)
        }
        StoreKind::DynamoDb => {
            let (local, config, direct, addr) = dynamodb_local(tuning, on)?;
            (
                local,
                "dynamodb",
                config,
                Direct::DynamoDb(direct),
                Some(addr),
            )
        }
    };
    // The first call creates the NATS buckets or the DynamoDB table with the
    // workers' parameters.
    await_ready(rt, || direct.get(Keyspace::Durable, "plan"))?;
    let link = match on {
        Some((network, name)) => {
            let port = match kind {
                StoreKind::Nats => NATS_CLIENT_PORT,
                StoreKind::DynamoDb => DYNAMODB_PORT,
            };
            Some((Toxiproxy::start(network)?, format!("{name}:{port}")))
        }
        None => None,
    };
    Ok(Env {
        gateway,
        link,
        store,
        store_name,
        store_config,
        direct,
        dynamodb,
    })
}

/// `request` on `on`'s network under its container name, when given.
fn joined(
    request: ContainerRequest<GenericImage>,
    on: Option<(&str, &str)>,
) -> ContainerRequest<GenericImage> {
    match on {
        Some((network, name)) => request.with_network(network).with_container_name(name),
        None => request,
    }
}

/// Starts DynamoDB Local, on `on`'s network under its container name when
/// given, and returns it with the store config workers get, the harness's
/// own store handle and its address. Needs a runtime context.
fn dynamodb_local(
    tuning: &Tuning,
    on: Option<(&str, &str)>,
) -> Result<
    (
        Container<GenericImage>,
        StoreConfig,
        DynamoDbStore,
        SocketAddr,
    ),
    String,
> {
    let (image, tag) = container_image(&["--pull", "dynamodb"]);
    let local = GenericImage::new(image, tag)
        .with_exposed_port(DYNAMODB_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Initializing DynamoDB Local"))
        .with_cmd(["-jar", "DynamoDBLocal.jar", "-inMemory"]);
    let local = joined(local, on)
        .start()
        .map_err(|e| format!("start DynamoDB Local: {e}"))?;
    let port = local
        .get_host_port_ipv4(DYNAMODB_PORT)
        .map_err(|e| format!("DynamoDB Local port: {e}"))?;
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let config = StoreConfig::DynamoDb {
        endpoint: format!("http://{addr}"),
        table: TABLE.to_owned(),
        job: JOB.to_owned(),
    };
    let direct = dynamodb_store(&config, tuning)?;
    Ok((local, config, direct, addr))
}

/// Retries `get` until it returns `Ok`, for at most [`SETUP_DEADLINE`].
fn await_ready<F>(rt: &tokio::runtime::Runtime, get: impl Fn() -> F) -> Result<(), String>
where
    F: Future<Output = Result<Option<Entry>, StoreError>>,
{
    let until = Instant::now() + SETUP_DEADLINE;
    loop {
        let ready = rt.block_on(async { tokio::time::timeout(STORE_CALL, get()).await });
        match ready {
            Ok(Ok(_)) => return Ok(()),
            failure if Instant::now() >= until => {
                return Err(format!(
                    "the store was not ready within a minute: {failure:?}"
                ));
            }
            _ => std::thread::sleep(Duration::from_millis(200)),
        }
    }
}

/// The link windows of `spec`'s run. Only a seeded-schedule run draws any, and
/// with more than one instance none falls on the instance carrying
/// `schedule`'s lost reply.
fn link_windows(
    rng: &mut SplitMix64,
    spec: &Spec<'_>,
    schedule: &Schedule,
    tuning: &Tuning,
) -> Vec<link::Window> {
    if spec.faults != Faults::Schedule {
        return Vec::new();
    }
    link::draw(
        rng,
        spec.store,
        spec.instances,
        schedule.lost_reply(),
        tuning,
    )
}

/// Where a worker process sends its store calls.
#[derive(Debug, PartialEq, Eq)]
enum Route {
    Direct,
    Through(SocketAddr),
    FaultProxy { seed: u64, upstream: SocketAddr },
}

/// A worker's route: a fault proxy when the run has a proxy seed, forwarding
/// to Toxiproxy at `linked` when the run has one and to `dynamodb` otherwise;
/// without a seed, Toxiproxy at `linked` or the store directly.
fn route(
    linked: Option<SocketAddr>,
    dynamodb: Option<SocketAddr>,
    proxy_seed: Option<u64>,
) -> Route {
    match (proxy_seed, linked.or(dynamodb)) {
        (Some(seed), Some(upstream)) => Route::FaultProxy { seed, upstream },
        _ => linked.map_or(Route::Direct, Route::Through),
    }
}

impl Route {
    /// The store config a worker on this route connects with; `None` for a
    /// fault proxy, whose address is known once it starts.
    fn store(&self, store: &StoreConfig) -> Option<StoreConfig> {
        match self {
            Route::Direct => Some(store.clone()),
            Route::Through(addr) => Some(through(store, *addr)),
            Route::FaultProxy { .. } => None,
        }
    }
}

/// `store` with its DynamoDB endpoint or NATS server at `addr`.
fn through(store: &StoreConfig, addr: SocketAddr) -> StoreConfig {
    match store {
        StoreConfig::DynamoDb { table, job, .. } => StoreConfig::DynamoDb {
            endpoint: format!("http://{addr}"),
            table: table.clone(),
            job: job.clone(),
        },
        StoreConfig::Nats { job, .. } => StoreConfig::Nats {
            server: format!("nats://{addr}"),
            job: job.clone(),
        },
    }
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
    /// The seed of the DynamoDB proxy scripts, when the run puts a fault
    /// proxy in front of each worker.
    proxy_seed: Option<u64>,
    /// The Toxiproxy windows on the workers' store links.
    links: Vec<link::Window>,
}

impl Run<'_> {
    /// Starts the workers and applies the schedule on real time until every
    /// worker has exited with no replacement due, or [`RUN_DEADLINE`] from
    /// their start passes. A worker that aborts on its plan is replaced after
    /// the plan's delay, and a kill due on the `ErrAfterLand` process waits
    /// until its journal shows the lost reply recovered, or one lease from the
    /// first poll at which the kill is due and the journal holds its
    /// `err_after_land` line. A stop due on the `ErrAfterLand` process, on a
    /// stopped process or on none is skipped. Link windows open as
    /// [`link::Links`] releases them. Returns whether one was still running
    /// at the deadline, and each kill, stop and window the run drew. Each
    /// kill's line records what the leader key held just before it. A proxy
    /// fault that could not be journalled fails the run once it ends.
    fn drive(
        &self,
        workers: &mut Workers,
        processes: &mut Vec<(String, u32, PathBuf)>,
        env: &Env,
        rt: &tokio::runtime::Runtime,
        tuning: &Tuning,
        faults_path: &Path,
    ) -> Result<(bool, Vec<FaultFired>), String> {
        let faults = Arc::new(
            Journal::open(faults_path).map_err(|e| format!("{}: {e}", faults_path.display()))?,
        );
        let mut proxies = Vec::new();
        let unjournalled = Arc::new(OnceLock::new());
        let log = |event| {
            faults
                .append(event)
                .map_err(|e| format!("{}: {e}", faults_path.display()))
        };
        let mut links = link::Links::new(&self.schedule, &self.links);
        let instances = self.spec.instances as usize;
        let start = Instant::now();
        let until = start + RUN_DEADLINE;
        let mut incarnations = vec![1; instances];
        for i in 0..self.spec.instances {
            processes.push(self.spawn_proxied(
                workers,
                env,
                i,
                1,
                tuning,
                &mut proxies,
                &faults,
                &unjournalled,
                &mut links,
            )?);
        }
        let status = |e: std::io::Error| format!("read worker status: {e}");
        let mut timeline = Timeline::new(&self.schedule);
        let mut stops = StopQueue::new(&self.schedule);
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
                        let leader = match live {
                            Some(_) => read_leader(rt, &env.direct),
                            None => LeaderAtKill::Unread,
                        };
                        let mut sent = false;
                        if let Some(pid) = live {
                            log(Event::Kill {
                                instance: name.clone(),
                                pid,
                                leader: leader.clone(),
                            })?;
                            workers
                                .kill(&name)
                                .map_err(|e| format!("kill {name}: {e}"))?;
                            sent = killed_sent(
                                &self.journal_path(instance, incarnations[instance as usize]),
                                &leader,
                            );
                            timeline.respawn(instance, respawn_at_ms);
                            // This replacement also stands in for an abort that
                            // ended the process after the `live` read.
                            judged[instance as usize] = true;
                        }
                        fired.push(FaultFired {
                            incarnation: format!("{name}-{}", incarnations[instance as usize]),
                            fault: kill_fault_text(at_ms, &leader, sent),
                            fired: live.is_some(),
                        });
                    }
                    Step::Respawn { instance, .. } => {
                        let at = instance as usize;
                        incarnations[at] += 1;
                        judged[at] = false;
                        let process = self.spawn_proxied(
                            workers,
                            env,
                            instance,
                            incarnations[at],
                            tuning,
                            &mut proxies,
                            &faults,
                            &unjournalled,
                            &mut links,
                        )?;
                        log(Event::Respawn {
                            instance: process.0.clone(),
                            pid: process.1,
                        })?;
                        processes.push(process);
                    }
                }
            }
            while let Some((instance, pid)) = stops.resume(now_ms) {
                let name = format!("w{instance}");
                if workers.resume(&name, pid).map_err(status)? {
                    log(Event::Sigcont {
                        instance: name,
                        pid,
                    })?;
                }
            }
            while let Some((stop, allowed)) = stops.next(now_ms, &incarnations) {
                let at = stop.instance as usize;
                let name = format!("w{}", stop.instance);
                let pid = if allowed {
                    workers.stop(&name, STOP_CONFIRM).map_err(status)?
                } else {
                    None
                };
                if let Some(pid) = pid {
                    log(Event::Sigstop {
                        instance: name.clone(),
                        pid,
                        duration_ms: stop.duration_ms,
                    })?;
                    stops.stopped(&stop, pid, now_ms, &incarnations);
                }
                fired.push(stop_fired(&stop, incarnations[at], pid.is_some()));
            }
            if let Some((toxiproxy, _)) = &env.link {
                let mut live = vec![None; instances];
                for (i, pid) in live.iter_mut().enumerate() {
                    *pid = workers.live(&format!("w{i}")).map_err(status)?;
                }
                fired.extend(links.step(toxiproxy, now_ms, &incarnations, &live, log)?);
            }
            std::thread::sleep(POLL);
        };
        for window in links.rest() {
            let proxy = format!(
                "w{}-{}",
                window.instance, incarnations[window.instance as usize]
            );
            fired.push(link::fired(&window, proxy, false));
        }
        for stop in stops.rest() {
            let incarnation = incarnations[stop.instance as usize];
            fired.push(stop_fired(&stop, incarnation, false));
        }
        for kill in timeline.drain_kills() {
            if let Action::Kill { at_ms, instance } = kill {
                fired.push(FaultFired {
                    incarnation: format!("w{instance}-{}", incarnations[instance as usize]),
                    fault: kill_fault_text(at_ms, &LeaderAtKill::Unread, false),
                    fired: false,
                });
            }
        }
        if let Some(failure) = unjournalled.get() {
            return Err(failure.clone());
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
        self.spawn_on(
            workers,
            env,
            env.store_config.clone(),
            instance,
            incarnation,
            tuning,
        )
    }

    /// Starts `instance`'s `incarnation` as [`Run::spawn`] does, behind a
    /// Toxiproxy proxy of its own when the run has link windows, and in front
    /// of that a DynamoDB fault proxy of its own when the run has a proxy
    /// seed. Each answer but `pass` is journalled to `faults` against the
    /// process, and one that cannot be is answered `pass` with the failure
    /// kept in `unjournalled`.
    #[allow(clippy::too_many_arguments)]
    fn spawn_proxied(
        &self,
        workers: &mut Workers,
        env: &Env,
        instance: u32,
        incarnation: u32,
        tuning: &Tuning,
        proxies: &mut Vec<DynamoDbFaultProxy>,
        faults: &Arc<Journal>,
        unjournalled: &Arc<OnceLock<String>>,
        links: &mut link::Links<'_>,
    ) -> Result<(String, u32, PathBuf), String> {
        let linked = match &env.link {
            Some((toxiproxy, store)) => {
                let (name, listen) = links.proxy(instance, incarnation)?;
                Some(toxiproxy.create_proxy(&name, listen, store)?)
            }
            None => None,
        };
        let route = route(linked, env.dynamodb, self.proxy_seed);
        let Route::FaultProxy { seed, upstream } = route else {
            let store = route
                .store(&env.store_config)
                .expect("a route without a fault proxy");
            return self.spawn_on(workers, env, store, instance, incarnation, tuning);
        };
        let script = proxy::ProxyScript::new(seed, &self.schedule, instance, incarnation);
        let pid = Arc::new(OnceLock::new());
        let faults = Arc::clone(faults);
        let answer = proxy::logged(
            script,
            format!("w{instance}"),
            Arc::clone(&pid),
            move |event| faults.append(event),
            Arc::clone(unjournalled),
        );
        let proxy = DynamoDbFaultProxy::start(upstream, answer)
            .map_err(|e| format!("start the fault proxy for w{instance}: {e}"))?;
        let store = through(&env.store_config, proxy.addr());
        let process = self.spawn_on(workers, env, store, instance, incarnation, tuning)?;
        let _ = pid.set(process.1);
        proxies.push(proxy);
        Ok(process)
    }

    /// Writes the config of `instance`'s `incarnation`, connecting to
    /// `store`, and starts it.
    fn spawn_on(
        &self,
        workers: &mut Workers,
        env: &Env,
        store: StoreConfig,
        instance: u32,
        incarnation: u32,
        tuning: &Tuning,
    ) -> Result<(String, u32, PathBuf), String> {
        let journal = self.journal_path(instance, incarnation);
        let abort = self
            .schedule
            .plan_for(instance, incarnation)
            .map(|p| p.plan);
        let index = instance;
        let instance = format!("w{instance}");
        let name = format!("{instance}-{incarnation}");
        let config = WorkerConfig {
            instance: instance.clone(),
            journal: journal.clone(),
            store,
            s3: S3Config {
                endpoint: env.gateway.endpoint(),
                bucket: env.gateway.bucket.clone(),
            },
            tuning: *tuning,
            sink_delay_ms: self.spec.sink_delay_ms,
            abort,
            stop_at: self.schedule.stop_for(index, incarnation),
            broken_fence: self.spec.faults.broken_fence(),
            stop_once: matches!(
                self.spec.faults,
                Faults::LeaderKilled | Faults::DeposedLeader { .. }
            )
            .then(|| self.dir.join(leader::TOKEN)),
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
            self.render(),
            self.dir.display()
        );
    }

    /// The schedule and the link windows, one line each.
    fn render(&self) -> String {
        self.schedule.render() + &link::render(&self.links)
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

/// A run's external stops, handed out in drawn order, and the stopped
/// processes awaiting SIGCONT. A process is its instance and incarnation.
#[derive(Debug)]
struct StopQueue<'a> {
    schedule: &'a Schedule,
    /// Stops not yet handed out, latest first.
    pending: Vec<Stop>,
    /// Stopped processes as instance, incarnation, pid and resume time.
    stopped: Vec<(u32, u32, u32, u64)>,
}

impl<'a> StopQueue<'a> {
    fn new(schedule: &'a Schedule) -> StopQueue<'a> {
        let mut pending = schedule.stops.clone();
        pending.sort_by_key(|s| (s.at_ms, s.instance));
        pending.reverse();
        StopQueue {
            schedule,
            pending,
            stopped: Vec::new(),
        }
    }

    /// Removes the next stop due by `now_ms`, with whether it may fall on
    /// its instance's process in `incarnations`: never on the one carrying
    /// the `ErrAfterLand` plan, nor on one already stopped.
    fn next(&mut self, now_ms: u64, incarnations: &[u32]) -> Option<(Stop, bool)> {
        if self.pending.last()?.at_ms > now_ms {
            return None;
        }
        let stop = self.pending.pop()?;
        let incarnation = incarnations[stop.instance as usize];
        let allowed = self
            .schedule
            .plan_for(stop.instance, incarnation)
            .is_none_or(|p| p.plan.mode != AbortMode::ErrAfterLand)
            && !self
                .stopped
                .iter()
                .any(|s| s.0 == stop.instance && s.1 == incarnation);
        Some((stop, allowed))
    }

    /// Records `stop` as applied at `now_ms` to `pid`, its instance's process
    /// in `incarnations`, and schedules its resume `stop.duration_ms` later.
    fn stopped(&mut self, stop: &Stop, pid: u32, now_ms: u64, incarnations: &[u32]) {
        let incarnation = incarnations[stop.instance as usize];
        self.stopped
            .push((stop.instance, incarnation, pid, now_ms + stop.duration_ms));
    }

    /// Removes a resume due by `now_ms` and returns its instance and pid,
    /// whose process may since have been killed or replaced.
    fn resume(&mut self, now_ms: u64) -> Option<(u32, u32)> {
        let at = self.stopped.iter().position(|s| s.3 <= now_ms)?;
        let (instance, _, pid, _) = self.stopped.remove(at);
        Some((instance, pid))
    }

    /// The stops never handed out, in drawn order.
    fn rest(self) -> impl Iterator<Item = Stop> {
        self.pending.into_iter().rev()
    }
}

fn stop_fired(stop: &Stop, incarnation: u32, fired: bool) -> FaultFired {
    FaultFired {
        incarnation: format!("w{}-{incarnation}", stop.instance),
        fault: format!("stop at {} ms for {} ms", stop.at_ms, stop.duration_ms),
        fired,
    }
}

/// The stopped-writer violation when the peer never claimed the stopped
/// split during the stop.
fn not_reassigned(stopped: &stopped::Stopped) -> Option<Violation> {
    (!stopped.reassigned).then(|| Violation {
        check: Check::StoppedSplitNotReassigned,
        key: Some(stopped.key.clone()),
        rev: None,
        instance: Some(stopped.instance.clone()),
        pid: Some(stopped.pid),
        detail: "no peer claimed the stopped split within four leases of the stop".to_owned(),
    })
}

/// The revision at which the stopped process's write landed after its
/// stop, sent from a revision other than the one it was stopped at.
fn resend_rev(journals: &[ProcessJournal], stopped: &stopped::Stopped) -> Option<u64> {
    let journal = journals.iter().find(|j| j.pid == stopped.pid)?;
    let after = journal.lines.iter().skip_while(|l| {
        !matches!(
            &l.event,
            Event::Stop { key, .. } | Event::LeaderStop { key, .. } if *key == stopped.key
        )
    });
    let mut resends = HashSet::new();
    for line in after {
        match &line.event {
            Event::Send {
                call,
                key,
                expected,
                ..
            } if *key == stopped.key && *expected != stopped.expected => {
                resends.insert(*call);
            }
            Event::Done {
                call,
                reply: Reply::Won(rev),
                ..
            } if resends.contains(call) => return Some(*rev),
            _ => {}
        }
    }
    None
}

/// What the leader key holds, read through `direct` under [`PROBE`].
fn read_leader(rt: &tokio::runtime::Runtime, direct: &Direct) -> LeaderAtKill {
    leader_from(
        rt.block_on(async {
            tokio::time::timeout(PROBE, direct.get(Keyspace::Ephemeral, "leader")).await
        })
        .ok(),
    )
}

/// The label for a leader read that returned `read`, or `None` when it timed out.
fn leader_from(read: Option<Result<Option<Entry>, StoreError>>) -> LeaderAtKill {
    /// The leader record's fields the label reads.
    #[derive(serde::Deserialize)]
    struct Record {
        owner: String,
        generation: u64,
    }
    match read {
        Some(Ok(None)) => LeaderAtKill::Vacant,
        Some(Ok(Some(entry))) => {
            serde_json::from_slice::<Record>(&entry.value).map_or(LeaderAtKill::Unread, |r| {
                LeaderAtKill::Held {
                    owner: r.owner,
                    generation: r.generation,
                    digest: spate_test_support::fnv1a(&entry.value),
                }
            })
        }
        Some(Err(_)) | None => LeaderAtKill::Unread,
    }
}

/// The fault-list text of a kill at `at_ms`, naming the leader key's owner
/// when the read found one. With `sent`, the killed process sent the bytes the
/// read returned, and the text names it the leader.
fn kill_fault_text(at_ms: u64, leader: &LeaderAtKill, sent: bool) -> String {
    match leader {
        LeaderAtKill::Held {
            owner, generation, ..
        } if sent => {
            format!("kill at {at_ms} ms (killed the leader, {owner}, generation {generation})")
        }
        LeaderAtKill::Held {
            owner, generation, ..
        } => format!("kill at {at_ms} ms (leader key named {owner}, generation {generation})"),
        LeaderAtKill::Vacant | LeaderAtKill::Unread => format!("kill at {at_ms} ms"),
    }
}

/// Whether `lines` hold a `leader_send` on the leader key whose bytes digest
/// to `digest`.
fn sent_leader_key(lines: &[Line], digest: u64) -> bool {
    lines.iter().any(|line| {
        matches!(&line.event, Event::LeaderSend { key, digest: d, .. }
            if key == "leader" && *d == digest)
    })
}

/// Whether the journal at `path` shows its process sent the leader key the
/// harness read as `leader`. A journal that cannot be read shows nothing.
fn killed_sent(path: &Path, leader: &LeaderAtKill) -> bool {
    let LeaderAtKill::Held { digest, .. } = leader else {
        return false;
    };
    journal::read(path).is_ok_and(|lines| sent_leader_key(&lines, *digest))
}

/// One expectation per `kill` line in `faults` whose leader read failed, in
/// a [`Faults::Schedule`] run.
fn unread_leader_kills(kind: Faults, faults: &[Line]) -> Vec<String> {
    if kind != Faults::Schedule {
        return Vec::new();
    }
    faults
        .iter()
        .filter_map(|line| match &line.event {
            Event::Kill {
                instance,
                pid,
                leader: LeaderAtKill::Unread,
            } => Some(format!(
                "leader key unread before the kill of {instance} (pid {pid})"
            )),
            _ => None,
        })
        .collect()
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
    use crate::journal::{Event, LeaderAtKill, Line};

    /// A run with link windows polls Toxiproxy beside the store and
    /// SeaweedFS, and one without polls only those two.
    #[test]
    #[ignore = "requires Docker"]
    fn a_linked_run_polls_toxiproxy() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        for (linked, expected) in [
            (true, &["nats", "seaweedfs", "toxiproxy"][..]),
            (false, &["nats", "seaweedfs"][..]),
        ] {
            let env = setup(&rt, StoreKind::Nats, &Tuning::nats(), Vec::new(), linked).unwrap();
            let polls: Vec<_> = env.health_targets(&rt).iter().map(Target::poll).collect();
            let names: Vec<_> = polls.iter().map(|p| p.container.as_str()).collect();
            assert_eq!(names, expected);
            assert!(polls.iter().all(|p| p.ok), "{polls:?}");
        }
    }

    fn stop(at_ms: u64, instance: u32, duration_ms: u64) -> Stop {
        Stop {
            at_ms,
            instance,
            duration_ms,
        }
    }

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

    /// A three-instance seeded-schedule run draws windows, none on the
    /// instance carrying its lost reply, and a run without the seeded
    /// schedule draws none.
    #[test]
    fn a_run_draws_no_window_on_its_lost_reply_instance() {
        let tuning = Tuning::nats();
        let spec = Spec {
            name: "nats_three_instances",
            store: StoreKind::Nats,
            instances: 3,
            worker: Path::new("worker"),
            sink_delay_ms: 0,
            faults: Faults::Schedule,
        };
        for seed in 0..100 {
            let mut rng = SplitMix64::new(seed);
            let schedule = Schedule::draw(&mut rng, spec.instances, tuning.lease_ms);
            let lost = schedule.lost_reply();
            let windows = link_windows(&mut rng, &spec, &schedule, &tuning);
            assert!(
                !windows.is_empty() && windows.iter().all(|w| Some(w.instance) != lost),
                "seed {seed}: lost {lost:?}, {windows:?}"
            );
            let control = Spec {
                faults: Faults::None,
                ..spec
            };
            assert_eq!(link_windows(&mut rng, &control, &schedule, &tuning), []);
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
            faults: Faults::None,
        };
        let run = Run {
            spec: &spec,
            seed: 0xff,
            dir: dir.clone(),
            schedule: Schedule::default(),
            proxy_seed: None,
            links: Vec::new(),
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

    /// A linked worker reaches its store through Toxiproxy, behind its fault
    /// proxy when the run has a proxy seed.
    #[test]
    fn linked_workers_reach_the_store_through_toxiproxy() {
        let toxiproxy = SocketAddr::from(([127, 0, 0, 1], 21_000));
        let local = SocketAddr::from(([127, 0, 0, 1], 8_000));
        assert_eq!(
            route(Some(toxiproxy), Some(local), Some(7)),
            Route::FaultProxy {
                seed: 7,
                upstream: toxiproxy
            }
        );
        assert_eq!(
            route(Some(toxiproxy), None, None),
            Route::Through(toxiproxy)
        );
        assert_eq!(
            route(None, Some(local), Some(7)),
            Route::FaultProxy {
                seed: 7,
                upstream: local
            }
        );
        assert_eq!(route(None, Some(local), None), Route::Direct);
    }

    /// A worker routed through Toxiproxy connects to Toxiproxy's address, and
    /// one routed directly keeps the store's.
    #[test]
    fn a_routed_worker_connects_to_its_route() {
        let toxiproxy = SocketAddr::from(([127, 0, 0, 1], 21_000));
        let nats = StoreConfig::Nats {
            server: "nats://127.0.0.1:4222".to_owned(),
            job: JOB.to_owned(),
        };
        assert_eq!(
            Route::Through(toxiproxy).store(&nats),
            Some(through(&nats, toxiproxy))
        );
        assert_eq!(Route::Direct.store(&nats), Some(nats.clone()));
        let proxied = Route::FaultProxy {
            seed: 7,
            upstream: toxiproxy,
        };
        assert_eq!(proxied.store(&nats), None);
    }

    /// A worker's store config reaches either store at the given address,
    /// keeping its job and table.
    #[test]
    fn through_points_either_store_at_the_address() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 21_000));
        let nats = StoreConfig::Nats {
            server: "nats://127.0.0.1:4222".to_owned(),
            job: JOB.to_owned(),
        };
        let dynamodb = StoreConfig::DynamoDb {
            endpoint: "http://127.0.0.1:8000".to_owned(),
            table: TABLE.to_owned(),
            job: JOB.to_owned(),
        };
        assert_eq!(
            through(&nats, addr),
            StoreConfig::Nats {
                server: "nats://127.0.0.1:21000".to_owned(),
                job: JOB.to_owned(),
            }
        );
        assert_eq!(
            through(&dynamodb, addr),
            StoreConfig::DynamoDb {
                endpoint: "http://127.0.0.1:21000".to_owned(),
                table: TABLE.to_owned(),
                job: JOB.to_owned(),
            }
        );
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

    /// No stop falls on the process carrying the `ErrAfterLand` plan, and a
    /// kill due on it waits until its hold is released, its replacement one
    /// respawn delay from then; stops and kills on other processes fall on
    /// time.
    #[test]
    fn err_after_land_incarnation_is_never_stopped_and_killed_only_after_its_release() {
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
            ..Schedule::default()
        };
        assert!(kill_held(&schedule, 0, 1, false));
        assert!(!kill_held(&schedule, 0, 1, true), "the hold was released");
        assert!(!kill_held(&schedule, 0, 2, false), "a replacement");
        assert!(!kill_held(&schedule, 1, 1, false), "another instance");
        let stops = Schedule {
            stops: vec![stop(500, 0, 500), stop(500, 1, 500)],
            ..schedule.clone()
        };
        let mut queue = StopQueue::new(&stops);
        assert_eq!(queue.next(500, &[1, 1]), Some((stop(500, 0, 500), false)));
        assert_eq!(
            queue.next(500, &[1, 1]),
            Some((stop(500, 1, 500), true)),
            "another instance"
        );
        let mut queue = StopQueue::new(&stops);
        assert_eq!(
            queue.next(500, &[2, 1]),
            Some((stop(500, 0, 500), true)),
            "a replacement"
        );

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
            ..Schedule::default()
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

    /// Due stops are handed out in drawn order, `(at_ms, instance)`, and the
    /// ones never due are left in that order.
    #[test]
    fn stop_queue_hands_out_due_stops_in_order_and_keeps_the_rest() {
        let schedule = Schedule {
            stops: vec![
                stop(3_000, 1, 500),
                stop(1_000, 1, 500),
                stop(9_000, 0, 500),
                stop(1_000, 0, 500),
                stop(8_000, 1, 500),
            ],
            ..Schedule::default()
        };
        let mut queue = StopQueue::new(&schedule);
        let mut handed = Vec::new();
        for now in (0..=5_000).step_by(50) {
            while let Some((stop, allowed)) = queue.next(now, &[1, 1]) {
                assert!(allowed);
                handed.push((now, stop));
            }
        }
        assert_eq!(
            handed,
            [
                (1_000, stop(1_000, 0, 500)),
                (1_000, stop(1_000, 1, 500)),
                (3_000, stop(3_000, 1, 500)),
            ]
        );
        assert_eq!(
            queue.rest().collect::<Vec<_>>(),
            [stop(8_000, 1, 500), stop(9_000, 0, 500)]
        );
    }

    /// A stopped process is resumed its stop's duration after the stop, once,
    /// and holds off another stop of the same process until then; a
    /// replacement process is not held by its predecessor's stop.
    #[test]
    fn a_stop_resumes_after_its_duration_and_holds_its_process() {
        let schedule = Schedule {
            stops: vec![
                stop(1_000, 1, 1_500),
                stop(2_000, 1, 500),
                stop(2_000, 0, 500),
                stop(3_000, 1, 2_000),
                stop(4_000, 1, 500),
            ],
            ..Schedule::default()
        };
        let mut queue = StopQueue::new(&schedule);
        let (first, allowed) = queue.next(1_000, &[1, 1]).unwrap();
        assert!(allowed);
        queue.stopped(&first, 42, 1_000, &[1, 1]);
        assert_eq!(queue.resume(2_499), None);
        assert_eq!(
            queue.next(2_000, &[1, 1]),
            Some((stop(2_000, 0, 500), true)),
            "another instance"
        );
        assert_eq!(
            queue.next(2_000, &[1, 1]),
            Some((stop(2_000, 1, 500), false)),
            "stopped"
        );
        assert_eq!(queue.resume(2_500), Some((1, 42)));
        assert_eq!(queue.resume(2_500), None, "resumed once");
        let (third, allowed) = queue.next(3_000, &[1, 1]).unwrap();
        assert!(allowed, "resumed");
        queue.stopped(&third, 43, 3_000, &[1, 1]);
        assert_eq!(
            queue.next(4_000, &[1, 2]),
            Some((stop(4_000, 1, 500), true)),
            "the stopped process's replacement"
        );
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

    /// `journal_holds` finds an event only in a journal that holds it.
    #[test]
    fn journal_holds_finds_only_a_journalled_event() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w1-1.ndjson");
        assert!(!journal_holds(&path, "abort"));
        let journal = Journal::open(&path).unwrap();
        journal
            .append(Event::ReadFailed {
                key: "split.abort".to_owned(),
            })
            .unwrap();
        assert!(!journal_holds(&path, "abort"));
        journal
            .append(Event::Abort {
                key: "split.a".to_owned(),
                kind: crate::classify::WriteKind::Commit,
                n: 1,
                at: crate::journal::AbortPoint::Before,
            })
            .unwrap();
        assert!(journal_holds(&path, "abort"));
    }

    /// The re-send's revision is the first win on the stopped key after the
    /// stop line, sent from a revision other than the one the stopped commit
    /// replaced; a win from that revision or on another key does not count.
    #[test]
    fn resend_rev_is_the_first_win_from_another_revision_after_the_stop() {
        use crate::journal::{Progress, Status, WriteOp};
        let value = Progress {
            schema: journal::SCHEMA,
            epoch: 1,
            owner: Some("w1".to_owned()),
            watermark: Some(10),
            completed: false,
            status: Status::Runnable,
            attempts: 0,
        };
        let line = |event| Line { t_ms: 1, event };
        let send = |call, key: &str, expected| {
            line(Event::Send {
                call,
                op: WriteOp::Update,
                key: key.to_owned(),
                expected: Some(expected),
                value: value.clone(),
            })
        };
        let done = |call, key: &str, reply| {
            line(Event::Done {
                call,
                key: key.to_owned(),
                reply,
            })
        };
        let stopped = stopped::Stopped {
            key: "split.a".to_owned(),
            expected: Some(4),
            instance: "w1".to_owned(),
            pid: 9,
            reassigned: true,
        };
        let journal = |lines| ProcessJournal {
            instance: "w1".to_owned(),
            pid: 9,
            lines,
        };
        let before = journal(vec![
            send(1, "split.a", 2),
            done(1, "split.a", Reply::Won(3)),
            line(Event::Stop {
                key: "split.a".to_owned(),
                expected: 4,
                epoch: 1,
            }),
            send(2, "split.a", 4),
            done(2, "split.a", Reply::Lost),
            send(3, "split.b", 5),
            done(3, "split.b", Reply::Won(9)),
            send(4, "split.a", 5),
            done(4, "split.a", Reply::Won(6)),
        ]);
        assert_eq!(resend_rev(&[before], &stopped), Some(6));
        let fenced = journal(vec![
            line(Event::Stop {
                key: "split.a".to_owned(),
                expected: 4,
                epoch: 1,
            }),
            send(2, "split.a", 4),
            done(2, "split.a", Reply::Won(5)),
        ]);
        assert_eq!(resend_rev(&[fenced], &stopped), None);
    }

    /// A stop the peer never claimed during is a property-4 violation
    /// against the stopped process; a claimed one is none.
    #[test]
    fn a_stop_the_peer_never_claimed_is_a_property_4_violation() {
        let stopped = |reassigned| stopped::Stopped {
            key: "split.a".to_owned(),
            expected: Some(4),
            instance: "w2".to_owned(),
            pid: 9,
            reassigned,
        };
        assert_eq!(not_reassigned(&stopped(true)), None);
        let violation = not_reassigned(&stopped(false)).unwrap();
        assert_eq!(
            (
                violation.check,
                violation.check.property(),
                violation.key.as_deref(),
                violation.instance.as_deref(),
                violation.pid
            ),
            (
                Check::StoppedSplitNotReassigned,
                4,
                Some("split.a"),
                Some("w2"),
                Some(9)
            )
        );
    }

    /// After a `leader_stop` on a seed create, the re-send is the first win
    /// on that key from a revision. The stopped create itself, lost or won,
    /// does not count.
    #[test]
    fn resend_rev_finds_the_update_after_a_leader_stop() {
        use crate::classify::WriteKind;
        use crate::journal::{Progress, Status, WriteOp};
        let value = Progress {
            schema: journal::SCHEMA,
            epoch: 0,
            owner: None,
            watermark: None,
            completed: false,
            status: Status::Runnable,
            attempts: 0,
        };
        let line = |event| Line { t_ms: 1, event };
        let send = |call, op, expected| {
            line(Event::Send {
                call,
                op,
                key: "split.c".to_owned(),
                expected,
                value: value.clone(),
            })
        };
        let done = |call, reply| {
            line(Event::Done {
                call,
                key: "split.c".to_owned(),
                reply,
            })
        };
        let stop = line(Event::LeaderStop {
            key: "split.c".to_owned(),
            kind: WriteKind::Seed,
            n: 3,
            value: serde_json::Value::Null,
            published: false,
        });
        let stopped = stopped::Stopped {
            key: "split.c".to_owned(),
            expected: None,
            instance: "w0".to_owned(),
            pid: 9,
            reassigned: true,
        };
        let journal = |lines| ProcessJournal {
            instance: "w0".to_owned(),
            pid: 9,
            lines,
        };
        let resent = journal(vec![
            stop.clone(),
            send(4, WriteOp::Create, None),
            done(4, Reply::Lost),
            send(5, WriteOp::Update, Some(7)),
            done(5, Reply::Won(8)),
        ]);
        assert_eq!(resend_rev(&[resent], &stopped), Some(8));
        let created = journal(vec![
            stop,
            send(4, WriteOp::Create, None),
            done(4, Reply::Won(3)),
        ]);
        assert_eq!(resend_rev(&[created], &stopped), None);
    }

    /// Both broken-fence variants report a broken fence, and no other
    /// scenario does.
    #[test]
    fn faults_broken_fence_covers_both_variants() {
        assert!(Faults::StoppedWriter { broken_fence: true }.broken_fence());
        assert!(Faults::DeposedLeader { broken_fence: true }.broken_fence());
        for faults in [
            Faults::StoppedWriter {
                broken_fence: false,
            },
            Faults::DeposedLeader {
                broken_fence: false,
            },
            Faults::LeaderKilled,
            Faults::Schedule,
            Faults::None,
        ] {
            assert!(!faults.broken_fence(), "{faults:?}");
        }
    }

    /// A kill under a `Held` read names the key's owner and generation, and
    /// a `Vacant` or `Unread` read renders a plain kill.
    #[test]
    fn kill_fault_text_names_the_key_owner() {
        let held = LeaderAtKill::Held {
            owner: "w2".to_owned(),
            generation: 3,
            digest: 9,
        };
        assert_eq!(
            kill_fault_text(1500, &held, false),
            "kill at 1500 ms (leader key named w2, generation 3)"
        );
        assert_eq!(
            kill_fault_text(1500, &LeaderAtKill::Vacant, false),
            "kill at 1500 ms"
        );
        assert_eq!(
            kill_fault_text(1500, &LeaderAtKill::Unread, false),
            "kill at 1500 ms"
        );
    }

    /// A kill is labelled as the leader only when the killed process's
    /// journal has a `leader_send` on the leader key with the digest the
    /// harness read; another digest, or only a read of that digest, keeps the
    /// owner label.
    #[test]
    fn kill_label_names_the_leader_only_from_its_own_send() {
        const D: u64 = 0xfeed;
        let read = LeaderAtKill::Held {
            owner: "w1".to_owned(),
            generation: 2,
            digest: D,
        };
        let line = |event| Line { t_ms: 1, event };
        let send = |digest| {
            line(Event::LeaderSend {
                call: 1,
                op: crate::journal::WriteOp::Create,
                key: "leader".to_owned(),
                expected: None,
                digest,
            })
        };
        let label = |lines: &[Line]| {
            let LeaderAtKill::Held { digest, .. } = &read else {
                unreachable!("a held read")
            };
            kill_fault_text(900, &read, sent_leader_key(lines, *digest))
        };
        assert_eq!(
            label(&[send(D)]),
            "kill at 900 ms (killed the leader, w1, generation 2)"
        );
        assert_eq!(
            label(&[send(D + 1)]),
            "kill at 900 ms (leader key named w1, generation 2)"
        );
        let seen_only = [line(Event::LeaderSeen {
            key: "leader".to_owned(),
            rev: 4,
            digest: D,
            from: crate::journal::Source::Get,
        })];
        assert_eq!(
            label(&seen_only),
            "kill at 900 ms (leader key named w1, generation 2)"
        );
    }

    /// Each `Unread` kill line of a scheduled-fault run is an expectation;
    /// `Held` and `Vacant` lines, and any line outside such a run, are not.
    #[test]
    fn an_unread_leader_before_a_kill_is_an_expectation() {
        let kill = |pid, leader| Line {
            t_ms: 1,
            event: Event::Kill {
                instance: "w1".to_owned(),
                pid,
                leader,
            },
        };
        let lines = [
            kill(10, LeaderAtKill::Unread),
            kill(11, LeaderAtKill::Vacant),
            kill(
                12,
                LeaderAtKill::Held {
                    owner: "w0".to_owned(),
                    generation: 1,
                    digest: 5,
                },
            ),
            kill(13, LeaderAtKill::Unread),
        ];
        assert_eq!(
            unread_leader_kills(Faults::Schedule, &lines),
            [
                "leader key unread before the kill of w1 (pid 10)",
                "leader key unread before the kill of w1 (pid 13)",
            ]
        );
        assert_eq!(
            unread_leader_kills(Faults::None, &lines),
            Vec::<String>::new()
        );
    }

    /// A leader record decodes to its owner, generation and digest; an absent
    /// key is `Vacant`; bytes that do not parse, a store error and a timeout
    /// are `Unread`.
    #[test]
    fn a_leader_read_is_labelled_by_what_it_returned() {
        let entry = |value: &[u8]| Entry {
            key: "leader".to_owned(),
            value: value.to_vec(),
            revision: spate_coordination::store::Revision(1),
        };
        let record = br#"{"schema":1,"owner":"w2","nonce":"n","generation":3}"#;
        assert_eq!(
            leader_from(Some(Ok(Some(entry(record))))),
            LeaderAtKill::Held {
                owner: "w2".to_owned(),
                generation: 3,
                digest: spate_test_support::fnv1a(record),
            }
        );
        assert_eq!(leader_from(Some(Ok(None))), LeaderAtKill::Vacant);
        assert_eq!(
            leader_from(Some(Ok(Some(entry(b"{\"owner\":7}"))))),
            LeaderAtKill::Unread
        );
        assert_eq!(
            leader_from(Some(Err(StoreError::Retryable("down".to_owned())))),
            LeaderAtKill::Unread
        );
        assert_eq!(leader_from(None), LeaderAtKill::Unread);
    }
}
