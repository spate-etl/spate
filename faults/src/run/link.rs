//! Seeded Toxiproxy windows on each worker process's store link, and the
//! queue that opens and closes them on real time.

use std::fmt;
use std::fmt::Write as _;

use spate_test_support::{LISTEN_PORTS, Stream, Toxic, Toxiproxy};

use super::proxy::link_faults;
use crate::journal::{Event, Line};
use crate::oracle::StoreKind;
use crate::outcome::FaultFired;
use crate::schedule::Schedule;
use crate::seed::SplitMix64;
use crate::worker::Tuning;

/// Earliest window, from the start of the running stage.
const FROM_MS: u64 = 1_000;
/// Latest window, from the start of the running stage. A later window can
/// fall due after a short run's workers have all finished.
const UNTIL_MS: u64 = 5_000;
/// Longest ordinary latency on a NATS link, below its store timeout.
const NATS_MAX_LATENCY_MS: u64 = 300;
/// Longest ordinary latency on a DynamoDB link. With the fault proxy's
/// longest delay it stays inside the SDK's read timeout.
pub(super) const DYNAMODB_MAX_LATENCY_MS: u64 = 100;

/// What a window does to the link.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LinkKind {
    /// Holds each chunk the worker sends this long.
    Latency { latency_ms: u64 },
    /// Holds everything the worker sends until the window closes, then
    /// delivers it on the same connection.
    HealNearExpiry { latency_ms: u64 },
    /// Drops everything both ways, and closes the connection on heal.
    Blackhole,
    /// Closes open connections, and closes each new one once accepted.
    Refuse,
    /// Closes each connection once this many bytes reached the worker.
    LimitData { bytes: u64 },
}

impl fmt::Display for LinkKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LinkKind::Latency { latency_ms } => write!(f, "latency({latency_ms}ms)"),
            LinkKind::HealNearExpiry { latency_ms } => {
                write!(f, "heal_near_expiry({latency_ms}ms)")
            }
            LinkKind::Blackhole => f.write_str("blackhole"),
            LinkKind::Refuse => f.write_str("refuse"),
            LinkKind::LimitData { bytes } => write!(f, "limit_data({bytes})"),
        }
    }
}

impl LinkKind {
    /// The toxics the window adds to its proxy as name, stream and toxic.
    /// [`LinkKind::Refuse`] adds none and disables the proxy instead.
    pub(super) fn toxics(self) -> Vec<(&'static str, Stream, Toxic)> {
        let ms = std::time::Duration::from_millis;
        match self {
            LinkKind::Latency { latency_ms } | LinkKind::HealNearExpiry { latency_ms } => {
                vec![("latency", Stream::Upstream, Toxic::Latency(ms(latency_ms)))]
            }
            LinkKind::Blackhole => vec![
                ("blackhole-up", Stream::Upstream, Toxic::Timeout(ms(0))),
                ("blackhole-down", Stream::Downstream, Toxic::Timeout(ms(0))),
            ],
            LinkKind::Refuse => Vec::new(),
            LinkKind::LimitData { bytes } => {
                vec![("limit", Stream::Downstream, Toxic::LimitData(bytes))]
            }
        }
    }
}

/// One window on an instance's store link.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Window {
    /// When the window is due, from the start of the running stage.
    pub(super) at_ms: u64,
    /// Instance index, `w<index>`.
    pub(super) instance: u32,
    pub(super) kind: LinkKind,
    pub(super) duration_ms: u64,
}

/// Draws between one and `instances + 1` windows due one to five seconds
/// into the running stage. Ordinary latency lasts half a lease to a lease;
/// heal-near-expiry, blackhole and refuse windows last a lease give or take
/// one store timeout; `limit_data` is drawn on NATS only. With more than one
/// instance, none is drawn on `lost`, whose first process never takes one.
pub(super) fn draw(
    rng: &mut SplitMix64,
    store: StoreKind,
    instances: u32,
    lost: Option<u32>,
    tuning: &Tuning,
) -> Vec<Window> {
    let (lease, op) = (tuning.lease_ms, tuning.op_timeout_ms);
    let (kinds, max_latency) = match store {
        StoreKind::Nats => (5, NATS_MAX_LATENCY_MS),
        StoreKind::DynamoDb => (4, DYNAMODB_MAX_LATENCY_MS),
    };
    (0..rng.in_range(1, u64::from(instances) + 1))
        .map(|_| {
            let at_ms = rng.in_range(FROM_MS, UNTIL_MS);
            let instance = match lost.filter(|_| instances > 1) {
                Some(lost) => {
                    let i = u32::try_from(rng.in_range(0, u64::from(instances) - 2))
                        .expect("an instance index fits in u32");
                    if i < lost { i } else { i + 1 }
                }
                None => u32::try_from(rng.in_range(0, u64::from(instances) - 1))
                    .expect("an instance index fits in u32"),
            };
            let short = rng.in_range(lease / 2, lease);
            let near_lease = rng.in_range(lease - op, lease + op);
            let (kind, duration_ms) = match rng.in_range(0, kinds - 1) {
                0 => (
                    LinkKind::Latency {
                        latency_ms: rng.in_range(10, max_latency),
                    },
                    short,
                ),
                1 => (
                    LinkKind::HealNearExpiry {
                        latency_ms: near_lease + op,
                    },
                    near_lease,
                ),
                2 => (LinkKind::Blackhole, near_lease),
                3 => (LinkKind::Refuse, near_lease),
                _ => (
                    LinkKind::LimitData {
                        bytes: rng.in_range(512, 8_192),
                    },
                    short,
                ),
            };
            Window {
                at_ms,
                instance,
                kind,
                duration_ms,
            }
        })
        .collect()
}

/// Opens and closes windows on a worker process's proxy.
pub(super) trait Control {
    /// Opens a `kind` window on `proxy`.
    fn open(&self, proxy: &str, kind: LinkKind) -> Result<(), String>;
    /// Closes the `kind` window [`Control::open`] opened on `proxy`.
    fn close(&self, proxy: &str, kind: LinkKind) -> Result<(), String>;
}

impl Control for Toxiproxy {
    fn open(&self, proxy: &str, kind: LinkKind) -> Result<(), String> {
        if kind == LinkKind::Refuse {
            return self.set_enabled(proxy, false);
        }
        for (name, stream, toxic) in kind.toxics() {
            self.add_toxic(proxy, name, stream, toxic)?;
        }
        Ok(())
    }

    fn close(&self, proxy: &str, kind: LinkKind) -> Result<(), String> {
        if kind == LinkKind::Refuse {
            return self.set_enabled(proxy, true);
        }
        for (name, _, _) in kind.toxics() {
            self.remove_toxic(proxy, name)?;
        }
        Ok(())
    }
}

/// The `faults_fired` entry for `window` on the process `proxy` serves.
pub(super) fn fired(window: &Window, proxy: String, fired: bool) -> FaultFired {
    FaultFired {
        incarnation: proxy,
        fault: format!(
            "{} at {} ms for {} ms",
            window.kind, window.at_ms, window.duration_ms
        ),
        fired,
    }
}

/// The failed expectation of a run that drew link windows when `faults`
/// holds no `toxic` line.
pub(super) fn unexercised(drawn: bool, faults: &[Line]) -> Option<String> {
    let opened = faults
        .iter()
        .any(|line| matches!(line.event, Event::Toxic { .. }));
    (drawn && !opened).then(|| "fault not exercised: no toxic line".to_owned())
}

/// One line per window, as a failure message shows it.
pub(super) fn render(windows: &[Window]) -> String {
    let mut text = String::new();
    for w in windows {
        let _ = writeln!(
            text,
            "{} ms: {} on w{} for {} ms",
            w.at_ms, w.kind, w.instance, w.duration_ms
        );
    }
    text
}

/// A run's link windows, opened in drawn order on the proxy of each
/// instance's current process, and the Toxiproxy proxy of each process.
#[derive(Debug)]
pub(super) struct Links<'a> {
    schedule: &'a Schedule,
    /// Windows not yet opened, earliest first.
    pending: Vec<Window>,
    /// Open windows with their proxy and close time.
    open: Vec<(Window, String, u64)>,
    /// Proxies created so far.
    proxies: u16,
}

impl<'a> Links<'a> {
    pub(super) fn new(schedule: &'a Schedule, windows: &[Window]) -> Links<'a> {
        let mut pending = windows.to_vec();
        pending.sort_by_key(|w| (w.at_ms, w.instance));
        Links {
            schedule,
            pending,
            open: Vec::new(),
            proxies: 0,
        }
    }

    /// The name and Toxiproxy listen port of a new proxy for `instance`'s
    /// `incarnation`.
    pub(super) fn proxy(
        &mut self,
        instance: u32,
        incarnation: u32,
    ) -> Result<(String, u16), String> {
        let listen = LISTEN_PORTS.start() + self.proxies;
        if !LISTEN_PORTS.contains(&listen) {
            return Err(format!(
                "no Toxiproxy port left for w{instance}-{incarnation}"
            ));
        }
        self.proxies += 1;
        Ok((format!("w{instance}-{incarnation}"), listen))
    }

    /// Removes the earliest window due by `now_ms` that may open, and returns
    /// it with its proxy and pid. A window waits while its instance has a
    /// window open, has no live process in `live`, or runs the process
    /// carrying the `ErrAfterLand` plan, and then closes its duration after
    /// it opens.
    pub(super) fn next_open(
        &mut self,
        now_ms: u64,
        incarnations: &[u32],
        live: impl Fn(u32) -> Option<u32>,
    ) -> Option<(Window, String, u32)> {
        let at = self.pending.iter().position(|w| {
            let incarnation = incarnations[w.instance as usize];
            w.at_ms <= now_ms
                && !self.open.iter().any(|o| o.0.instance == w.instance)
                && link_faults(self.schedule, w.instance, incarnation)
                && live(w.instance).is_some()
        })?;
        let window = self.pending.remove(at);
        let pid = live(window.instance)?;
        let proxy = format!(
            "w{}-{}",
            window.instance, incarnations[window.instance as usize]
        );
        self.open
            .push((window, proxy.clone(), now_ms + window.duration_ms));
        Some((window, proxy, pid))
    }

    /// Removes an open window whose close is due by `now_ms`, and returns it
    /// with its proxy.
    pub(super) fn next_close(&mut self, now_ms: u64) -> Option<(Window, String)> {
        let at = self.open.iter().position(|o| o.2 <= now_ms)?;
        let (window, proxy, _) = self.open.remove(at);
        Some((window, proxy))
    }

    /// Closes every window due to close by `now_ms`, then opens every window
    /// [`Links::next_open`] releases, each after `log` journals its `toxic`
    /// line against the pid in `live`, and returns the opened windows'
    /// `faults_fired` entries.
    pub(super) fn step(
        &mut self,
        control: &impl Control,
        now_ms: u64,
        incarnations: &[u32],
        live: &[Option<u32>],
        log: impl Fn(Event) -> Result<(), String>,
    ) -> Result<Vec<FaultFired>, String> {
        while let Some((window, proxy)) = self.next_close(now_ms) {
            control.close(&proxy, window.kind)?;
        }
        let mut fired = Vec::new();
        while let Some((window, proxy, pid)) =
            self.next_open(now_ms, incarnations, |i| live[i as usize])
        {
            log(Event::Toxic {
                instance: format!("w{}", window.instance),
                pid,
                toxic: window.kind.to_string(),
                duration_ms: window.duration_ms,
            })?;
            control.open(&proxy, window.kind)?;
            fired.push(self::fired(&window, proxy, true));
        }
        Ok(fired)
    }

    /// The windows never opened, in drawn order.
    pub(super) fn rest(self) -> Vec<Window> {
        self.pending
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::journal::LeaderAtKill;
    use crate::run::proxy::MAX_DELAY_MS;

    fn window(at_ms: u64, instance: u32, duration_ms: u64) -> Window {
        Window {
            at_ms,
            instance,
            kind: LinkKind::Blackhole,
            duration_ms,
        }
    }

    /// Across seeds, a DynamoDB call's longest proxy delay plus its longest
    /// ordinary link latency is at most 200 ms, inside the SDK's read timeout
    /// of a quarter store timeout; heal-near-expiry windows exceed it.
    #[test]
    fn dynamodb_draws_keep_delay_plus_latency_under_the_read_timeout() {
        let tuning = Tuning::dynamodb();
        let read_timeout = tuning.op_timeout_ms / 4;
        let (mut latencies, mut heals) = (0, 0);
        for seed in 0..500 {
            for w in draw(
                &mut SplitMix64::new(seed),
                StoreKind::DynamoDb,
                3,
                None,
                &tuning,
            ) {
                match w.kind {
                    LinkKind::Latency { latency_ms } => {
                        assert!(latency_ms + MAX_DELAY_MS <= 200, "seed {seed}: {w:?}");
                        latencies += 1;
                    }
                    LinkKind::HealNearExpiry { latency_ms } => {
                        assert!(latency_ms > read_timeout, "seed {seed}: {w:?}");
                        heals += 1;
                    }
                    _ => {}
                }
            }
        }
        assert!(200 < read_timeout);
        assert!(latencies > 0 && heals > 0);
    }

    /// Across seeds, every kind is drawn on NATS and all but `limit_data` on
    /// DynamoDB; NATS latency stays below the store timeout; heal-near-expiry
    /// holds data past its own close; and windows fall inside their bounds.
    #[test]
    fn window_draws_stay_inside_their_bounds() {
        for (store, tuning, kinds) in [
            (StoreKind::Nats, Tuning::nats(), 5),
            (StoreKind::DynamoDb, Tuning::dynamodb(), 4),
        ] {
            let (lease, op) = (tuning.lease_ms, tuning.op_timeout_ms);
            let mut drawn = std::collections::HashSet::new();
            let (mut counts, mut instances) = (BTreeSet::new(), BTreeSet::new());
            for seed in 0..500 {
                let windows = draw(&mut SplitMix64::new(seed), store, 3, None, &tuning);
                counts.insert(windows.len());
                for w in windows {
                    instances.insert(w.instance);
                    assert!((FROM_MS..=UNTIL_MS).contains(&w.at_ms), "{w:?}");
                    assert!(w.instance < 3, "{w:?}");
                    let near = (lease - op..=lease + op).contains(&w.duration_ms);
                    let short = (lease / 2..=lease).contains(&w.duration_ms);
                    match w.kind {
                        LinkKind::Latency { latency_ms } => {
                            assert!(short && latency_ms < op, "{store:?} {w:?}");
                            assert!(store == StoreKind::DynamoDb || latency_ms <= 300);
                        }
                        LinkKind::HealNearExpiry { latency_ms } => {
                            assert!(near && latency_ms > w.duration_ms, "{w:?}");
                        }
                        LinkKind::Blackhole | LinkKind::Refuse => assert!(near, "{w:?}"),
                        LinkKind::LimitData { .. } => {
                            assert!(short && store == StoreKind::Nats, "{w:?}");
                        }
                    }
                    drawn.insert(std::mem::discriminant(&w.kind));
                }
            }
            assert_eq!(drawn.len(), kinds, "{store:?}");
            assert_eq!(counts, BTreeSet::from([1, 2, 3, 4]), "{store:?}");
            assert_eq!(instances, BTreeSet::from([0, 1, 2]), "{store:?}");
        }
    }

    /// No seed draws a window due after 5 s into the running stage, for
    /// either store and instance count.
    #[test]
    fn windows_fall_due_within_five_seconds() {
        for (store, tuning) in [
            (StoreKind::Nats, Tuning::nats()),
            (StoreKind::DynamoDb, Tuning::dynamodb()),
        ] {
            for (instances, lost) in [(1, None), (1, Some(0)), (3, None), (3, Some(1))] {
                for seed in 0..1000 {
                    let mut rng = SplitMix64::new(seed);
                    for w in draw(&mut rng, store, instances, lost, &tuning) {
                        assert!(w.at_ms <= 5_000, "seed {seed}: {w:?}");
                    }
                }
            }
        }
    }

    /// Latency and heal-near-expiry slow what the worker sends, a blackhole
    /// drops both directions, `limit_data` counts what reaches the worker,
    /// and refuse adds no toxic.
    #[test]
    fn windows_map_to_their_toxics() {
        use std::time::Duration;
        let ms = Duration::from_millis;
        assert_eq!(
            LinkKind::Latency { latency_ms: 40 }.toxics(),
            [("latency", Stream::Upstream, Toxic::Latency(ms(40)))]
        );
        assert_eq!(
            LinkKind::HealNearExpiry { latency_ms: 4_000 }.toxics(),
            [("latency", Stream::Upstream, Toxic::Latency(ms(4_000)))]
        );
        assert_eq!(
            LinkKind::Blackhole.toxics(),
            [
                ("blackhole-up", Stream::Upstream, Toxic::Timeout(ms(0))),
                ("blackhole-down", Stream::Downstream, Toxic::Timeout(ms(0))),
            ]
        );
        assert_eq!(
            LinkKind::LimitData { bytes: 900 }.toxics(),
            [("limit", Stream::Downstream, Toxic::LimitData(900))]
        );
        assert!(LinkKind::Refuse.toxics().is_empty());
    }

    /// A window opens on the instance's current process once due, and closes
    /// its duration after it opened, which may be later than it was due.
    #[test]
    fn a_window_closes_its_duration_after_it_opens() {
        let schedule = Schedule::default();
        let mut links = Links::new(&schedule, &[window(1_000, 0, 500)]);
        assert_eq!(links.next_open(999, &[2], |_| Some(7)), None);
        let opened = links.next_open(1_200, &[2], |_| Some(7));
        assert_eq!(opened, Some((window(1_000, 0, 500), "w0-2".to_owned(), 7)));
        assert_eq!(links.next_close(1_699), None);
        assert_eq!(
            links.next_close(1_700),
            Some((window(1_000, 0, 500), "w0-2".to_owned()))
        );
    }

    /// A window due while its instance has one open, or has no live process,
    /// waits; a window on another instance opens meanwhile.
    #[test]
    fn a_window_waits_while_its_instance_is_busy_or_down() {
        let schedule = Schedule::default();
        let windows = [
            window(1_000, 0, 500),
            window(1_100, 0, 100),
            window(1_200, 1, 100),
        ];
        let mut links = Links::new(&schedule, &windows);
        let up = |_| Some(7);
        assert!(links.next_open(1_000, &[1, 1], up).is_some());
        let next = links.next_open(1_300, &[1, 1], up).map(|o| o.0);
        assert_eq!(next, Some(windows[2]));
        assert_eq!(links.next_open(1_300, &[1, 1], up), None);
        assert!(links.next_close(1_500).is_some());
        assert_eq!(links.next_open(1_500, &[1, 1], |_| None), None);
        let next = links.next_open(1_600, &[2, 1], up).map(|o| (o.0, o.1));
        assert_eq!(next, Some((windows[1], "w0-2".to_owned())));
        assert!(links.rest().is_empty());
    }

    /// A run that drew windows and opened none fails its expectation; one
    /// that opened a window, or drew none, does not.
    #[test]
    fn a_run_that_opens_no_window_is_not_exercised() {
        let toxic = Line {
            t_ms: 1,
            event: Event::Toxic {
                instance: "w0".to_owned(),
                pid: 10,
                toxic: "blackhole".to_owned(),
                duration_ms: 100,
            },
        };
        let kill = Line {
            t_ms: 2,
            event: Event::Kill {
                instance: "w0".to_owned(),
                pid: 10,
                leader: LeaderAtKill::Unread,
            },
        };
        assert_eq!(
            unexercised(true, std::slice::from_ref(&kill)).as_deref(),
            Some("fault not exercised: no toxic line")
        );
        assert_eq!(unexercised(true, &[kill.clone(), toxic]), None);
        assert_eq!(unexercised(false, &[kill]), None);
    }

    /// What [`Links::step`] did, in order.
    #[derive(Debug, PartialEq)]
    enum Call {
        Log(Event),
        Open(String, LinkKind),
        Close(String, LinkKind),
    }

    #[derive(Default)]
    struct Fake(std::cell::RefCell<Vec<Call>>);

    impl Control for Fake {
        fn open(&self, proxy: &str, kind: LinkKind) -> Result<(), String> {
            self.0.borrow_mut().push(Call::Open(proxy.to_owned(), kind));
            Ok(())
        }

        fn close(&self, proxy: &str, kind: LinkKind) -> Result<(), String> {
            self.0
                .borrow_mut()
                .push(Call::Close(proxy.to_owned(), kind));
            Ok(())
        }
    }

    /// A window's `toxic` line carries its own instance's pid and precedes the
    /// open on that instance's current proxy, and each window closes once its
    /// duration has passed.
    #[test]
    fn a_step_journals_each_window_before_opening_it_and_closes_it_when_due() {
        let latency = LinkKind::Latency { latency_ms: 40 };
        let windows = [
            window(1_000, 0, 500),
            Window {
                kind: latency,
                ..window(1_000, 1, 300)
            },
        ];
        let schedule = Schedule::default();
        let mut links = Links::new(&schedule, &windows);
        let fake = Fake::default();
        let log = |event| {
            fake.0.borrow_mut().push(Call::Log(event));
            Ok(())
        };
        let (incarnations, live) = ([2, 1], [Some(10), Some(11)]);
        let fired = links.step(&fake, 1_000, &incarnations, &live, log);
        assert_eq!(fired.map(|f| f.len()), Ok(2));
        links.step(&fake, 1_299, &incarnations, &live, log).unwrap();
        links.step(&fake, 1_300, &incarnations, &live, log).unwrap();
        links.step(&fake, 1_500, &incarnations, &live, log).unwrap();
        let toxic = |instance: &str, pid, toxic: &str, duration_ms| {
            Call::Log(Event::Toxic {
                instance: instance.to_owned(),
                pid,
                toxic: toxic.to_owned(),
                duration_ms,
            })
        };
        assert_eq!(
            fake.0.into_inner(),
            [
                toxic("w0", 10, "blackhole", 500),
                Call::Open("w0-2".to_owned(), LinkKind::Blackhole),
                toxic("w1", 11, "latency(40ms)", 300),
                Call::Open("w1-1".to_owned(), latency),
                Call::Close("w1-1".to_owned(), latency),
                Call::Close("w0-2".to_owned(), LinkKind::Blackhole),
            ]
        );
    }

    /// A window due on an instance whose open window closes in the same step
    /// opens in that step and is reported fired; one still waiting is left for
    /// [`Links::rest`].
    #[test]
    fn a_step_closes_before_it_opens_and_leaves_the_rest() {
        let windows = [
            window(1_000, 0, 500),
            window(1_200, 0, 100),
            window(1_300, 0, 100),
        ];
        let schedule = Schedule::default();
        let mut links = Links::new(&schedule, &windows);
        let fake = Fake::default();
        let (incarnations, live) = ([1], [Some(10)]);
        links
            .step(&fake, 1_000, &incarnations, &live, |_| Ok(()))
            .unwrap();
        let fired = links.step(&fake, 1_500, &incarnations, &live, |_| Ok(()));
        assert_eq!(
            fired,
            Ok(vec![self::fired(&windows[1], "w0-1".to_owned(), true)])
        );
        assert_eq!(links.rest(), [windows[2]]);
    }

    /// Each proxy takes the next listen port, and none is handed out past
    /// the last.
    #[test]
    fn proxies_take_listen_ports_in_turn() {
        let schedule = Schedule::default();
        let mut links = Links::new(&schedule, &[]);
        assert_eq!(links.proxy(0, 1), Ok(("w0-1".to_owned(), 21_000)));
        assert_eq!(links.proxy(1, 1), Ok(("w1-1".to_owned(), 21_001)));
        for _ in 2..16 {
            links.proxy(2, 1).unwrap();
        }
        assert!(links.proxy(2, 2).is_err());
    }

    /// With more than one instance no window is drawn on the instance whose
    /// first process carries the lost reply; with one, every window is on it.
    #[test]
    fn windows_skip_the_lost_reply_instance_of_a_three_instance_run() {
        let tuning = Tuning::nats();
        let mut hit = BTreeSet::new();
        for seed in 0..500 {
            for lost in 0..3 {
                for w in draw(
                    &mut SplitMix64::new(seed),
                    StoreKind::Nats,
                    3,
                    Some(lost),
                    &tuning,
                ) {
                    assert_ne!(w.instance, lost, "seed {seed}: {w:?}");
                    hit.insert((lost, w.instance));
                }
            }
            let one = draw(
                &mut SplitMix64::new(seed),
                StoreKind::Nats,
                1,
                Some(0),
                &tuning,
            );
            assert!(one.iter().all(|w| w.instance == 0), "seed {seed}: {one:?}");
        }
        assert_eq!(hit.len(), 6, "{hit:?}");
    }
}
