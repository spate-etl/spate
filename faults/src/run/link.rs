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
/// Latest window, from the start of the running stage.
const UNTIL_MS: u64 = 10_000;
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
    /// Drops everything both ways, and resets the connection on heal.
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

/// Draws between one and `instances + 1` windows over the first ten seconds
/// of the running stage. Ordinary latency lasts half a lease to a lease;
/// heal-near-expiry, blackhole and refuse windows last a lease give or take
/// one store timeout; `limit_data` is drawn on NATS only.
pub(super) fn draw(
    rng: &mut SplitMix64,
    store: StoreKind,
    instances: u32,
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
            let instance = u32::try_from(rng.in_range(0, u64::from(instances) - 1))
                .expect("an instance index fits in u32");
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

/// Opens a `kind` window on `proxy`.
pub(super) fn open(toxiproxy: &Toxiproxy, proxy: &str, kind: LinkKind) -> Result<(), String> {
    if kind == LinkKind::Refuse {
        return toxiproxy.set_enabled(proxy, false);
    }
    for (name, stream, toxic) in kind.toxics() {
        toxiproxy.add_toxic(proxy, name, stream, toxic)?;
    }
    Ok(())
}

/// Closes the `kind` window [`open`] opened on `proxy`.
pub(super) fn close(toxiproxy: &Toxiproxy, proxy: &str, kind: LinkKind) -> Result<(), String> {
    if kind == LinkKind::Refuse {
        return toxiproxy.set_enabled(proxy, true);
    }
    for (name, _, _) in kind.toxics() {
        toxiproxy.remove_toxic(proxy, name)?;
    }
    Ok(())
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

    /// The windows never opened, in drawn order.
    pub(super) fn rest(self) -> Vec<Window> {
        self.pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
            for w in draw(&mut SplitMix64::new(seed), StoreKind::DynamoDb, 3, &tuning) {
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
            for seed in 0..500 {
                for w in draw(&mut SplitMix64::new(seed), store, 3, &tuning) {
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
            },
        };
        assert_eq!(
            unexercised(true, std::slice::from_ref(&kill)).as_deref(),
            Some("fault not exercised: no toxic line")
        );
        assert_eq!(unexercised(true, &[kill.clone(), toxic]), None);
        assert_eq!(unexercised(false, &[kill]), None);
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
}
