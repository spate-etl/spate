//! The seeded fault schedule a run applies to its worker processes, and the
//! timeline that applies it on real time.

use std::fmt::Write as _;

use crate::classify::WriteKind;
use crate::seed::SplitMix64;
use crate::store::{AbortMode, AbortPlan};

/// Earliest kill, from the start of the running stage.
const KILL_FROM_MS: u64 = 1_000;
/// Latest kill, from the start of the running stage.
const KILL_UNTIL_MS: u64 = 6_000;

/// A SIGKILL of one instance's live process, and its replacement under the
/// same instance id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Kill {
    /// When the kill is due, from the start of the running stage.
    pub at_ms: u64,
    /// Instance index, `w<index>`.
    pub instance: u32,
    /// How long after the kill the replacement starts, at most one lease.
    pub respawn_after_ms: u64,
}

/// One step of a schedule, due `at_ms` after the start of the running stage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// SIGKILL the instance's live process.
    Kill {
        /// When the step is due.
        at_ms: u64,
        /// Instance index.
        instance: u32,
    },
    /// Start a replacement for the instance's ended process.
    Respawn {
        /// When the step is due.
        at_ms: u64,
        /// Instance index.
        instance: u32,
    },
}

impl Action {
    /// When the step is due.
    #[must_use]
    pub fn at_ms(self) -> u64 {
        match self {
            Action::Kill { at_ms, .. } | Action::Respawn { at_ms, .. } => at_ms,
        }
    }
}

/// The in-process fault one instance's first process carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InProcess {
    /// Instance index, `w<index>`.
    pub instance: u32,
    /// The fault.
    pub plan: AbortPlan,
    /// How long after an abort the replacement starts, at most one lease.
    pub respawn_after_ms: u64,
}

/// The faults drawn for one run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Schedule {
    /// Kills in the order drawn.
    pub kills: Vec<Kill>,
    /// In-process faults, at most one per instance.
    pub in_process: Vec<InProcess>,
}

impl Schedule {
    /// Draws between one and `instances + 1` kills over the first six
    /// seconds of the running stage, each with a replacement within `lease_ms`.
    /// Then one instance's first process gets an `ErrAfterLand` plan, and
    /// each other instance's first process an abort before or after a write.
    #[must_use]
    pub fn draw(rng: &mut SplitMix64, instances: u32, lease_ms: u64) -> Schedule {
        let count = rng.in_range(1, u64::from(instances) + 1);
        let instance = |rng: &mut SplitMix64| {
            u32::try_from(rng.in_range(0, u64::from(instances) - 1))
                .expect("an instance index fits in u32")
        };
        let kills = (0..count)
            .map(|_| Kill {
                at_ms: rng.in_range(KILL_FROM_MS, KILL_UNTIL_MS),
                instance: instance(rng),
                respawn_after_ms: rng.in_range(0, lease_ms),
            })
            .collect();
        let lost = instance(rng);
        let mut in_process = vec![InProcess {
            instance: lost,
            plan: draw_err_after_land(rng),
            respawn_after_ms: 0,
        }];
        for i in (0..instances).filter(|i| *i != lost) {
            in_process.push(InProcess {
                instance: i,
                plan: draw_abort(rng),
                respawn_after_ms: rng.in_range(0, lease_ms),
            });
        }
        Schedule { kills, in_process }
    }

    /// The plan `instance`'s `incarnation` carries: only a first process
    /// carries one.
    #[must_use]
    pub fn plan_for(&self, instance: u32, incarnation: u32) -> Option<&InProcess> {
        if incarnation != 1 {
            return None;
        }
        self.in_process.iter().find(|p| p.instance == instance)
    }

    /// The instance whose first process carries the `ErrAfterLand` plan.
    #[must_use]
    pub fn lost_reply(&self) -> Option<u32> {
        self.in_process
            .iter()
            .find(|p| p.plan.mode == AbortMode::ErrAfterLand)
            .map(|p| p.instance)
    }

    /// The steps in time order, each kill followed by its respawn. A kill due
    /// while its instance waits for a replacement is dropped with its respawn.
    #[must_use]
    pub fn actions(&self) -> Vec<Action> {
        let mut actions = Vec::new();
        for kill in self.rendered_kills() {
            actions.push(Action::Kill {
                at_ms: kill.at_ms,
                instance: kill.instance,
            });
            actions.push(Action::Respawn {
                at_ms: kill.at_ms + kill.respawn_after_ms,
                instance: kill.instance,
            });
        }
        actions.sort_by_key(|a| (a.at_ms(), matches!(a, Action::Respawn { .. })));
        actions
    }

    /// The kills [`Schedule::actions`] keeps, in time order.
    fn rendered_kills(&self) -> Vec<Kill> {
        let mut kills = self.kills.clone();
        kills.sort_by_key(|k| (k.at_ms, k.instance));
        let mut down_until: Vec<(u32, u64)> = Vec::new();
        kills.retain(|kill| {
            let respawn_at = kill.at_ms + kill.respawn_after_ms;
            match down_until.iter_mut().find(|(i, _)| *i == kill.instance) {
                Some((_, until)) if kill.at_ms <= *until => return false,
                Some((_, until)) => *until = respawn_at,
                None => down_until.push((kill.instance, respawn_at)),
            }
            true
        });
        kills
    }

    /// One line per step and per in-process fault, as a failure message
    /// shows it.
    #[must_use]
    pub fn render(&self) -> String {
        let mut text = String::new();
        for p in &self.in_process {
            let _ = match p.plan.mode {
                AbortMode::ErrAfterLand => writeln!(text, "w{}-1: {}", p.instance, p.plan),
                AbortMode::Before | AbortMode::After => writeln!(
                    text,
                    "w{}-1: {}, respawn {} ms later",
                    p.instance, p.plan, p.respawn_after_ms
                ),
            };
        }
        for action in self.actions() {
            let _ = match action {
                Action::Kill { at_ms, instance } => writeln!(text, "{at_ms} ms: kill w{instance}"),
                Action::Respawn { at_ms, instance } => {
                    writeln!(text, "{at_ms} ms: respawn w{instance}")
                }
            };
        }
        text
    }
}

/// Kinds an `ErrAfterLand` is drawn on: the durable writes whose recovery the
/// journal can show. A renewal is ephemeral, so it is never drawn.
const LOST_REPLY_KINDS: [WriteKind; 3] = [WriteKind::Claim, WriteKind::Commit, WriteKind::Complete];
/// Kinds an abort is drawn on.
const ABORT_KINDS: [WriteKind; 4] = [
    WriteKind::Claim,
    WriteKind::Commit,
    WriteKind::Complete,
    WriteKind::Renew,
];

/// An `ErrAfterLand` plan at a write every worker that holds a split reaches.
fn draw_err_after_land(rng: &mut SplitMix64) -> AbortPlan {
    let kind = LOST_REPLY_KINDS[pick(rng, LOST_REPLY_KINDS.len())];
    let n = if kind == WriteKind::Commit {
        rng.in_range(1, 2)
    } else {
        1
    };
    AbortPlan {
        kind,
        n: u32::try_from(n).expect("small"),
        mode: AbortMode::ErrAfterLand,
    }
}

/// An abort before or after one of the first three writes of a kind.
fn draw_abort(rng: &mut SplitMix64) -> AbortPlan {
    let kind = ABORT_KINDS[pick(rng, ABORT_KINDS.len())];
    let mode = if rng.next_u64().is_multiple_of(2) {
        AbortMode::Before
    } else {
        AbortMode::After
    };
    AbortPlan {
        kind,
        n: u32::try_from(rng.in_range(1, 3)).expect("small"),
        mode,
    }
}

fn pick(rng: &mut SplitMix64, len: usize) -> usize {
    usize::try_from(rng.in_range(0, len as u64 - 1)).expect("an index fits in usize")
}

/// A step [`Timeline::next`] hands the harness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// SIGKILL the instance's live process. When one was live, the harness
    /// schedules its replacement with [`Timeline::respawn`] at
    /// `respawn_at_ms`.
    Kill {
        /// When the schedule drew it.
        at_ms: u64,
        /// Instance index.
        instance: u32,
        /// One respawn delay after the kill was released.
        respawn_at_ms: u64,
    },
    /// Start a replacement for the instance's ended process.
    Respawn {
        /// When the step is due.
        at_ms: u64,
        /// Instance index.
        instance: u32,
    },
}

/// A schedule applied on real time: the kills it renders, the respawns kills
/// and aborts cause, and kills held while the harness says so.
///
/// With nothing held and every kill firing, it hands out the steps of
/// [`Schedule::actions`] in the same order. Every rendered kill is handed out
/// or returned by [`Timeline::drain_kills`], including one due while its
/// instance waits for a replacement, and no other kill is.
#[derive(Debug, Default)]
pub struct Timeline {
    pending: Vec<Pending>,
}

#[derive(Debug)]
struct Pending {
    action: Action,
    respawn_after_ms: u64,
    /// The kill was due and held.
    held: bool,
}

impl Timeline {
    /// The timeline of the kills `schedule` renders.
    #[must_use]
    pub fn new(schedule: &Schedule) -> Timeline {
        let pending = schedule
            .rendered_kills()
            .into_iter()
            .map(|k| Pending {
                action: Action::Kill {
                    at_ms: k.at_ms,
                    instance: k.instance,
                },
                respawn_after_ms: k.respawn_after_ms,
                held: false,
            })
            .collect();
        Timeline { pending }
    }

    /// Schedules a replacement for `instance`'s ended process at `at_ms`.
    pub fn respawn(&mut self, instance: u32, at_ms: u64) {
        self.pending.push(Pending {
            action: Action::Respawn { at_ms, instance },
            respawn_after_ms: 0,
            held: false,
        });
    }

    /// Whether some instance waits for a replacement.
    #[must_use]
    pub fn awaiting_respawn(&self) -> bool {
        self.pending
            .iter()
            .any(|p| matches!(p.action, Action::Respawn { .. }))
    }

    /// The earliest step due by `now_ms`, kills before respawns due at the
    /// same time. A kill for which `held` returns true stays pending, and once
    /// released its replacement waits one respawn delay from `now_ms`.
    pub fn next(&mut self, now_ms: u64, mut held: impl FnMut(u32) -> bool) -> Option<Step> {
        let mut due: Vec<usize> = (0..self.pending.len())
            .filter(|&i| self.pending[i].action.at_ms() <= now_ms)
            .collect();
        due.sort_by_key(|&i| {
            let a = self.pending[i].action;
            let (Action::Kill { instance, .. } | Action::Respawn { instance, .. }) = a;
            (a.at_ms(), matches!(a, Action::Respawn { .. }), instance)
        });
        let mut step = None;
        for i in due {
            let (action, respawn_after_ms, was_held) = {
                let p = &self.pending[i];
                (p.action, p.respawn_after_ms, p.held)
            };
            match action {
                Action::Respawn { at_ms, instance } => {
                    step = Some((i, Step::Respawn { at_ms, instance }));
                    break;
                }
                Action::Kill { at_ms, instance } => {
                    if held(instance) {
                        self.pending[i].held = true;
                        continue;
                    }
                    let from = if was_held { now_ms } else { at_ms };
                    step = Some((
                        i,
                        Step::Kill {
                            at_ms,
                            instance,
                            respawn_at_ms: from + respawn_after_ms,
                        },
                    ));
                    break;
                }
            }
        }
        let (i, step) = step?;
        self.pending.remove(i);
        Some(step)
    }

    /// Removes every pending step and returns the kills among them.
    pub fn drain_kills(&mut self) -> Vec<Action> {
        let mut kills: Vec<Action> = self
            .pending
            .drain(..)
            .map(|p| p.action)
            .filter(|a| matches!(a, Action::Kill { .. }))
            .collect();
        kills.sort_by_key(|a| a.at_ms());
        kills
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEASE: u64 = 2_000;

    /// Across seeds, each kill is followed by a respawn of the same instance
    /// within one lease, before that instance is killed again.
    #[test]
    fn every_kill_is_followed_by_a_respawn() {
        for seed in 0..200 {
            let schedule = Schedule::draw(&mut SplitMix64::new(seed), 3, LEASE);
            let actions = schedule.actions();
            assert!(!actions.is_empty(), "seed {seed}");
            for (i, action) in actions.iter().enumerate() {
                let Action::Kill { at_ms, instance } = *action else {
                    continue;
                };
                let next = actions[i + 1..]
                    .iter()
                    .find(|a| matches!(a, Action::Kill { instance: j, .. } | Action::Respawn { instance: j, .. } if *j == instance));
                match next {
                    Some(Action::Respawn { at_ms: r, .. }) => {
                        assert!(
                            *r >= at_ms && *r <= at_ms + LEASE,
                            "seed {seed}: {actions:?}"
                        );
                    }
                    other => panic!("seed {seed}: kill of w{instance} followed by {other:?}"),
                }
            }
        }
    }

    /// The same seed draws the same schedule, and another seed another one.
    #[test]
    fn schedule_replays_from_the_seed() {
        let draw = |seed| Schedule::draw(&mut SplitMix64::new(seed), 3, LEASE);
        assert_eq!(draw(5), draw(5));
        assert_eq!(draw(5).render(), draw(5).render());
        assert_ne!(draw(5), draw(6));
    }

    /// Across seeds, the kill count spans one to one more than the
    /// instances, every instance is drawn, and kills fall inside the window.
    #[test]
    fn draws_stay_inside_their_bounds() {
        let mut instances_hit = [false; 3];
        let mut counts = std::collections::BTreeSet::new();
        for seed in 0..200 {
            let schedule = Schedule::draw(&mut SplitMix64::new(seed), 3, LEASE);
            counts.insert(schedule.kills.len());
            for kill in &schedule.kills {
                assert!((KILL_FROM_MS..=KILL_UNTIL_MS).contains(&kill.at_ms));
                instances_hit[kill.instance as usize] = true;
            }
        }
        assert_eq!(counts.into_iter().collect::<Vec<_>>(), [1, 2, 3, 4]);
        assert_eq!(instances_hit, [true; 3]);
        let one = Schedule::draw(&mut SplitMix64::new(1), 1, LEASE);
        assert!(one.kills.iter().all(|k| k.instance == 0));
    }

    /// A kill due while its instance waits for a replacement is dropped with
    /// its respawn; a kill on another instance at that time stays.
    #[test]
    fn a_kill_while_its_instance_is_down_is_dropped() {
        let kill = |at_ms, instance, respawn_after_ms| Kill {
            at_ms,
            instance,
            respawn_after_ms,
        };
        let schedule = Schedule {
            kills: vec![kill(1_000, 0, 500), kill(1_200, 0, 0), kill(1_200, 1, 100)],
            ..Schedule::default()
        };
        assert_eq!(
            schedule.actions(),
            [
                Action::Kill {
                    at_ms: 1_000,
                    instance: 0
                },
                Action::Kill {
                    at_ms: 1_200,
                    instance: 1
                },
                Action::Respawn {
                    at_ms: 1_300,
                    instance: 1
                },
                Action::Respawn {
                    at_ms: 1_500,
                    instance: 0
                },
            ]
        );
    }

    /// A kill drawn with a zero respawn delay comes before its respawn.
    #[test]
    fn a_zero_delay_respawn_follows_its_kill() {
        let schedule = Schedule {
            kills: vec![Kill {
                at_ms: 1_000,
                instance: 0,
                respawn_after_ms: 0,
            }],
            ..Schedule::default()
        };
        assert_eq!(
            schedule.actions(),
            [
                Action::Kill {
                    at_ms: 1_000,
                    instance: 0
                },
                Action::Respawn {
                    at_ms: 1_000,
                    instance: 0
                },
            ]
        );
    }

    /// Kills are judged in time order, whatever order they were drawn in, and
    /// a kill due when its instance's replacement starts is dropped.
    #[test]
    fn kills_are_dropped_in_time_order() {
        let schedule = Schedule {
            kills: vec![
                Kill {
                    at_ms: 1_500,
                    instance: 0,
                    respawn_after_ms: 0,
                },
                Kill {
                    at_ms: 1_000,
                    instance: 0,
                    respawn_after_ms: 500,
                },
            ],
            ..Schedule::default()
        };
        assert_eq!(
            schedule.actions(),
            [
                Action::Kill {
                    at_ms: 1_000,
                    instance: 0
                },
                Action::Respawn {
                    at_ms: 1_500,
                    instance: 0
                },
            ]
        );
    }

    /// Across seeds and instance counts, each run draws one `ErrAfterLand`
    /// plan, on a claim, commit or completion and never on a renewal, and an
    /// abort before or after a write on every other instance.
    #[test]
    fn err_after_land_is_never_drawn_on_renew() {
        let mut lost_kinds = std::collections::BTreeSet::new();
        let mut abort_kinds = std::collections::BTreeSet::new();
        for instances in [1, 3] {
            for seed in 0..300 {
                let schedule = Schedule::draw(&mut SplitMix64::new(seed), instances, LEASE);
                let lost: Vec<_> = schedule
                    .in_process
                    .iter()
                    .filter(|p| p.plan.mode == AbortMode::ErrAfterLand)
                    .collect();
                assert_eq!(lost.len(), 1, "seed {seed}");
                assert!(LOST_REPLY_KINDS.contains(&lost[0].plan.kind));
                lost_kinds.insert(format!("{:?}", lost[0].plan.kind));
                let mut covered: Vec<u32> =
                    schedule.in_process.iter().map(|p| p.instance).collect();
                covered.sort_unstable();
                assert_eq!(covered, (0..instances).collect::<Vec<_>>());
                for p in schedule
                    .in_process
                    .iter()
                    .filter(|p| p.instance != lost[0].instance)
                {
                    assert!(matches!(p.plan.mode, AbortMode::Before | AbortMode::After));
                    assert!((1..=3).contains(&p.plan.n) && p.respawn_after_ms <= LEASE);
                    abort_kinds.insert(format!("{:?}", p.plan.kind));
                }
            }
        }
        assert_eq!(lost_kinds.len(), 3, "{lost_kinds:?}");
        assert!(abort_kinds.contains("Renew"), "{abort_kinds:?}");
    }

    /// Only an instance's first process carries its plan; every replacement
    /// starts with none.
    #[test]
    fn replacement_config_drops_abort_and_err_after_land_plans() {
        let schedule = Schedule::draw(&mut SplitMix64::new(3), 3, LEASE);
        for instance in 0..3 {
            assert!(schedule.plan_for(instance, 1).is_some());
            for incarnation in 2..5 {
                assert_eq!(schedule.plan_for(instance, incarnation), None);
            }
        }
    }

    /// With nothing held and every kill firing, the timeline hands out the
    /// rendered actions in order.
    #[test]
    fn a_timeline_with_nothing_held_applies_the_rendered_actions() {
        for seed in 0..200 {
            let schedule = Schedule::draw(&mut SplitMix64::new(seed), 3, LEASE);
            let mut timeline = Timeline::new(&schedule);
            let mut applied = Vec::new();
            while let Some(step) = timeline.next(u64::MAX, |_| false) {
                applied.push(match step {
                    Step::Kill {
                        at_ms,
                        instance,
                        respawn_at_ms,
                    } => {
                        timeline.respawn(instance, respawn_at_ms);
                        Action::Kill { at_ms, instance }
                    }
                    Step::Respawn { at_ms, instance } => Action::Respawn { at_ms, instance },
                });
            }
            assert_eq!(applied, schedule.actions(), "seed {seed}");
        }
    }

    /// A rendered kill due while its instance waits for a replacement is
    /// handed out, so the harness records it.
    #[test]
    fn a_kill_while_its_instance_awaits_a_replacement_is_handed_out() {
        let schedule = Schedule {
            kills: vec![Kill {
                at_ms: 2_000,
                instance: 0,
                respawn_after_ms: 0,
            }],
            ..Schedule::default()
        };
        let mut timeline = Timeline::new(&schedule);
        timeline.respawn(0, 3_000);
        assert_eq!(
            timeline.next(2_000, |_| false),
            Some(Step::Kill {
                at_ms: 2_000,
                instance: 0,
                respawn_at_ms: 2_000
            })
        );
        assert_eq!(timeline.next(2_000, |_| false), None);
        assert!(timeline.awaiting_respawn());
    }

    /// A kill the schedule omits, due on an instance's replacement after an
    /// abort, is never handed out.
    #[test]
    fn a_kill_the_schedule_omits_is_never_handed_out() {
        let schedule = Schedule {
            kills: vec![
                Kill {
                    at_ms: 2_000,
                    instance: 1,
                    respawn_after_ms: 500,
                },
                Kill {
                    at_ms: 2_100,
                    instance: 1,
                    respawn_after_ms: 0,
                },
            ],
            ..Schedule::default()
        };
        assert!(!schedule.render().contains("2100 ms: kill w1"));
        let mut timeline = Timeline::new(&schedule);
        timeline.respawn(1, 2_050);
        let mut live = false;
        let mut fired = Vec::new();
        for now in [2_000, 2_050, 2_100] {
            while let Some(step) = timeline.next(now, |_| false) {
                match step {
                    Step::Kill {
                        at_ms,
                        instance,
                        respawn_at_ms,
                    } => {
                        if live {
                            timeline.respawn(instance, respawn_at_ms);
                        }
                        fired.push((at_ms, live));
                        live = false;
                    }
                    Step::Respawn { .. } => live = true,
                }
            }
        }
        assert_eq!(fired, [(2_000, false)]);
        assert!(timeline.drain_kills().is_empty());
    }
}
