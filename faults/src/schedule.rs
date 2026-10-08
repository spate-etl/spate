//! The seeded fault schedule a run applies to its worker processes.

use std::fmt::Write as _;

use crate::seed::SplitMix64;

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
    /// Start a replacement for the instance's killed process.
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

/// The kills drawn for one run.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Schedule {
    /// Kills in the order drawn.
    pub kills: Vec<Kill>,
}

impl Schedule {
    /// Draws between one and `instances + 1` kills over the first six
    /// seconds of the running stage, each with a replacement within `lease_ms`.
    #[must_use]
    pub fn draw(rng: &mut SplitMix64, instances: u32, lease_ms: u64) -> Schedule {
        let count = rng.in_range(1, u64::from(instances) + 1);
        let kills = (0..count)
            .map(|_| Kill {
                at_ms: rng.in_range(KILL_FROM_MS, KILL_UNTIL_MS),
                instance: u32::try_from(rng.in_range(0, u64::from(instances) - 1))
                    .expect("an instance index fits in u32"),
                respawn_after_ms: rng.in_range(0, lease_ms),
            })
            .collect();
        Schedule { kills }
    }

    /// The steps in time order, each kill followed by its respawn. A kill due
    /// while its instance waits for a replacement is dropped with its respawn.
    #[must_use]
    pub fn actions(&self) -> Vec<Action> {
        let mut kills = self.kills.clone();
        kills.sort_by_key(|k| (k.at_ms, k.instance));
        let mut down_until: Vec<(u32, u64)> = Vec::new();
        let mut actions = Vec::new();
        for kill in kills {
            let respawn_at = kill.at_ms + kill.respawn_after_ms;
            match down_until.iter_mut().find(|(i, _)| *i == kill.instance) {
                Some((_, until)) if kill.at_ms <= *until => continue,
                Some((_, until)) => *until = respawn_at,
                None => down_until.push((kill.instance, respawn_at)),
            }
            actions.push(Action::Kill {
                at_ms: kill.at_ms,
                instance: kill.instance,
            });
            actions.push(Action::Respawn {
                at_ms: respawn_at,
                instance: kill.instance,
            });
        }
        actions.sort_by_key(|a| (a.at_ms(), matches!(a, Action::Respawn { .. })));
        actions
    }

    /// One line per step, as a failure message shows it.
    #[must_use]
    pub fn render(&self) -> String {
        let mut text = String::new();
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
}
