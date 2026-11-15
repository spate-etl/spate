//! The milestone step, run by hand after `finish`: the released milestone takes
//! every issue the release closed as completed and closes, and its unfinished
//! issues move to the next milestone or, after a second miss, return to the
//! backlog. RELEASING.md is the account of the rule.

use super::io::{
    DryForge, Forge, Git, Host, Issue, Milestone, MilestoneEvent, RELEASE_BLOCKER,
    SECURITY_LABEL, Target, Tracked,
};
use super::version::Version;
use crate::checks::subject::printable;
use crate::run::{Error, Outcome};

/// The label a returning issue gets when it carries no priority.
const DEMOTED: &str = "priority: medium";

const PRIORITY: &str = "priority: ";

/// The longest gap between the two events of one milestone change. A move
/// writes `demilestoned` and `milestoned` within a second of each other.
const MOVE_GAP: i64 = 60;

/// One write the step makes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Change {
    /// The released milestone does not exist.
    Create,
    /// The released milestone is closed and issues are still to be stamped.
    Reopen,
    /// A closed issue the release shipped.
    Stamp(Issue),
    /// An unfinished issue goes to the next milestone. `held` names the label
    /// that kept an issue which slipped before from returning to the backlog.
    Move {
        issue: Issue,
        to: Target,
        held: Option<&'static str>,
    },
    /// An issue that slipped before returns to the backlog, labeled
    /// [`DEMOTED`] when `label` is set.
    Demote { issue: Issue, label: bool },
    Close,
}

impl Change {
    /// The change as one log line, with issue titles made printable.
    fn describe(&self, release: &str) -> String {
        let issue = |verb: &str, i: &Issue, rest: &str| {
            format!(
                "{verb} #{}{rest}: {} ({})",
                i.number,
                printable(&i.title),
                printable(&i.url)
            )
        };
        match self {
            Self::Create => format!("create {release}"),
            Self::Reopen => format!("reopen {release}"),
            Self::Stamp(i) => issue("stamp", i, &format!(" on {release}")),
            Self::Move { issue: i, to, held } => {
                let why = held.map_or_else(String::new, |l| {
                    format!(", which slipped before and keeps a milestone as `{l}`")
                });
                issue("move", i, &format!(" to {}{why}", to.title))
            }
            Self::Demote { issue: i, label } => {
                let rest = if *label {
                    format!(" to the backlog, labeled `{DEMOTED}`")
                } else {
                    " to the backlog".to_owned()
                };
                issue("return", i, &rest)
            }
            Self::Close => format!("close {release}"),
        }
    }
}

/// Everything the plan rests on, read before any write.
#[derive(Clone, Debug, Default)]
pub(crate) struct Reads {
    pub(crate) milestones: Vec<Milestone>,
    /// The closed issues updated since the window opened.
    pub(crate) closed: Vec<Tracked>,
    /// The open issues on the released milestone, each with its events.
    pub(crate) open: Vec<(Tracked, Vec<MilestoneEvent>)>,
    /// When the previous release landed, or `None` for the first release.
    pub(crate) after: Option<i64>,
    /// When this release landed.
    pub(crate) until: i64,
}

/// Plans the step, or refuses before any write.
///
/// # Errors
///
/// When an unfinished issue has no later open milestone to move to, or a close
/// time does not parse.
pub(crate) fn plan(version: Version, reads: &Reads) -> Result<Vec<Change>, Error> {
    let title = format!("v{version}");
    let release = reads.milestones.iter().find(|m| m.title == title);
    let mut stamps = Vec::new();
    for t in &reads.closed {
        if t.state_reason.as_deref() != Some("completed")
            || release.is_some_and(|m| t.milestone == Some(m.number))
        {
            continue;
        }
        let Some(closed) = t.closed_at.as_deref() else {
            continue;
        };
        let closed = seconds(closed)?;
        if reads.after.is_none_or(|after| closed > after) && closed <= reads.until {
            stamps.push(Change::Stamp(t.issue.clone()));
        }
    }

    let next = reads
        .milestones
        .iter()
        .filter(|m| m.open)
        .filter_map(|m| Some((title_version(&m.title)?, m)))
        .filter(|(v, _)| *v > version)
        .min_by_key(|(v, _)| *v)
        .map(|(_, m)| Target::from(m));
    let mut moves = Vec::new();
    for (t, events) in &reads.open {
        let held = [SECURITY_LABEL, RELEASE_BLOCKER]
            .into_iter()
            .find(|l| t.labels.iter().any(|have| have == l));
        let slipped = slipped(events, version)?;
        if slipped && held.is_none() {
            let label = !t.labels.iter().any(|l| l.starts_with(PRIORITY));
            moves.push(Change::Demote {
                issue: t.issue.clone(),
                label,
            });
            continue;
        }
        let Some(to) = &next else {
            return Err(Error::msg(format!(
                "{title} has unfinished issues and no later open milestone to move them to; \
                 create the next release's milestone first"
            )));
        };
        moves.push(Change::Move {
            issue: t.issue.clone(),
            to: to.clone(),
            held: held.filter(|_| slipped),
        });
    }

    let mut changes = Vec::new();
    let stamping = !stamps.is_empty();
    let close = match release {
        None if stamping => {
            changes.push(Change::Create);
            true
        }
        None => false,
        Some(m) if !m.open && stamping => {
            changes.push(Change::Reopen);
            true
        }
        Some(m) => m.open,
    };
    changes.extend(stamps);
    changes.extend(moves);
    if close {
        changes.push(Change::Close);
    }
    Ok(changes)
}

/// Whether the events record a move off a milestone below `version` since
/// the issue last returned to the backlog. A move is a `demilestoned` event
/// whose next event is a `milestoned` within [`MOVE_GAP`]; a `demilestoned`
/// with none is a return to the backlog, and the count starts again after it.
fn slipped(events: &[MilestoneEvent], version: Version) -> Result<bool, Error> {
    let mut slipped = false;
    for (i, event) in events.iter().enumerate() {
        if event.added {
            continue;
        }
        let moved = match events.get(i + 1) {
            Some(next) if next.added => seconds(&next.at)? - seconds(&event.at)? <= MOVE_GAP,
            _ => false,
        };
        if !moved {
            slipped = false;
        } else if title_version(&event.title).is_some_and(|v| v < version) {
            slipped = true;
        }
    }
    Ok(slipped)
}

/// The version a milestone title `vX.Y.Z` names.
fn title_version(title: &str) -> Option<Version> {
    Version::parse(title.strip_prefix('v')?)
}

/// Seconds since the epoch of a UTC timestamp as GitHub writes it,
/// `YYYY-MM-DDTHH:MM:SSZ`.
fn seconds(at: &str) -> Result<i64, Error> {
    let bad = || Error::msg(format!("'{at}' is not a YYYY-MM-DDTHH:MM:SSZ timestamp"));
    let b = at.as_bytes();
    let shape = b.len() == 20
        && b[4] == b'-'
        && b[7] == b'-'
        && b[10] == b'T'
        && b[13] == b':'
        && b[16] == b':'
        && b[19] == b'Z';
    if !shape {
        return Err(bad());
    }
    let field = |from: usize, to: usize| at[from..to].parse::<i64>().map_err(|_| bad());
    let (year, month, day) = (field(0, 4)?, field(5, 7)?, field(8, 10)?);
    let (hour, minute, second) = (field(11, 13)?, field(14, 16)?, field(17, 19)?);
    Ok(days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second)
}

/// The days from 1970-01-01 to a civil date, by the era algorithm, with the
/// year shifted to start in March so the leap day falls at its end.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// The tag `vX.Y.Z` names on origin and the newest release tag below it, as
/// commits.
///
/// # Errors
///
/// When origin has no `vX.Y.Z` tag, which `finish` pushes.
fn tags(tags: &[(String, String)], version: Version) -> Result<(String, Option<String>), Error> {
    let release = tags
        .iter()
        .find(|(name, _)| title_version(name) == Some(version))
        .map(|(_, sha)| sha.clone())
        .ok_or_else(|| {
            Error::msg(format!(
                "origin has no v{version} tag; the milestone step runs after the release finishes"
            ))
        })?;
    let previous = tags
        .iter()
        .filter_map(|(name, sha)| Some((title_version(name)?, sha)))
        .filter(|(v, _)| *v < version)
        .max_by_key(|(v, _)| *v)
        .map(|(_, sha)| sha.clone());
    Ok((release, previous))
}

/// Reads what [`plan`] rests on: the window between the previous release and
/// this one, from the tags on origin, then the milestones and issues.
fn read(git: &dyn Git, forge: &dyn Forge, version: Version) -> Result<Reads, Error> {
    let (release, previous) = tags(&git.remote_tags()?, version)?;
    let until = seconds(&forge.landed_at(&release)?)?;
    let since = previous.map(|sha| forge.landed_at(&sha)).transpose()?;
    let after = since.as_deref().map(seconds).transpose()?;
    let milestones = forge.milestones()?;
    let closed = forge.closed_issues(since.as_deref())?;
    let title = format!("v{version}");
    let mut open = Vec::new();
    if let Some(m) = milestones.iter().find(|m| m.title == title) {
        for t in forge.open_issues(m.number)? {
            let events = forge.milestone_events(t.issue.number)?;
            open.push((t, events));
        }
    }
    Ok(Reads {
        milestones,
        closed,
        open,
        after,
        until,
    })
}

/// Plans the milestone step for `version` and prints each change. With
/// `apply`, each change is made in the plan's order and the plan is read
/// again, which must then be empty; without it, each write is printed instead.
///
/// # Errors
///
/// When a read or a write fails, the plan refuses, or a change is still
/// planned after the writes.
pub(crate) fn run(host: &Host<'_>, version: Version, apply: bool) -> Outcome {
    let dry = DryForge(host.forge, true);
    let forge: &dyn Forge = if apply { host.forge } else { &dry };
    let reads = read(host.git, forge, version)?;
    let changes = plan(version, &reads)?;
    let title = format!("v{version}");
    if changes.is_empty() {
        println!("milestone: {title} has nothing to change.");
        return Ok(());
    }
    let mut release = Target {
        title: title.clone(),
        number: reads
            .milestones
            .iter()
            .find(|m| m.title == title)
            .map(|m| m.number),
    };
    for change in &changes {
        println!("{}", change.describe(&title));
        match change {
            Change::Create => release.number = forge.create_milestone(&title)?,
            Change::Reopen => forge.set_milestone_open(&release, true)?,
            Change::Stamp(issue) => forge.set_milestone(issue.number, Some(&release))?,
            Change::Move { issue, to, .. } => forge.set_milestone(issue.number, Some(to))?,
            Change::Demote { issue, label } => {
                // Labeled first: once the milestone is cleared, a re-run no
                // longer sees the issue.
                if *label {
                    forge.add_label(issue.number, DEMOTED)?;
                }
                forge.set_milestone(issue.number, None)?;
            }
            Change::Close => forge.set_milestone_open(&release, false)?,
        }
    }
    if !apply {
        println!("milestone: dry run; pass --apply to make these changes.");
        return Ok(());
    }
    // GitHub drops a milestone change it does not permit without an error.
    let left = plan(version, &read(host.git, forge, version)?)?;
    if !left.is_empty() {
        let lines: Vec<String> = left.iter().map(|c| c.describe(&title)).collect();
        return Err(Error::msg(format!(
            "these changes did not take; does the token have push access?\n  {}",
            lines.join("\n  ")
        )));
    }
    println!("milestone: {title} lists what it shipped.");
    Ok(())
}

#[cfg(test)]
mod tests;
