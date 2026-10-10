//! The lost-reply evidence check: after each `err_after_land` line, the same
//! process's journal shows that it recovered the landed write, or that the
//! split or the leader's work left the process.

use std::collections::HashMap;

use crate::classify::{WriteKind, classify};
use crate::journal::{Event, Progress, Reply, Source, WriteOp};
use crate::oracle::ProcessJournal;
use crate::outcome::LostReplies;

/// Judges every `err_after_land` line in `journals`; `drawn` says the
/// schedule drew an `ErrAfterLand` plan.
///
/// After a lost claim reply the process reads the landed claim back, or a
/// read of the key fails and it claims the split again. A claim sent again
/// from the replaced revision, or from the landed one at a higher epoch, with
/// no failed read of the key before it, is not a recovery. After a lost
/// commit or completion reply, its first later write on the key that wins or
/// loses keeps the landed epoch, and either loses with the landed value read
/// back, or wins from the landed revision after the landed value was read. A
/// write in between with no `won` or `lost` reply may have landed, so a read
/// of its value at a higher revision counts as the landed value at that
/// revision. A process that writes nothing more on the key that wins or loses
/// has let the split go.
///
/// Writes to the leader key, `plan` and `assign.*` are matched by digest.
/// After a lost election reply, a `get` reads the landed key back. After any
/// other, the next write on the key that wins or loses either loses with the
/// landed value read back, or wins from the landed revision after the landed
/// value was read. With no such write, a read of the landed value at its
/// revision recovers a `plan` or `assign.*` write; a `plan` write without one
/// is unrecovered, and any other process has stopped writing the key.
#[must_use]
pub fn lost_replies(journals: &[ProcessJournal], drawn: bool) -> LostReplies {
    let mut out = LostReplies {
        drawn,
        ..LostReplies::default()
    };
    for journal in journals {
        for (at, line) in journal.lines.iter().enumerate() {
            if let Event::ErrAfterLand { key, rev } = &line.event {
                out.lines += 1;
                if let Err(why) = recovered(journal, at, key, *rev) {
                    out.unexplained.push(format!(
                        "{} (pid {}) {key} at rev {rev}: {why}",
                        journal.instance, journal.pid
                    ));
                }
            }
        }
    }
    out
}

/// Whether `journal` holds an `err_after_land` line and shows the landed
/// write recovered after each one.
#[must_use]
pub fn recovery_shown(journal: &ProcessJournal) -> bool {
    let mut lost = journal
        .lines
        .iter()
        .enumerate()
        .filter_map(|(at, line)| match &line.event {
            Event::ErrAfterLand { key, rev } => Some((at, key, *rev)),
            _ => None,
        })
        .peekable();
    lost.peek().is_some() && lost.all(|(at, key, rev)| recovered(journal, at, key, rev) == Ok(true))
}

/// A `send` line on one key, with its reply when the journal holds one.
struct Write<'a> {
    index: usize,
    expected: Option<u64>,
    value: &'a Progress,
    reply: Option<&'a Reply>,
}

/// `Ok(true)` when the journal shows the landed write recovered, `Ok(false)`
/// when the split left the process.
fn recovered(journal: &ProcessJournal, at: usize, key: &str, rev: u64) -> Result<bool, String> {
    if !key.starts_with("split.") {
        return leader_recovered(journal, at, key, rev);
    }
    let lines = &journal.lines;
    let landed_done = lines[..at]
        .iter()
        .rposition(|l| {
            matches!(&l.event, Event::Done { key: k, reply: Reply::Won(r), .. } if k == key && *r == rev)
        })
        .ok_or("no write landed at that revision before it")?;
    let writes = writes_on(journal, key);
    let Event::Done { call, .. } = &lines[landed_done].event else {
        unreachable!("matched a done line")
    };
    let landed = writes
        .iter()
        .find(|w| w.index < landed_done && send_call(journal, w.index) == Some(*call))
        .ok_or("no send for the landed write")?;
    let kind = value_before(journal, landed.index, key, landed.expected)
        .and_then(|prev| classify(prev, landed.value, &journal.instance));
    let seen_landed = |from: usize, to: usize| {
        lines[from..to].iter().any(|l| {
            matches!(&l.event, Event::Seen { key: k, rev: r, value, .. }
                if k == key && *r == rev && value == landed.value)
        })
    };
    let later: Vec<&Write<'_>> = writes.iter().filter(|w| w.index > at).collect();

    if kind == Some(WriteKind::Claim) {
        let read_failed = |to: usize| {
            lines[landed_done..to]
                .iter()
                .any(|l| matches!(&l.event, Event::ReadFailed { key: k } if k == key))
        };
        let reclaim = |w: &Write<'_>| {
            w.expected == landed.expected
                || (w.expected == Some(rev) && w.value.epoch > landed.value.epoch)
        };
        if later.iter().any(|w| reclaim(w) && !read_failed(w.index)) {
            return Err("the split was claimed again with no failed read of it before".to_owned());
        }
        if seen_landed(landed_done, lines.len()) {
            return Ok(true);
        }
    } else if let Some(next) = later
        .iter()
        .find(|w| matches!(w.reply, Some(Reply::Won(_) | Reply::Lost)))
    {
        // Values sent by earlier writes with no `won` or `lost` reply, any of
        // which may have landed.
        let unreplied: Vec<&Progress> = later
            .iter()
            .filter(|w| w.index < next.index)
            .map(|w| w.value)
            .collect();
        let adopted = |from: usize, to: usize, at: Option<u64>| {
            lines[from..to].iter().any(|l| {
                matches!(&l.event, Event::Seen { key: k, rev: r, value, .. }
                    if k == key
                        && at.is_none_or(|a| a == *r)
                        && ((*r == rev && value == landed.value)
                            || (*r > rev && unreplied.contains(&value))))
            })
        };
        match next.reply {
            Some(Reply::Lost)
                if next.value.epoch == landed.value.epoch
                    && adopted(next.index, lines.len(), None) =>
            {
                return Ok(true);
            }
            Some(Reply::Won(_))
                if next.expected.is_some()
                    && next.value.epoch == landed.value.epoch
                    && adopted(landed_done, next.index, next.expected) =>
            {
                return Ok(true);
            }
            _ => {}
        }
    }
    if later
        .iter()
        .all(|w| !matches!(w.reply, Some(Reply::Won(_) | Reply::Lost)))
    {
        return Ok(false);
    }
    Err("no recovery of the landed write follows it".to_owned())
}

/// A `leader_send` line on one key, with its reply when the journal holds
/// one.
struct LeaderWrite<'a> {
    index: usize,
    call: u64,
    op: WriteOp,
    expected: Option<u64>,
    digest: u64,
    reply: Option<&'a Reply>,
}

/// [`recovered`] for a write to the leader key, `plan` or an `assign.*` key:
/// `Ok(false)` when the process stopped writing the key.
fn leader_recovered(
    journal: &ProcessJournal,
    at: usize,
    key: &str,
    rev: u64,
) -> Result<bool, String> {
    let lines = &journal.lines;
    let landed_done = lines[..at]
        .iter()
        .rposition(|l| {
            matches!(&l.event, Event::Done { key: k, reply: Reply::Won(r), .. } if k == key && *r == rev)
        })
        .ok_or("no write landed at that revision before it")?;
    let Event::Done { call, .. } = &lines[landed_done].event else {
        unreachable!("matched a done line")
    };
    let writes = leader_writes_on(journal, key);
    let landed = writes
        .iter()
        .find(|w| w.index < landed_done && w.call == *call)
        .ok_or("no send for the landed write")?;
    let seen_landed = |from: Option<Source>| {
        lines[landed_done..].iter().any(|l| {
            matches!(&l.event, Event::LeaderSeen { key: k, rev: r, digest, from: f }
                if k == key && *r == rev && *digest == landed.digest
                    && from.is_none_or(|from| from == *f))
        })
    };
    if key == "leader" && landed.op == WriteOp::Create {
        return if seen_landed(Some(Source::Get)) {
            Ok(true)
        } else {
            Err("no read-back of the landed leader key follows it".to_owned())
        };
    }
    let later: Vec<&LeaderWrite<'_>> = writes.iter().filter(|w| w.index > at).collect();
    let Some(next) = later
        .iter()
        .find(|w| matches!(w.reply, Some(Reply::Won(_) | Reply::Lost)))
    else {
        if key != "leader" && seen_landed(None) {
            return Ok(true);
        }
        if key == "plan" {
            return Err("no read of the landed plan follows it".to_owned());
        }
        return Ok(false);
    };
    // Digests sent by earlier writes with no `won` or `lost` reply, any of
    // which may have landed.
    let unreplied: Vec<u64> = later
        .iter()
        .filter(|w| w.index < next.index)
        .map(|w| w.digest)
        .collect();
    let adopted = |from: usize, to: usize, at: Option<u64>| {
        lines[from..to].iter().any(|l| {
            matches!(&l.event, Event::LeaderSeen { key: k, rev: r, digest, .. }
                if k == key
                    && at.is_none_or(|a| a == *r)
                    && ((*r == rev && *digest == landed.digest)
                        || (*r > rev && unreplied.contains(digest))))
        })
    };
    match next.reply {
        Some(Reply::Lost) if adopted(next.index, lines.len(), None) => Ok(true),
        Some(Reply::Won(_))
            if next.expected.is_some() && adopted(landed_done, next.index, next.expected) =>
        {
            Ok(true)
        }
        _ => Err("no recovery of the landed write follows it".to_owned()),
    }
}

/// Every `leader_send` on `key`, in journal order, each with its reply.
fn leader_writes_on<'a>(journal: &'a ProcessJournal, key: &str) -> Vec<LeaderWrite<'a>> {
    let replies: HashMap<u64, &Reply> = journal
        .lines
        .iter()
        .filter_map(|l| match &l.event {
            Event::Done { call, reply, .. } => Some((*call, reply)),
            _ => None,
        })
        .collect();
    journal
        .lines
        .iter()
        .enumerate()
        .filter_map(|(index, l)| match &l.event {
            Event::LeaderSend {
                call,
                op,
                key: k,
                expected,
                digest,
            } if k == key => Some(LeaderWrite {
                index,
                call: *call,
                op: *op,
                expected: *expected,
                digest: *digest,
                reply: replies.get(call).copied(),
            }),
            _ => None,
        })
        .collect()
}

fn send_call(journal: &ProcessJournal, index: usize) -> Option<u64> {
    match &journal.lines[index].event {
        Event::Send { call, .. } => Some(*call),
        _ => None,
    }
}

/// Every `send` on `key`, in journal order, each with its reply.
fn writes_on<'a>(journal: &'a ProcessJournal, key: &str) -> Vec<Write<'a>> {
    let replies: HashMap<u64, &Reply> = journal
        .lines
        .iter()
        .filter_map(|l| match &l.event {
            Event::Done { call, reply, .. } => Some((*call, reply)),
            _ => None,
        })
        .collect();
    journal
        .lines
        .iter()
        .enumerate()
        .filter_map(|(index, l)| match &l.event {
            Event::Send {
                call,
                key: k,
                expected,
                value,
                ..
            } if k == key => Some(Write {
                index,
                expected: *expected,
                value,
                reply: replies.get(call).copied(),
            }),
            _ => None,
        })
        .collect()
}

/// The value this process last learned at `(key, expected)` before line
/// `before`, from a read or from its own landed write.
fn value_before<'a>(
    journal: &'a ProcessJournal,
    before: usize,
    key: &str,
    expected: Option<u64>,
) -> Option<&'a Progress> {
    let expected = expected?;
    let lines = &journal.lines[..before];
    let mut sent: HashMap<u64, &Progress> = HashMap::new();
    let mut value = None;
    for line in lines {
        match &line.event {
            Event::Seen {
                key: k,
                rev,
                value: v,
                ..
            } if k == key && *rev == expected => value = Some(v),
            Event::Send {
                call,
                key: k,
                value: v,
                ..
            } if k == key => {
                sent.insert(*call, v);
            }
            Event::Done {
                call,
                key: k,
                reply: Reply::Won(rev),
            } if k == key && *rev == expected => {
                if let Some(v) = sent.get(call) {
                    value = Some(*v);
                }
            }
            _ => {}
        }
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{Line, SCHEMA, Status};

    const KEY: &str = "split.a";

    fn value(epoch: u64, owner: Option<&str>, watermark: Option<i64>) -> Progress {
        Progress {
            schema: SCHEMA,
            epoch,
            owner: owner.map(str::to_owned),
            watermark,
            completed: false,
            status: Status::Runnable,
            attempts: 0,
        }
    }

    fn seen(rev: u64, value: Progress, from: Source) -> Event {
        Event::Seen {
            key: KEY.to_owned(),
            rev,
            value,
            from,
        }
    }

    fn send(call: u64, expected: u64, value: Progress) -> Event {
        Event::Send {
            call,
            op: WriteOp::Update,
            key: KEY.to_owned(),
            expected: Some(expected),
            value,
        }
    }

    fn done(call: u64, reply: Reply) -> Event {
        Event::Done {
            call,
            key: KEY.to_owned(),
            reply,
        }
    }

    fn lost_reply(rev: u64) -> Event {
        Event::ErrAfterLand {
            key: KEY.to_owned(),
            rev,
        }
    }

    fn read_failed() -> Event {
        Event::ReadFailed {
            key: KEY.to_owned(),
        }
    }

    fn journal(events: Vec<Event>) -> ProcessJournal {
        let lines = events
            .into_iter()
            .zip(1..)
            .map(|(event, t_ms)| Line { t_ms, event })
            .collect();
        ProcessJournal {
            instance: "w0".to_owned(),
            pid: 7,
            lines,
        }
    }

    fn judge(events: Vec<Event>) -> LostReplies {
        lost_replies(&[journal(events)], true)
    }

    /// The claim at rev 6 over the unowned value at rev 5, its `won` reply
    /// and the `err_after_land` line.
    fn claim_lost() -> Vec<Event> {
        vec![
            seen(5, value(1, None, None), Source::Watch),
            send(1, 5, value(2, Some("w0"), None)),
            done(1, Reply::Won(6)),
            lost_reply(6),
        ]
    }

    /// The commit at rev 7 over the claim at rev 6, its `won` reply and the
    /// `err_after_land` line.
    fn commit_lost() -> Vec<Event> {
        vec![
            seen(6, value(2, Some("w0"), None), Source::Get),
            send(1, 6, value(2, Some("w0"), Some(10))),
            done(1, Reply::Won(7)),
            lost_reply(7),
        ]
    }

    /// A lost claim reply followed by the adopting `get` of the landed claim
    /// and a commit from its revision is recovered.
    #[test]
    fn claim_evidence_is_the_adopting_get() {
        let mut events = claim_lost();
        events.extend([
            seen(6, value(2, Some("w0"), None), Source::Get),
            send(2, 6, value(2, Some("w0"), Some(10))),
            done(2, Reply::Won(7)),
        ]);
        let judged = judge(events);
        assert_eq!(
            (judged.lines, judged.unexplained),
            (1, Vec::<String>::new())
        );
    }

    /// A second claim from the revision the lost claim replaced is not a
    /// recovery, even after the landed claim was read.
    #[test]
    fn claim_evidence_rejects_a_second_claim_send() {
        let mut events = claim_lost();
        events.extend([
            seen(6, value(2, Some("w0"), None), Source::Get),
            send(2, 5, value(2, Some("w0"), None)),
            done(2, Reply::Lost),
        ]);
        assert_eq!(judge(events).unexplained.len(), 1);
    }

    /// A lost claim reply whose read-back failed: the process claims again
    /// from its view and loses, reads the landed claim, then claims from it.
    #[test]
    fn claim_evidence_accepts_a_reclaim_after_a_failed_readback() {
        let mut events = claim_lost();
        events.extend([
            read_failed(),
            send(2, 5, value(2, Some("w0"), None)),
            done(2, Reply::Lost),
            seen(6, value(2, Some("w0"), None), Source::Get),
            send(3, 6, value(3, Some("w0"), None)),
            done(3, Reply::Won(7)),
        ]);
        assert_eq!(judge(events).unexplained, Vec::<String>::new());
    }

    /// A claim from the landed claim's revision at a higher epoch, after the
    /// watch delivered the landed claim and with no failed read, is not a
    /// recovery.
    #[test]
    fn claim_evidence_rejects_a_reclaim_from_the_landed_claim_without_a_failed_read() {
        let mut events = claim_lost();
        events.extend([
            seen(6, value(2, Some("w0"), None), Source::Watch),
            send(2, 6, value(3, Some("w0"), None)),
            done(2, Reply::Won(7)),
        ]);
        assert_eq!(judge(events).unexplained.len(), 1);
    }

    /// The watch echo of the landed commit, then a commit from the landed
    /// revision that wins, is a recovery.
    #[test]
    fn commit_evidence_accepts_a_folded_echo_then_a_send_at_rev() {
        let mut events = commit_lost();
        events.extend([
            seen(7, value(2, Some("w0"), Some(10)), Source::Watch),
            send(2, 7, value(2, Some("w0"), Some(20))),
            done(2, Reply::Won(8)),
        ]);
        assert!(judge(events).unexplained.is_empty());
    }

    /// A commit from the pre-fault revision that loses, then a read of the
    /// landed commit, is a recovery.
    #[test]
    fn commit_evidence_accepts_lost_then_adoption() {
        let mut events = commit_lost();
        events.extend([
            send(2, 6, value(2, Some("w0"), Some(20))),
            done(2, Reply::Lost),
            seen(7, value(2, Some("w0"), Some(10)), Source::Get),
        ]);
        assert!(judge(events).unexplained.is_empty());
    }

    /// A commit retry that fails retryable, then loses, then wins from the
    /// landed commit after reading it, is a recovery.
    #[test]
    fn commit_evidence_accepts_a_retryable_retry_then_adoption() {
        let mut events = commit_lost();
        events.extend([
            send(2, 6, value(2, Some("w0"), Some(20))),
            done(2, Reply::Err("retryable".to_owned())),
            send(3, 6, value(2, Some("w0"), Some(20))),
            done(3, Reply::Lost),
            seen(7, value(2, Some("w0"), Some(10)), Source::Get),
            send(4, 7, value(2, Some("w0"), Some(20))),
            done(4, Reply::Won(8)),
        ]);
        assert_eq!(judge(events).unexplained, Vec::<String>::new());
    }

    /// A lost commit reply answered by dropping the tenancy and claiming the
    /// split again is not a recovery of the landed commit.
    #[test]
    fn commit_evidence_rejects_a_reclaim_after_the_lost_reply() {
        let mut events = commit_lost();
        events.extend([
            send(2, 6, value(3, Some("w0"), None)),
            done(2, Reply::Lost),
            seen(7, value(2, Some("w0"), Some(10)), Source::Get),
            send(3, 7, value(3, Some("w0"), Some(10))),
            done(3, Reply::Won(8)),
        ]);
        assert_eq!(judge(events).unexplained.len(), 1);
    }

    /// A claim from the landed commit's revision at a higher epoch that wins,
    /// after a read of the landed commit, is not a recovery of it.
    #[test]
    fn commit_evidence_rejects_a_reclaim_from_the_landed_commit() {
        let mut events = commit_lost();
        events.extend([
            seen(7, value(2, Some("w0"), Some(10)), Source::Watch),
            send(2, 7, value(3, Some("w0"), Some(10))),
            done(2, Reply::Won(8)),
        ]);
        assert_eq!(judge(events).unexplained.len(), 1);
    }

    /// A later commit from the pre-fault revision that wins is not a
    /// recovery, though it follows a read of the landed commit.
    #[test]
    fn commit_evidence_rejects_a_send_at_the_pre_fault_revision_that_wins() {
        let mut events = commit_lost();
        events.extend([
            seen(7, value(2, Some("w0"), Some(10)), Source::Watch),
            send(2, 6, value(2, Some("w0"), Some(20))),
            done(2, Reply::Won(8)),
        ]);
        assert_eq!(judge(events).unexplained.len(), 1);
    }

    /// A process that writes nothing more on the key that wins or loses, as
    /// one killed with its next write in flight, has let the split go.
    #[test]
    fn evidence_accepts_a_split_that_left_the_process() {
        assert!(judge(commit_lost()).unexplained.is_empty());
        let mut in_flight = commit_lost();
        in_flight.push(send(2, 7, value(2, Some("w0"), Some(20))));
        assert!(judge(in_flight).unexplained.is_empty());
        assert!(judge(claim_lost()).unexplained.is_empty());
    }

    /// A commit from the landed revision that loses, with no read of the
    /// landed value anywhere, shows no recovery; nor does a journal whose
    /// `err_after_land` follows no landed write.
    #[test]
    fn evidence_rejects_a_journal_with_no_path() {
        let mut events = commit_lost();
        events.extend([
            send(2, 7, value(2, Some("w0"), Some(20))),
            done(2, Reply::Lost),
        ]);
        assert_eq!(judge(events).unexplained.len(), 1);
        assert_eq!(judge(vec![lost_reply(7)]).unexplained.len(), 1);
    }

    /// A lost commit reply counts as recovered only once the losing retry's
    /// read-back of the landed commit is journalled.
    #[test]
    fn recovery_is_shown_only_after_the_read_back() {
        let mut events = commit_lost();
        assert!(!recovery_shown(&journal(events.clone())));
        events.extend([
            send(2, 6, value(2, Some("w0"), Some(20))),
            done(2, Reply::Lost),
        ]);
        assert!(!recovery_shown(&journal(events.clone())));
        events.push(seen(7, value(2, Some("w0"), Some(10)), Source::Get));
        assert!(recovery_shown(&journal(events)));
        assert!(!recovery_shown(&journal(Vec::new())));
    }

    /// A retry from the landed revision that landed with its reply cancelled
    /// at `op_timeout`, then a commit from the retry's revision, is a recovery.
    #[test]
    fn commit_evidence_accepts_a_cancelled_landed_retry_then_a_win() {
        let mut events = commit_lost();
        events.extend([
            seen(7, value(2, Some("w0"), Some(10)), Source::Watch),
            send(2, 7, value(2, Some("w0"), Some(20))),
            done(2, Reply::Cancelled),
            seen(8, value(2, Some("w0"), Some(20)), Source::Watch),
            send(3, 8, value(2, Some("w0"), Some(30))),
            done(3, Reply::Won(9)),
        ]);
        assert_eq!(judge(events).unexplained, Vec::<String>::new());
    }

    /// A retry from the landed revision that landed with its reply cancelled,
    /// then a retry that loses and reads the cancelled retry's value back, is
    /// a recovery.
    #[test]
    fn commit_evidence_accepts_a_cancelled_landed_retry_then_lost_read_back() {
        let mut events = commit_lost();
        events.extend([
            seen(7, value(2, Some("w0"), Some(10)), Source::Watch),
            send(2, 7, value(2, Some("w0"), Some(20))),
            done(2, Reply::Cancelled),
            send(3, 7, value(2, Some("w0"), Some(20))),
            done(3, Reply::Lost),
            seen(8, value(2, Some("w0"), Some(20)), Source::Get),
        ]);
        assert_eq!(judge(events).unexplained, Vec::<String>::new());
    }

    /// A retry from the landed revision that landed with its reply answered
    /// `retryable`, then a retry that loses and reads the landed retry's value
    /// back, is a recovery.
    #[test]
    fn commit_evidence_accepts_a_retryable_landed_retry_then_lost_read_back() {
        let mut events = commit_lost();
        events.extend([
            seen(7, value(2, Some("w0"), Some(10)), Source::Watch),
            send(2, 7, value(2, Some("w0"), Some(20))),
            done(2, Reply::Err("retryable".to_owned())),
            send(3, 7, value(2, Some("w0"), Some(20))),
            done(3, Reply::Lost),
            seen(8, value(2, Some("w0"), Some(20)), Source::Get),
        ]);
        assert_eq!(judge(events).unexplained, Vec::<String>::new());
    }

    fn leader_send(call: u64, key: &str, expected: Option<u64>, digest: u64) -> Event {
        Event::LeaderSend {
            call,
            op: if expected.is_some() {
                WriteOp::Update
            } else {
                WriteOp::Create
            },
            key: key.to_owned(),
            expected,
            digest,
        }
    }

    fn leader_done(call: u64, key: &str, reply: Reply) -> Event {
        Event::Done {
            call,
            key: key.to_owned(),
            reply,
        }
    }

    fn leader_seen(key: &str, rev: u64, digest: u64, from: Source) -> Event {
        Event::LeaderSeen {
            key: key.to_owned(),
            rev,
            digest,
            from,
        }
    }

    /// A write of digest 10 on `key` from `expected` that landed at rev 5,
    /// and its `err_after_land` line.
    fn leader_lost(key: &str, expected: Option<u64>) -> Vec<Event> {
        vec![
            leader_send(1, key, expected, 10),
            leader_done(1, key, Reply::Won(5)),
            Event::ErrAfterLand {
                key: key.to_owned(),
                rev: 5,
            },
        ]
    }

    /// A lost election reply is recovered only by a `get` that reads the
    /// landed key back.
    #[test]
    fn elect_evidence_needs_the_read_back() {
        let lost = leader_lost("leader", None);
        assert_eq!(judge(lost.clone()).unexplained.len(), 1, "silence");
        let mut watched = lost.clone();
        watched.push(leader_seen("leader", 5, 10, Source::Watch));
        assert_eq!(
            judge(watched).unexplained.len(),
            1,
            "a watch is no read-back"
        );
        let mut read = lost;
        read.push(leader_seen("leader", 5, 10, Source::Get));
        assert!(recovery_shown(&journal(read.clone())));
        assert_eq!(judge(read).unexplained, Vec::<String>::new());
    }

    /// A lost publish reply followed only by the watch echo of the landed
    /// plan is recovered; with no echo it is not.
    #[test]
    fn plan_evidence_accepts_the_echo() {
        let lost = leader_lost("plan", Some(4));
        assert_eq!(judge(lost.clone()).unexplained.len(), 1);
        let mut echoed = lost;
        echoed.push(leader_seen("plan", 5, 10, Source::Watch));
        assert!(recovery_shown(&journal(echoed.clone())));
        assert_eq!(judge(echoed).unexplained, Vec::<String>::new());
    }

    /// A lost assignment reply, a retry from the replaced revision that
    /// loses, and a read of the landed assignment is a recovery, as is a
    /// read of it before a write from the landed revision that wins.
    #[test]
    fn assign_evidence_accepts_lost_then_seen() {
        let mut lost_then_seen = leader_lost("assign.w0", Some(4));
        lost_then_seen.extend([
            leader_send(2, "assign.w0", Some(4), 10),
            leader_done(2, "assign.w0", Reply::Lost),
            leader_seen("assign.w0", 5, 10, Source::Get),
        ]);
        assert!(recovery_shown(&journal(lost_then_seen.clone())));
        assert_eq!(judge(lost_then_seen).unexplained, Vec::<String>::new());

        let mut seen_then_won = leader_lost("assign.w0", Some(4));
        seen_then_won.extend([
            leader_seen("assign.w0", 5, 10, Source::Watch),
            leader_send(2, "assign.w0", Some(5), 11),
            leader_done(2, "assign.w0", Reply::Won(6)),
        ]);
        assert_eq!(judge(seen_then_won).unexplained, Vec::<String>::new());
    }

    /// A lost assignment reply is recovered once the watch echo of the landed
    /// assignment is journalled.
    #[test]
    fn assign_evidence_accepts_the_echo() {
        let mut events = leader_lost("assign.w0", None);
        assert!(!recovery_shown(&journal(events.clone())));
        events.push(leader_seen("assign.w0", 5, 10, Source::Watch));
        assert!(recovery_shown(&journal(events)));
    }

    /// A retry that loses with no read of the landed value, a retry from the
    /// replaced revision that wins, and a read of another digest are no
    /// recovery; a renewal with no later write is let go.
    #[test]
    fn leader_evidence_rejects_a_journal_with_no_path() {
        for key in ["plan", "assign.w0", "leader"] {
            let mut lost = leader_lost(key, Some(4));
            lost.extend([
                leader_send(2, key, Some(4), 10),
                leader_done(2, key, Reply::Lost),
                leader_seen(key, 5, 99, Source::Get),
            ]);
            assert_eq!(
                judge(lost).unexplained.len(),
                1,
                "{key}: lost, other digest"
            );

            let mut won = leader_lost(key, Some(4));
            won.extend([
                leader_seen(key, 5, 10, Source::Watch),
                leader_send(2, key, Some(4), 11),
                leader_done(2, key, Reply::Won(6)),
            ]);
            assert_eq!(
                judge(won).unexplained.len(),
                1,
                "{key}: won from the replaced revision"
            );
        }
        let renewed = leader_lost("leader", Some(4));
        assert_eq!(judge(renewed.clone()).unexplained, Vec::<String>::new());
        assert!(!recovery_shown(&journal(renewed)));
    }

    /// An `assign.*` write whose reply was lost, with no later write on the
    /// key and no read of the landed value, has let the assignment go.
    #[test]
    fn assign_with_no_later_write_is_let_go() {
        for expected in [None, Some(4)] {
            let lost = leader_lost("assign.w0", expected);
            assert_eq!(
                judge(lost).unexplained,
                Vec::<String>::new(),
                "{expected:?}"
            );
        }
    }

    /// A write from the landed revision whose reply was dropped, then a write
    /// that loses and a read of the dropped write's value at a higher
    /// revision, recovers the landed write.
    #[test]
    fn leader_evidence_accepts_an_unreplied_retry_read_back() {
        let mut events = leader_lost("assign.w0", Some(4));
        events.extend([
            leader_seen("assign.w0", 5, 10, Source::Watch),
            leader_send(2, "assign.w0", Some(5), 11),
            leader_done(2, "assign.w0", Reply::Cancelled),
            leader_send(3, "assign.w0", Some(5), 12),
            leader_done(3, "assign.w0", Reply::Lost),
            leader_seen("assign.w0", 6, 11, Source::Get),
        ]);
        assert!(recovery_shown(&journal(events)));
    }

    /// A read of the landed value before a retry that loses is no recovery:
    /// the read must follow the losing write.
    #[test]
    fn leader_evidence_needs_the_read_after_the_lost_write() {
        let mut events = leader_lost("assign.w0", Some(4));
        events.extend([
            leader_seen("assign.w0", 5, 10, Source::Watch),
            leader_send(2, "assign.w0", Some(4), 10),
            leader_done(2, "assign.w0", Reply::Lost),
        ]);
        assert_eq!(judge(events).unexplained.len(), 1);
    }

    /// A read of a later write's digest whose own reply was `lost` is no
    /// recovery.
    #[test]
    fn leader_evidence_rejects_the_losing_writes_digest() {
        let mut events = leader_lost("assign.w0", Some(4));
        events.extend([
            leader_send(2, "assign.w0", Some(4), 12),
            leader_done(2, "assign.w0", Reply::Lost),
            leader_seen("assign.w0", 6, 12, Source::Get),
        ]);
        assert_eq!(judge(events).unexplained.len(), 1);
    }

    /// A create that wins after the landed value was read is no write from
    /// the landed revision.
    #[test]
    fn leader_evidence_rejects_a_create_after_the_read() {
        let mut events = leader_lost("assign.w0", Some(4));
        events.extend([
            leader_seen("assign.w0", 5, 10, Source::Watch),
            leader_send(2, "assign.w0", None, 11),
            leader_done(2, "assign.w0", Reply::Won(6)),
        ]);
        assert_eq!(judge(events).unexplained.len(), 1);
    }

    fn completed(epoch: u64, watermark: i64) -> Progress {
        let mut v = value(epoch, Some("w0"), Some(watermark));
        v.completed = true;
        v.status = Status::Completed;
        v
    }

    /// A lost completion reply, a retry from the pre-fault revision that
    /// loses, and a read of the landed completion is a recovery.
    #[test]
    fn completion_evidence_accepts_lost_then_adoption() {
        let events = vec![
            seen(6, value(2, Some("w0"), None), Source::Get),
            send(1, 6, completed(2, 10)),
            done(1, Reply::Won(7)),
            lost_reply(7),
            send(2, 6, completed(2, 10)),
            done(2, Reply::Lost),
            seen(7, completed(2, 10), Source::Get),
        ];
        assert_eq!(judge(events).unexplained, Vec::<String>::new());
    }
}
