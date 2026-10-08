//! The lost-reply evidence check: after each `err_after_land` line, the same
//! process's journal shows that it recovered the landed write, or that the
//! split left the process.

use std::collections::HashMap;

use crate::classify::{WriteKind, classify};
use crate::journal::{Event, Progress, Reply};
use crate::oracle::ProcessJournal;
use crate::outcome::LostReplies;

/// Judges every `err_after_land` line in `journals`; `drawn` says the
/// schedule drew an `ErrAfterLand` plan.
///
/// After a lost claim reply the process reads the landed claim back and sends
/// no second claim from the revision the first one replaced. After a lost
/// commit or completion reply its next write on the key either loses and the
/// landed value is read back, or is sent from the landed revision after the
/// landed value was read, and wins. Either way, a process that writes nothing
/// more on the key that wins or loses has let the split go.
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

/// A `send` line on one key, with its reply when the journal holds one.
struct Write<'a> {
    index: usize,
    expected: Option<u64>,
    value: &'a Progress,
    reply: Option<&'a Reply>,
}

fn recovered(journal: &ProcessJournal, at: usize, key: &str, rev: u64) -> Result<(), String> {
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
        if later.iter().any(|w| w.expected == landed.expected) {
            return Err("a second claim was sent from the revision the first replaced".to_owned());
        }
        if seen_landed(landed_done, lines.len()) {
            return Ok(());
        }
    } else if let Some(next) = later.first() {
        match next.reply {
            Some(Reply::Lost) if seen_landed(next.index, lines.len()) => return Ok(()),
            Some(Reply::Won(_))
                if next.expected == Some(rev) && seen_landed(landed_done, next.index) =>
            {
                return Ok(());
            }
            _ => {}
        }
    }
    if later
        .iter()
        .all(|w| !matches!(w.reply, Some(Reply::Won(_) | Reply::Lost)))
    {
        return Ok(());
    }
    Err("no recovery of the landed write follows it".to_owned())
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
    use crate::journal::{Line, SCHEMA, Source, Status, WriteOp};

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

    fn judge(events: Vec<Event>) -> LostReplies {
        let lines = events
            .into_iter()
            .zip(1..)
            .map(|(event, t_ms)| Line { t_ms, event })
            .collect();
        let journal = ProcessJournal {
            instance: "w0".to_owned(),
            pid: 7,
            lines,
        };
        lost_replies(&[journal], true)
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
}
