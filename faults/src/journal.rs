//! The append-only NDJSON journal every worker process and the harness write.
//!
//! A worker journals the rows its sink made durable, each durable `split.*`
//! write it sent with its reply, and each durable `split.*` entry it read. The
//! harness journals the faults it injected, in the same line format.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Write as _};
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::classify::WriteKind;

/// The progress-record schema this journal parses. A record at any other
/// schema fails the read.
pub const SCHEMA: u32 = 3;

/// The fields of a durable `split.*` progress record that the oracle reads.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    /// The record's schema.
    pub schema: u32,
    /// Fencing epoch.
    pub epoch: u64,
    /// Instance id of the current or last tenancy.
    pub owner: Option<String>,
    /// Committed watermark.
    pub watermark: Option<i64>,
    /// Terminal flag of the committed progress.
    pub completed: bool,
    /// Lifecycle status.
    pub status: Status,
    /// Delivery attempts consumed.
    pub attempts: u32,
}

impl Progress {
    /// Parses the stored bytes of a progress record, ignoring fields the
    /// oracle does not read.
    ///
    /// # Errors
    ///
    /// Fails when the bytes are not a progress record or carry a schema other
    /// than [`SCHEMA`].
    pub fn parse(bytes: &[u8]) -> Result<Progress, JournalError> {
        let progress: Progress =
            serde_json::from_slice(bytes).map_err(|e| JournalError::Record(e.to_string()))?;
        if progress.schema != SCHEMA {
            return Err(JournalError::Schema(progress.schema));
        }
        Ok(progress)
    }
}

/// A split's lifecycle on its progress record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Claimable.
    Runnable,
    /// Fully delivered and committed.
    Completed,
    /// Out of delivery attempts.
    Quarantined,
}

/// The store call a `send` line records.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteOp {
    /// Create-if-absent.
    Create,
    /// Compare-and-set against `expected`.
    Update,
}

/// The reply a `done` line records.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reply {
    /// The write landed at this revision.
    Won(u64),
    /// The precondition failed.
    Lost,
    /// The store returned an error of this class.
    Err(String),
    /// The caller dropped the call before it returned.
    Cancelled,
}

/// Where a `seen` line's entry came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    /// A `get`.
    Get,
    /// A `list`.
    List,
    /// A watch `Put`.
    Watch,
}

/// Whether an injected abort fires before the write is sent or after its
/// `Won` reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AbortPoint {
    /// Before the write is sent.
    Before,
    /// After a `Won` reply was journalled.
    After,
}

/// What the harness read from the leader key before a kill. The key names an
/// instance, and every process of that instance writes the same owner.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "read", rename_all = "snake_case")]
pub enum LeaderAtKill {
    /// The read failed or timed out, or the value did not parse.
    #[default]
    Unread,
    /// The key was absent.
    Vacant,
    /// The key held a leader record.
    Held {
        /// Instance id the record names.
        owner: String,
        /// Generation the record names.
        generation: u64,
        /// `fnv1a` over the bytes the read returned.
        digest: u64,
    },
}

/// One journal entry, without its timestamp.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "ev", rename_all = "snake_case")]
pub enum Event {
    /// The sink made these record ids durable.
    Rows {
        /// Record ids in batch order.
        ids: Vec<String>,
    },
    /// A durable `split.*` write is about to be sent.
    Send {
        /// Pairs this line with its `done`; unique within one process.
        call: u64,
        /// The store call.
        op: WriteOp,
        /// Store key.
        key: String,
        /// The revision an update replaces; absent on a create.
        expected: Option<u64>,
        /// The value written.
        value: Progress,
    },
    /// The reply to the `send` with the same `call`.
    Done {
        /// The `send` this answers.
        call: u64,
        /// Store key.
        key: String,
        /// The reply.
        reply: Reply,
    },
    /// A durable `split.*` entry read from the store.
    Seen {
        /// Store key.
        key: String,
        /// Revision of the entry.
        rev: u64,
        /// The entry's value.
        value: Progress,
        /// The call that returned it.
        from: Source,
    },
    /// A `get` of a durable `split.*` key returned an error or was dropped
    /// before it returned.
    ReadFailed {
        /// Store key.
        key: String,
    },
    /// The process is about to abort on an injected fault.
    Abort {
        /// Store key of the write the abort is tied to.
        key: String,
        /// Kind of the write.
        kind: WriteKind,
        /// The write's ordinal among writes of its kind, from 1.
        n: u32,
        /// Where the abort fires.
        at: AbortPoint,
    },
    /// A write landed and its caller was handed a retryable error instead.
    ErrAfterLand {
        /// Store key.
        key: String,
        /// Revision the write landed at.
        rev: u64,
    },
    /// The process is about to stop itself before sending a commit.
    Stop {
        /// Store key of the commit.
        key: String,
        /// The revision the commit replaces.
        expected: u64,
        /// Epoch the commit carries.
        epoch: u64,
    },
    /// The process is about to stop itself before sending a leader write.
    LeaderStop {
        /// Store key of the write.
        key: String,
        /// The stop plan's kind.
        kind: WriteKind,
        /// The write's ordinal among the writes the plan counts, from 1.
        n: u32,
        /// The value the write would send, as JSON.
        value: serde_json::Value,
        /// The process had sent its publish: its first `plan` update after
        /// its first seed.
        published: bool,
    },
    /// The harness sent SIGKILL.
    Kill {
        /// Target instance id.
        instance: String,
        /// Target pid.
        pid: u32,
        /// What the leader key held just before the kill.
        #[serde(default)]
        leader: LeaderAtKill,
    },
    /// The harness started a replacement process.
    Respawn {
        /// Instance id the replacement takes over.
        instance: String,
        /// The replacement's pid.
        pid: u32,
    },
    /// The harness sent SIGSTOP.
    Sigstop {
        /// Target instance id.
        instance: String,
        /// Target pid.
        pid: u32,
        /// Planned length of the stop.
        duration_ms: u64,
    },
    /// The harness sent SIGCONT.
    Sigcont {
        /// Target instance id.
        instance: String,
        /// Target pid.
        pid: u32,
    },
    /// The DynamoDB fault proxy serving a worker answered a call with a fault.
    ProxyFault {
        /// Instance id the proxy serves.
        instance: String,
        /// That instance's pid.
        pid: u32,
        /// The fault, as the proxy names it.
        fault: String,
        /// Store key of the call, when it carried one.
        key: Option<String>,
    },
    /// The harness opened a Toxiproxy window on a worker's store link.
    Toxic {
        /// Instance id the proxy serves.
        instance: String,
        /// That instance's pid.
        pid: u32,
        /// The window's kind, such as `blackhole` or `latency(120ms)`.
        toxic: String,
        /// Length of the window.
        duration_ms: u64,
    },
}

/// One journal line: an [`Event`] and the wall-clock time it was written.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Line {
    /// Milliseconds since the Unix epoch. Comparable across the processes
    /// of one run, which share a host.
    pub t_ms: u64,
    /// The entry.
    #[serde(flatten)]
    pub event: Event,
}

/// An open journal file. Lines from concurrent callers never interleave.
#[derive(Debug)]
pub struct Journal {
    file: Mutex<File>,
}

impl Journal {
    /// Opens `path` for appending, creating it when absent.
    ///
    /// # Errors
    ///
    /// Fails when the file cannot be opened.
    pub fn open(path: &Path) -> io::Result<Journal> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Journal {
            file: Mutex::new(file),
        })
    }

    /// Appends `event`, stamped with the current time, as one line written
    /// from a single buffer under the lock.
    ///
    /// Safe to call from `Drop`: it neither blocks on the runtime nor awaits.
    ///
    /// # Errors
    ///
    /// Fails when the write fails.
    pub fn append(&self, event: Event) -> io::Result<()> {
        let line = Line {
            t_ms: now_ms(),
            event,
        };
        let mut buf = serde_json::to_vec(&line).map_err(io::Error::other)?;
        buf.push(b'\n');
        let mut file = self
            .file
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        file.write_all(&buf)
    }
}

/// Reads the journal at `path`.
///
/// # Errors
///
/// Fails as [`parse`] does, or when the file cannot be read.
pub fn read(path: &Path) -> Result<Vec<Line>, JournalError> {
    let text = std::fs::read_to_string(path).map_err(JournalError::Io)?;
    parse(&text)
}

/// Parses journal text.
///
/// A final line with no terminating newline is dropped: a process killed
/// mid-write can leave one.
///
/// # Errors
///
/// Fails on a complete line that does not parse, or on a progress record at a
/// schema other than [`SCHEMA`].
pub fn parse(text: &str) -> Result<Vec<Line>, JournalError> {
    let complete = match text.rfind('\n') {
        Some(end) => &text[..end],
        None => "",
    };
    let mut lines = Vec::new();
    for (index, raw) in complete.split('\n').enumerate() {
        if raw.is_empty() {
            continue;
        }
        let line: Line = serde_json::from_str(raw).map_err(|e| JournalError::Line {
            line: index + 1,
            error: e.to_string(),
        })?;
        if let Event::Send { value, .. } | Event::Seen { value, .. } = &line.event
            && value.schema != SCHEMA
        {
            return Err(JournalError::Schema(value.schema));
        }
        lines.push(line);
    }
    Ok(lines)
}

/// Why a journal or a progress record could not be read.
#[derive(Debug)]
pub enum JournalError {
    /// The file could not be read.
    Io(io::Error),
    /// A complete line did not parse.
    Line {
        /// One-based line number.
        line: usize,
        /// The parse error.
        error: String,
    },
    /// Stored bytes were not a progress record.
    Record(String),
    /// A progress record carried this schema instead of [`SCHEMA`].
    Schema(u32),
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JournalError::Io(e) => write!(f, "reading the journal: {e}"),
            JournalError::Line { line, error } => write!(f, "journal line {line}: {error}"),
            JournalError::Record(e) => write!(f, "not a progress record: {e}"),
            JournalError::Schema(schema) => write!(
                f,
                "progress record at schema {schema}, but the journal reads schema {SCHEMA}"
            ),
        }
    }
}

impl std::error::Error for JournalError {}

/// Milliseconds since the Unix epoch, as journal lines carry them.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn progress(schema: u32) -> Progress {
        Progress {
            schema,
            epoch: 1,
            owner: Some("w0".to_owned()),
            watermark: Some(7),
            completed: false,
            status: Status::Runnable,
            attempts: 0,
        }
    }

    fn line(event: Event) -> String {
        let mut s = serde_json::to_string(&Line { t_ms: 1, event }).unwrap();
        s.push('\n');
        s
    }

    /// A last line cut short by a kill is dropped and every complete line
    /// before it is kept.
    #[test]
    fn reader_drops_a_truncated_last_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w0-1.ndjson");
        let journal = Journal::open(&path).unwrap();
        journal
            .append(Event::Rows {
                ids: vec!["o001-r000001".to_owned()],
            })
            .unwrap();
        journal
            .append(Event::Done {
                call: 1,
                key: "split.a".to_owned(),
                reply: Reply::Cancelled,
            })
            .unwrap();
        drop(journal);
        let mut text = std::fs::read_to_string(&path).unwrap();
        text.push_str(r#"{"t_ms":3,"ev":"rows","ids":["o00"#);
        std::fs::write(&path, &text).unwrap();

        let lines = read(&path).unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(
            lines[1].event,
            Event::Done {
                call: 1,
                key: "split.a".to_owned(),
                reply: Reply::Cancelled,
            }
        );

        let complete = parse(&text[..text.rfind('\n').unwrap()]);
        assert_eq!(
            complete.unwrap().len(),
            1,
            "an unterminated line is dropped"
        );
        let corrupt = format!("{}{}", line(Event::Rows { ids: vec![] }), "nope\n");
        assert!(
            matches!(parse(&corrupt), Err(JournalError::Line { line: 2, .. })),
            "a complete line that does not parse fails the read"
        );
    }

    /// A progress record at another schema fails the read, in a journal line
    /// and in stored bytes.
    #[test]
    fn reader_rejects_schema_other_than_3() {
        let seen = |schema| Event::Seen {
            key: "split.a".to_owned(),
            rev: 4,
            value: progress(schema),
            from: Source::Watch,
        };
        assert_eq!(parse(&line(seen(SCHEMA))).unwrap().len(), 1);
        assert!(matches!(
            parse(&line(seen(4))),
            Err(JournalError::Schema(4))
        ));
        let send = Event::Send {
            call: 1,
            op: WriteOp::Update,
            key: "split.a".to_owned(),
            expected: Some(3),
            value: progress(2),
        };
        assert!(matches!(parse(&line(send)), Err(JournalError::Schema(2))));

        let record = br#"{"schema":3,"id":"a","fp":1,"epoch":2,"status":"runnable","owner":null,"attempts":1,"watermark":null,"state":null,"completed":false,"written_at_ms":5}"#;
        let parsed = Progress::parse(record).unwrap();
        assert_eq!((parsed.epoch, parsed.attempts, parsed.owner), (2, 1, None));
        let old = br#"{"schema":2,"epoch":2,"status":"runnable","owner":null,"attempts":1,"watermark":null,"completed":false}"#;
        assert!(matches!(Progress::parse(old), Err(JournalError::Schema(2))));
    }

    /// A `kill` line with no `leader` field parses as `Unread`, and a `Held`
    /// read round-trips with its digest.
    #[test]
    fn a_kill_line_without_leader_reads_as_unread() {
        let old = parse("{\"t_ms\":1,\"ev\":\"kill\",\"instance\":\"w0\",\"pid\":10}\n").unwrap();
        assert_eq!(
            old[0].event,
            Event::Kill {
                instance: "w0".to_owned(),
                pid: 10,
                leader: LeaderAtKill::Unread,
            }
        );
        let held = Event::Kill {
            instance: "w1".to_owned(),
            pid: 11,
            leader: LeaderAtKill::Held {
                owner: "w1".to_owned(),
                generation: 2,
                digest: u64::MAX - 1,
            },
        };
        assert_eq!(parse(&line(held.clone())).unwrap()[0].event, held);
    }

    /// A journal holding only an unterminated line reads as empty.
    #[test]
    fn reader_drops_a_lone_truncated_line() {
        let lines = parse(r#"{"t_ms":3,"ev":"rows","ids":["o0"#).unwrap();
        assert!(lines.is_empty());
    }
}
