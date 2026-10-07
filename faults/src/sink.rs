//! The worker's sink: journals the id of every record it makes durable.

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use spate_core::error::{ErrorClass, SinkError};
use spate_core::sink::{
    BatchConfig, BreakerConfig, InflightConfig, RetryConfig, SealedBatch, ShardWriter, SinkBundle,
    SinkParts, SinkPoolConfig,
};

use crate::journal::{Event, Journal};

/// Rows per sealed batch.
const BATCH_ROWS: u64 = 1_000;
/// How long a batch waits for more rows before it is sealed.
const LINGER: Duration = Duration::from_millis(100);

/// A one-shard sink whose write appends a `rows` line naming each record's
/// id and returns `Ok` only after the line is written. It seals a batch at
/// 1,000 rows or after 100 ms, so a run's commits interleave with its writes.
///
/// Rows are expected as `spate_test::TestEncoder` frames of JSON records
/// carrying their id in `k`.
#[derive(Clone, Debug)]
pub struct JournalSink {
    writer: JournalWriter,
}

impl JournalSink {
    /// A sink appending to `journal` that holds each write for `delay` first.
    #[must_use]
    pub fn new(journal: Arc<Journal>, delay: Duration) -> JournalSink {
        JournalSink {
            writer: JournalWriter { journal, delay },
        }
    }
}

impl SinkBundle for JournalSink {
    type Writer = JournalWriter;

    fn into_parts(self) -> SinkParts<JournalWriter> {
        let mut batch = BatchConfig::default();
        batch.max_rows = BATCH_ROWS;
        batch.linger = LINGER;
        let pool = SinkPoolConfig::new(
            batch,
            InflightConfig::default(),
            RetryConfig::default(),
            BreakerConfig::default(),
        );
        SinkParts::new(self.writer, vec![vec![()]], pool).with_component_type("journal")
    }
}

/// The [`ShardWriter`] of a [`JournalSink`].
#[derive(Clone, Debug)]
pub struct JournalWriter {
    journal: Arc<Journal>,
    delay: Duration,
}

impl ShardWriter for JournalWriter {
    type Endpoint = ();

    fn write_batch(
        &self,
        _endpoint: &(),
        batch: &SealedBatch,
    ) -> impl Future<Output = Result<(), SinkError>> + Send {
        let journal = Arc::clone(&self.journal);
        let delay = self.delay;
        let payload: Vec<u8> = batch
            .frames
            .iter()
            .flat_map(|f| f.iter().copied())
            .collect();
        async move {
            let ids = record_ids(&payload).map_err(fatal)?;
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            journal.append(Event::Rows { ids }).map_err(fatal)
        }
    }
}

/// The `k` field of each JSON row in a `TestEncoder` payload.
fn record_ids(payload: &[u8]) -> Result<Vec<String>, String> {
    #[derive(Deserialize)]
    struct Row {
        k: String,
    }
    spate_test::decode_rows(payload)
        .iter()
        .map(|row| {
            serde_json::from_slice::<Row>(row)
                .map(|r| r.k)
                .map_err(|e| format!("row is not a generated record: {e}"))
        })
        .collect()
}

fn fatal(reason: impl ToString) -> SinkError {
    SinkError::Client {
        class: ErrorClass::Fatal,
        reason: reason.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    /// A write journals every row's id, in batch order, before it returns.
    #[tokio::test]
    async fn write_journals_each_row_id_before_returning() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w0-1.ndjson");
        let journal = Arc::new(Journal::open(&path).unwrap());
        let writer = JournalWriter {
            journal,
            delay: Duration::ZERO,
        };
        let mut frame = Vec::new();
        for row in [
            r#"{"k":"o001-r000002","pad":"x"}"#,
            r#"{"k":"o000-r000000"}"#,
        ] {
            frame.extend_from_slice(&u32::try_from(row.len()).unwrap().to_le_bytes());
            frame.extend_from_slice(row.as_bytes());
        }
        let batch = SealedBatch {
            frames: vec![Bytes::from(frame)],
            rows: 2,
            bytes: 0,
            dedup_token: String::new(),
        };
        writer.write_batch(&(), &batch).await.unwrap();

        let lines = crate::journal::read(&path).unwrap();
        assert_eq!(
            lines.iter().map(|l| &l.event).collect::<Vec<_>>(),
            [&Event::Rows {
                ids: vec!["o001-r000002".to_owned(), "o000-r000000".to_owned()],
            }]
        );
    }

    /// A write appends nothing while its `delay` is pending.
    #[tokio::test]
    async fn write_holds_for_delay_before_journalling() {
        use futures_util::FutureExt as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("w0-1.ndjson");
        let journal = Arc::new(Journal::open(&path).unwrap());
        let writer = JournalWriter {
            journal,
            delay: Duration::from_secs(60),
        };
        let row = r#"{"k":"o000-r000000"}"#;
        let mut frame = u32::try_from(row.len()).unwrap().to_le_bytes().to_vec();
        frame.extend_from_slice(row.as_bytes());
        let batch = SealedBatch {
            frames: vec![Bytes::from(frame)],
            rows: 1,
            bytes: 0,
            dedup_token: String::new(),
        };
        assert!(writer.write_batch(&(), &batch).now_or_never().is_none());
        assert!(crate::journal::read(&path).unwrap().is_empty());
    }
}
