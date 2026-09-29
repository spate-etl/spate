//! [`DynamoDbFakeBackend`]: the DynamoDB store over one in-memory table,
//! with a handle of its own per worker.

use super::{Backend, LEASE, config_for};
use spate_coordination::store::dynamodb::{DynamoDbConfig, DynamoDbStore, FakeTable};
use spate_core::clock::tokio::{Clock, SystemClock};
use std::sync::Arc;
use std::time::Duration;

/// A store over `table` with the suite's lease and `op_timeout`, polling
/// every tenth of [`LEASE`].
pub fn store(table: &FakeTable, clock: Arc<dyn Clock>) -> DynamoDbStore {
    let mut config = DynamoDbConfig::new("spate-test", "job");
    config.poll_interval = LEASE / 10;
    let op_timeout = config_for(LEASE, None).op_timeout;
    DynamoDbStore::over_fake_table(config, LEASE, op_timeout, clock, table).expect("dynamodb store")
}

pub struct DynamoDbFakeBackend(FakeTable);

impl DynamoDbFakeBackend {
    pub fn new() -> DynamoDbFakeBackend {
        DynamoDbFakeBackend(FakeTable::new())
    }
}

impl Backend for DynamoDbFakeBackend {
    type Store = DynamoDbStore;

    fn store(&self) -> DynamoDbStore {
        store(&self.0, Arc::new(SystemClock))
    }

    fn lease(&self) -> Duration {
        LEASE
    }
}
