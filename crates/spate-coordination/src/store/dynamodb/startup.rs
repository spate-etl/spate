//! The checks a handle runs against its table before its first operation.

use super::DynamoDbConfig;
use super::table::{Meta, Shape, Status, Table, Ttl};
use crate::store::StoreError;
use std::time::Duration;

/// The item layout this store writes; a job written with another is refused.
pub(super) const LAYOUT: u64 = 1;

/// Checks the table's shape and TTL, creating the table and enabling TTL
/// when the config allows, then fixes or compares the job's meta item.
pub(super) async fn check(
    table: &dyn Table,
    config: &DynamoDbConfig,
    lease_ttl: Duration,
    meta_pk: &str,
) -> Result<(), StoreError> {
    let name = &config.table;
    let Some(shape) = table.describe().await? else {
        if !config.create_table {
            return Err(StoreError::Fatal(format!(
                "DynamoDB table {name} does not exist: create it, or set \
                 dynamodb.create_table to let the store create it"
            )));
        }
        table.create_table().await?;
        return Err(StoreError::Retryable(format!(
            "DynamoDB table {name} is being created"
        )));
    };
    check_shape(name, &shape)?;
    match table.describe_ttl().await? {
        Ttl::On(attr) if attr == "x" => {}
        Ttl::Off if config.create_table && shape.status == Status::Active => {
            table.enable_ttl().await?;
        }
        other => tracing::warn!(
            table = %name,
            ttl = ?other,
            "time to live is not enabled on attribute `x`; deleted and expired items \
             stay in the table"
        ),
    }
    let meta = Meta {
        lease_ms: u64::try_from(lease_ttl.as_millis()).unwrap_or(u64::MAX),
        layout: LAYOUT,
    };
    match table.put_meta(meta_pk, meta).await? {
        Some(held) if held != meta => Err(StoreError::Fatal(format!(
            "job {} in DynamoDB table {name} was started with a lease of {}ms and item \
             layout {}, and this worker has a lease of {}ms and layout {}; finish the job or \
             delete its items first",
            config.job, held.lease_ms, held.layout, meta.lease_ms, meta.layout
        ))),
        _ => Ok(()),
    }
}

fn check_shape(name: &str, shape: &Shape) -> Result<(), StoreError> {
    match &shape.status {
        Status::Active | Status::Updating => {}
        Status::Creating => {
            return Err(StoreError::Retryable(format!(
                "DynamoDB table {name} is being created"
            )));
        }
        Status::Other(status) => {
            return Err(StoreError::Fatal(format!(
                "DynamoDB table {name} is {status}"
            )));
        }
    }
    let keys: Vec<(&str, bool, &str)> = shape
        .keys
        .iter()
        .map(|k| (k.name.as_str(), k.hash, k.kind.as_str()))
        .collect();
    if keys != [("pk", true, "S"), ("sk", false, "S")] {
        return Err(StoreError::Fatal(format!(
            "DynamoDB table {name} must have a string hash key `pk` and a string range key \
             `sk`, found {keys:?}"
        )));
    }
    if shape.local_indexes > 0 {
        return Err(StoreError::Fatal(format!(
            "DynamoDB table {name} has a local secondary index, which caps a job's items at \
             10 GB; use a table without one"
        )));
    }
    if shape.replicas > 0 {
        return Err(StoreError::Fatal(format!(
            "DynamoDB table {name} has replicas, whose conditional writes do not order \
             against each other; use a table without replicas"
        )));
    }
    if shape.global_indexes > 0 {
        tracing::warn!(
            table = %name,
            "the table has a global secondary index, which every store write also updates"
        );
    }
    Ok(())
}
