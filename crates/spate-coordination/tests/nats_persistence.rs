//! Adoption diagnostics and configuration preservation on a real NATS server.
#![cfg(feature = "nats")]

use async_nats::jetstream::{Context, stream};
use spate_coordination::store::nats::{NatsConfig, NatsStore};
use spate_coordination::store::{CasOutcome, CoordinationStore, Keyspace};
use spate_test::LogCapture;
use spate_test_support::container_image;
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{GenericImage, ImageExt};

const LEASE: Duration = Duration::from_secs(2);

fn warnings(capture: &LogCapture) -> Vec<String> {
    capture
        .lines()
        .into_iter()
        .filter(|line| line.contains("WARN") && line.contains("uses async persistence"))
        .collect()
}

async fn provision(
    js: &Context,
    job: &str,
    lease: bool,
    mode: stream::PersistenceMode,
    ttls: bool,
) -> stream::Config {
    let suffix = if lease { "lease" } else { "state" };
    let bucket = format!("spate_coordination_{job}_{suffix}");
    let mut created = js
        .create_stream(stream::Config {
            name: format!("KV_{bucket}"),
            subjects: vec![format!("$KV.{bucket}.>")],
            description: Some("operator persistence fixture".into()),
            max_messages_per_subject: 1,
            max_bytes: 1 << 24,
            max_age: if lease { LEASE } else { Duration::ZERO },
            storage: stream::StorageType::File,
            num_replicas: 1,
            allow_rollup: true,
            deny_delete: true,
            deny_purge: false,
            allow_direct: true,
            discard: stream::DiscardPolicy::New,
            allow_message_ttl: ttls,
            subject_delete_marker_ttl: ttls.then_some(LEASE),
            persist_mode: Some(mode),
            ..Default::default()
        })
        .await
        .expect("create raw KV stream");
    let config = created.info().await.unwrap().config.clone();
    let kv = js
        .get_key_value(bucket)
        .await
        .expect("raw stream is a KV bucket");
    kv.put("fixture", b"known-value".to_vec().into())
        .await
        .unwrap();
    assert_eq!(
        kv.get("fixture").await.unwrap().unwrap().as_ref(),
        b"known-value"
    );
    config
}

async fn config(js: &Context, name: &str) -> stream::Config {
    js.get_stream(name)
        .await
        .unwrap()
        .info()
        .await
        .unwrap()
        .config
        .clone()
}

/// Async state adoption warns once, continues writing, and retains operator settings.
/// Regression for #808.
#[test]
#[ignore = "requires Docker"]
fn adopted_async_state_bucket_warns() {
    let capture = LogCapture::new();
    tracing_subscriber::fmt()
        .with_writer(capture.clone())
        .with_max_level(tracing::Level::WARN)
        .without_time()
        .init();
    let (name, tag) = container_image(&["--pull", "nats", "async"]);
    let server = GenericImage::new(name, tag)
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["-js"])
        .start()
        .expect("start NATS persistence fixture");
    let url = format!(
        "nats://127.0.0.1:{}",
        server.get_host_port_ipv4(4222).unwrap()
    );
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = async_nats::connect(&url).await.unwrap();
        assert!(
            client.server_info().version.starts_with("2.12."),
            "fixture must run NATS 2.12"
        );
        let js = async_nats::jetstream::new(client);
        for (job, mode, ttls, async_lease) in [
            (
                "persistence_default",
                Some(stream::PersistenceMode::Default),
                true,
                false,
            ),
            (
                "persistence_async_ttls",
                Some(stream::PersistenceMode::Async),
                true,
                false,
            ),
            (
                "persistence_async_patch",
                Some(stream::PersistenceMode::Async),
                false,
                false,
            ),
            ("persistence_fresh", None, true, false),
            (
                "persistence_async_lease",
                Some(stream::PersistenceMode::Default),
                true,
                true,
            ),
        ] {
            let state_name = format!("KV_spate_coordination_{job}_state");
            let before_config = if let Some(mode) = mode {
                Some(provision(&js, job, false, mode, ttls).await)
            } else {
                None
            };
            let lease_config = if async_lease {
                Some(provision(&js, job, true, stream::PersistenceMode::Async, true).await)
            } else {
                None
            };
            let before = warnings(&capture).len();
            let store = NatsStore::new(NatsConfig::new(vec![url.clone()], job), LEASE).unwrap();
            assert!(matches!(
                store
                    .create(Keyspace::Durable, "first", b"first-value".to_vec())
                    .await
                    .unwrap(),
                CasOutcome::Won(_)
            ));
            assert_eq!(
                store
                    .get(Keyspace::Durable, "first")
                    .await
                    .unwrap()
                    .unwrap()
                    .value,
                b"first-value"
            );
            if !ttls {
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        if config(&js, &state_name).await.allow_message_ttl {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("TTL patch completed");
            }
            let expected = usize::from(mode == Some(stream::PersistenceMode::Async));
            let lines = warnings(&capture);
            assert_eq!(
                lines.len() - before,
                expected,
                "{job}: persistence warnings: {lines:?}"
            );
            if expected == 1 {
                assert!(
                    lines[before].contains(&format!("stream={state_name}")),
                    "warning identifies state stream: {}",
                    lines[before]
                );
                assert!(
                    lines[before].contains("sync_interval: always")
                        && lines[before].contains("default"),
                    "warning gives actionable durability settings: {}",
                    lines[before]
                );
            }
            assert!(matches!(
                store
                    .create(Keyspace::Durable, "second", b"second-value".to_vec())
                    .await
                    .unwrap(),
                CasOutcome::Won(_)
            ));
            assert_eq!(
                store
                    .get(Keyspace::Durable, "second")
                    .await
                    .unwrap()
                    .unwrap()
                    .value,
                b"second-value"
            );
            assert_eq!(
                warnings(&capture).len() - before,
                expected,
                "repeated operations do not warn again"
            );
            let after = config(&js, &state_name).await;
            assert_eq!(
                after.persist_mode.unwrap_or_default(),
                mode.unwrap_or_default()
            );
            if let Some(mut existing) = before_config {
                if !ttls {
                    existing.allow_message_ttl = true;
                    existing.subject_delete_marker_ttl = Some(LEASE);
                }
                assert_eq!(
                    after, existing,
                    "adoption changes only the established TTL fields"
                );
            }
            if let Some(existing) = lease_config {
                assert_eq!(
                    config(&js, &existing.name).await,
                    existing,
                    "lease settings remain intact"
                );
            }
        }
    });
}
