//! A panic in the registry pre-warm task is logged with the task's name.

use spate_avro::{AvroDeserializerBuilder, AvroMode, AvroSettings, RegistrySection};
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

const SCHEMA: &str = r#"{"type":"record","name":"E","fields":[{"name":"id","type":"long"}]}"#;

/// The `message`, `task` and `panic` fields of one event.
#[derive(Default)]
struct Fields {
    message: String,
    task: String,
    panic: String,
}

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "task" => self.task = value.to_owned(),
            "panic" => self.panic = value.to_owned(),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
}

/// Panics inside the pre-warm's poll on its `pre-warmed schema` event, and
/// records each `background task panicked` event as `(task, panic)`.
struct PanicOnPrewarm {
    next: AtomicU64,
    panics: Arc<Mutex<Vec<(String, String)>>>,
    seen: Arc<tokio::sync::Notify>,
}

impl Subscriber for PanicOnPrewarm {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(self.next.fetch_add(1, Ordering::Relaxed) + 1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        if fields.message == "pre-warmed schema" {
            panic!("pre-warm");
        }
        if fields.message == "background task panicked" {
            self.panics
                .lock()
                .unwrap()
                .push((fields.task, fields.panic));
            self.seen.notify_one();
        }
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

/// Pins that a panic in the pre-warm task is logged with the task name
/// `avro-registry-prewarm`.
#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "the stub server's tasks end when the test's runtime drops"
)]
fn a_prewarm_panic_is_logged_with_its_name() {
    let panics = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::new(tokio::sync::Notify::new());
    let subscriber = PanicOnPrewarm {
        next: AtomicU64::new(0),
        panics: Arc::clone(&panics),
        seen: Arc::clone(&seen),
    };
    tracing::subscriber::with_default(subscriber, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let body = serde_json::json!({
                    "schema": SCHEMA, "id": 42, "version": 1, "subject": "e-value"
                })
                .to_string();
                tokio::spawn(async move {
                    while let Ok((mut tcp, _)) = listener.accept().await {
                        let body = body.clone();
                        tokio::spawn(async move {
                            let mut request = [0u8; 4096];
                            let _ = tcp.read(&mut request).await;
                            let out = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                                 Content-Length: {}\r\n\r\n{body}",
                                body.len()
                            );
                            let _ = tcp.write_all(out.as_bytes()).await;
                        });
                    }
                });
                let settings = AvroSettings {
                    mode: AvroMode::Confluent,
                    registry: Some(RegistrySection::new(format!("http://{addr}"))),
                    prewarm_subjects: vec!["e-value".into()],
                    ..AvroSettings::default()
                };
                let _builder = AvroDeserializerBuilder::from_settings(
                    &settings,
                    &tokio::runtime::Handle::current(),
                )
                .unwrap();
                tokio::time::timeout(Duration::from_secs(10), seen.notified())
                    .await
                    .expect("the pre-warm panic is logged");
            });
    });
    assert_eq!(
        *panics.lock().unwrap(),
        vec![("avro-registry-prewarm".to_owned(), "pre-warm".to_owned())]
    );
}
