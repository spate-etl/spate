//! A panic in the registry pre-warm task is logged with the task's name.

use spate_avro::{AvroDeserializerBuilder, AvroDeserializerConfig, AvroRegistrySection};
use spate_test_support::RegistryStub;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
                let stub = RegistryStub::start();
                stub.script(
                    "/subjects/e-value/versions/latest",
                    200,
                    serde_json::json!({
                        "schema": SCHEMA, "id": 42, "version": 1, "subject": "e-value"
                    })
                    .to_string(),
                    0,
                );
                let mut settings =
                    AvroDeserializerConfig::confluent(AvroRegistrySection::new(stub.endpoint()));
                settings.prewarm_subjects = vec!["e-value".into()];
                let _builder =
                    AvroDeserializerBuilder::new(settings, &tokio::runtime::Handle::current())
                        .unwrap();
                tokio::time::timeout(spate_test::HANG, seen.notified())
                    .await
                    .expect("the pre-warm panic is logged");
            });
    });
    assert_eq!(
        *panics.lock().unwrap(),
        vec![("avro-registry-prewarm".to_owned(), "pre-warm".to_owned())]
    );
}
