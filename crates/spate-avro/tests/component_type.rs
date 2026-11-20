//! Each Avro deserializer reports `component_type` `"avro"`, so a chain built
//! with `with_metrics` labels its deserializer stage with it.

use serde::Deserialize;
use spate_avro::{AvroDeserializerBuilder, AvroValue};
use spate_core::deser::{Deserializer, Owned};
use spate_core::ops::chain;
use spate_test::{component_config, metric_series, render_metrics};

const SCHEMA: &str = r#"{"type":"record","name":"E","fields":[{"name":"id","type":"long"}]}"#;

#[derive(Debug, Deserialize)]
struct E {
    #[expect(dead_code, reason = "decoded, never read")]
    id: i64,
}

fn builder(runtime: &tokio::runtime::Runtime) -> AvroDeserializerBuilder {
    let section = component_config(
        "avro",
        &format!("mode: raw\nschema:\n  inline: '{SCHEMA}'\n"),
    );
    AvroDeserializerBuilder::from_component_config(&section, runtime.handle())
        .expect("avro builder")
}

fn assert_stage_is_avro<T, D>(pipeline: &'static str, deser: D)
where
    T: Send + 'static,
    D: Deserializer<Owned<T>> + Send + 'static,
{
    let rendered = render_metrics(|| {
        let _chain = chain::<Owned<T>, _>(deser).with_metrics(pipeline, "main");
    });
    let labels = [
        ("pipeline", pipeline),
        ("component", "main.deserializer"),
        ("component_type", "avro"),
    ];
    assert!(
        !metric_series(&rendered, "spate_deser_records_total", &labels).is_empty(),
        "{rendered}"
    );
}

#[test]
fn every_avro_deserializer_labels_its_stage_avro() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let builder = builder(&runtime);
    assert_stage_is_avro::<AvroValue, _>("avro-value", builder.build_value());
    assert_stage_is_avro::<E, _>("avro-serde", builder.build_serde::<E>());
    assert_stage_is_avro::<E, _>("avro-datum", builder.build_serde_datum::<E>().unwrap());
}
