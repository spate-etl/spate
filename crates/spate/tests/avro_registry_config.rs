//! Avro configuration validation through the facade.

use spate::avro::{AvroConfigError, AvroDeserializerBuilder};
use spate::config::ComponentConfig;

/// Facade component loading rejects URL credentials before constructing a deserializer.
/// Regression for #876.
#[test]
fn avro_registry_credentials_rejected_at_configuration_load() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for configured in [false, true] {
        let mut registry = serde_json::json!({"url": "https://urluser:urlsecret@registry.invalid"});
        if configured {
            registry["username"] = "svc".into();
            registry["password"] = "configuredsecret".into();
        }
        let component: ComponentConfig =
            serde_json::from_value(serde_json::json!({"avro": {"registry": registry}})).unwrap();
        let error =
            AvroDeserializerBuilder::from_component(&component, runtime.handle()).unwrap_err();
        assert!(matches!(error, AvroConfigError::Invalid { .. }), "{error}");
        for printed in [error.to_string(), format!("{error:?}")] {
            for field in ["registry.url", "registry.username", "registry.password"] {
                assert!(printed.contains(field), "{printed}");
            }
            for secret in [
                "urluser",
                "urlsecret",
                "configuredsecret",
                "registry.invalid",
            ] {
                assert!(!printed.contains(secret), "{printed}");
            }
        }
    }
}
