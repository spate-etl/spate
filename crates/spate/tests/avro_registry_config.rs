//! Avro configuration validation through the facade.

use spate::avro::AvroDeserializerBuilder;
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
        let error = AvroDeserializerBuilder::from_component_config(&component, runtime.handle())
            .map(|_| ())
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "deserializer.avro.registry.url: carries userinfo; set them in \
             `registry.username` and `registry.password`"
        );
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
