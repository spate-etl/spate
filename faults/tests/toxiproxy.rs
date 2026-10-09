//! Toxiproxy on a Docker network shared with the store it proxies. Needs
//! Docker and is ignored by default; run it with `cargo xtask fault-test`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use spate_coordination::store::nats::{NatsConfig, NatsStore};
use spate_coordination::store::{CoordinationStore as _, Keyspace};
use spate_test_support::{Stream, Toxic, Toxiproxy, container_image};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{GenericImage, ImageExt};

/// A proxy whose upstream names the NATS container reaches it over the
/// network both containers joined, and the API accepts each toxic call.
#[test]
#[ignore = "requires Docker"]
fn toxiproxy_resolves_containers_on_a_run_network() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let network = format!("spate-faults-{}-{nanos}", std::process::id());
    let nats_name = format!("{network}-nats");
    let (image, tag) = container_image(&["--pull", "nats"]);
    let _nats = GenericImage::new(image, tag)
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["-js"])
        .with_network(&network)
        .with_container_name(&nats_name)
        .start()
        .expect("start NATS");
    let toxiproxy = Toxiproxy::start(&network).expect("start Toxiproxy");
    let addr = toxiproxy
        .create_proxy("nats", 21000, &format!("{nats_name}:4222"))
        .expect("create the proxy");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let store = NatsStore::new(
        NatsConfig::new(vec![format!("nats://{addr}")], "toxiproxy"),
        Duration::from_secs(2),
    )
    .unwrap();
    let plan = rt.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(30),
            store.get(Keyspace::Durable, "plan"),
        )
        .await
    });
    assert!(matches!(plan, Ok(Ok(None))), "{plan:?}");

    let latency = Toxic::Latency(Duration::from_millis(50));
    toxiproxy
        .add_toxic("nats", "slow", Stream::Upstream, latency)
        .unwrap();
    toxiproxy.remove_toxic("nats", "slow").unwrap();
    for (stream, name) in [(Stream::Upstream, "up"), (Stream::Downstream, "down")] {
        toxiproxy
            .add_toxic("nats", name, stream, Toxic::Timeout(Duration::ZERO))
            .unwrap();
    }
    toxiproxy
        .add_toxic("nats", "cut", Stream::Downstream, Toxic::LimitData(1024))
        .unwrap();
    toxiproxy.set_enabled("nats", false).unwrap();
    toxiproxy.set_enabled("nats", true).unwrap();
}
