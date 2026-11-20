//! A unit names the keys the source reads, and whoever can publish to the
//! feed writes the unit. These tests pin that neither unit form makes the
//! source read an object outside its URL's prefix.

mod support;

use spate_core::feed::Unit;
use spate_s3::{DescriptorObject, SplitDescriptor};
use spate_test::{scripted_feed, wait_until};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use support::spy::{SpyOptions, spying_local_store};
use support::{
    DEADLINE, PipelineYaml, captured_rows, key_of, launch_customized, launch_on_notifications,
    line_framer, lines_bytes, notification, recs, stop, test_options,
};

/// A descriptor unit naming the whole object `key`.
fn descriptor_unit(key: String, size: u64) -> Unit {
    let body = SplitDescriptor::new(vec![DescriptorObject {
        key,
        size,
        etag: None,
        last_modified_ms: 1,
    }])
    .encode()
    .unwrap();
    Unit::new("descriptor", body)
}

/// Stage `root/data/` empty and `root/secret/a.ndjson` with three records.
/// Returns the `secret` directory and the object's size.
fn stage_outside(root: &Path) -> (std::path::PathBuf, u64) {
    fs::create_dir_all(root.join("data")).unwrap();
    let secret = root.join("secret");
    fs::create_dir_all(&secret).unwrap();
    fs::write(secret.join("a.ndjson"), lines_bytes(&recs("secret", 3))).unwrap();
    let size = fs::metadata(secret.join("a.ndjson")).unwrap().len();
    (secret, size)
}

fn secret_rows(script: &spate_test::SinkScript) -> Vec<String> {
    captured_rows(script)
        .into_iter()
        .filter(|r| r.contains("secret-"))
        .collect()
}

/// Pins that a source over `s3://b/data/` fails back a unit naming
/// `secret/a.ndjson` in the same bucket, for the same reason, whether the
/// unit is a notification or a descriptor, and delivers none of its records.
#[test]
fn a_unit_naming_a_key_outside_the_prefix_is_failed_back() {
    let dir = tempfile::tempdir().unwrap();
    let (_, size) = stage_outside(dir.path());

    let (feed, script) = scripted_feed();
    let announced = script.deliver(notification(&[("secret/a.ndjson", size)]));
    let described = script.deliver(descriptor_unit("secret/a.ndjson".to_owned(), size));
    let l = launch_on_notifications("s3", dir.path(), Arc::new(feed), |_| {});

    wait_until(DEADLINE, "both units are settled", || {
        script.failed().len() + script.acknowledged().len() >= 2
    });
    let read = secret_rows(&l.script);
    let failed = script.failed();
    stop(&l);

    assert_eq!(read, Vec::<String>::new());
    for (unit, receipt) in [("notification", announced), ("descriptor", described)] {
        let reason = failed
            .iter()
            .find_map(|(r, reason)| (*r == receipt).then_some(reason))
            .unwrap_or_else(|| panic!("the {unit} is failed back: {failed:?}"));
        assert!(
            reason
                .ends_with("the key \"secret/a.ndjson\" is outside the source's prefix \"data/\""),
            "{unit}: {reason}"
        );
    }
}

/// Pins the same for a `file://` source, whose store is rooted at `/`: a
/// descriptor naming a file beside the URL's directory is failed back, and
/// the store is never asked for the file.
#[test]
fn a_descriptor_naming_a_file_outside_a_file_url_is_failed_back() {
    let dir = tempfile::tempdir().unwrap();
    let (secret, size) = stage_outside(dir.path());
    let data = dir.path().join("data");

    let (feed, script) = scripted_feed();
    let feed = Arc::new(feed);
    let described = script.deliver(descriptor_unit(key_of(&secret, "a.ndjson"), size));
    let yaml = PipelineYaml::file("s3-unit-scope", &data)
        .fed_by_code()
        .build();
    let (store, spy) = spying_local_store(SpyOptions::default());
    let l = launch_customized(
        &yaml,
        test_options(),
        |_| {},
        move |source, _io| line_framer(source).with_store(store).with_feed(feed),
    );

    wait_until(DEADLINE, "the unit is settled", || {
        !script.failed().is_empty() || !script.acknowledged().is_empty()
    });
    let read = secret_rows(&l.script);
    let failed: Vec<_> = script.failed().into_iter().map(|(r, _)| r).collect();
    stop(&l);

    assert_eq!(read, Vec::<String>::new());
    assert_eq!(failed, [described]);
    let gets: Vec<String> = spy.gets().into_iter().map(|g| g.key).collect();
    assert_eq!(gets, Vec::<String>::new());
}
