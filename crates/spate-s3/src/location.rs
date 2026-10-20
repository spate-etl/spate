//! The job location of a bounded feed job: an object-store prefix holding
//! the job fingerprint, the end marker and the lister's checkpoint, which
//! the lister writes and every worker reads.
//!
//! [`Location::parse`] checks the `feed:` section's `location` against the
//! source URL. [`feed_fingerprint`] is the identity a worker and the lister
//! compare, and [`await_fingerprint`] is a worker's startup wait for it.

use crate::config::Compression;
use crate::error::{classify, reason};
use bytes::Bytes;
use futures_util::future::{Either, select};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, ObjectStoreScheme};
use spate_core::clock::tokio::Clock;
use spate_core::config::{ConfigError, redact};
use spate_core::error::{ErrorClass, SourceError};
use spate_core::source::StopSignal;
use std::fmt;
use std::time::Duration;
use url::Url;

/// How long a worker waits for the lister's fingerprint before it exits.
/// The settling window's default.
pub(crate) const SETTLING_WINDOW: Duration = Duration::from_secs(5 * 60);

/// The step between a waiting worker's reads of the fingerprint.
pub(crate) const FINGERPRINT_POLL: Duration = Duration::from_secs(5);

/// The largest object a location read accepts. Every object here is a few
/// hundred bytes, so a larger one is not this job's.
pub(crate) const LOCATION_OBJECT_MAX_BYTES: u64 = 4096;

/// What every feed fingerprint starts with, so a foreign object at the
/// location is told apart from a mismatch.
const FINGERPRINT_TAG: &str = "spate-s3:feed";

/// A bounded job's location, checked against the source URL.
#[derive(Clone, Debug)]
pub(crate) struct Location {
    url: Url,
    /// The prefix within the location's store. A trailing `/` in the URL
    /// names the same prefix.
    prefix: Path,
}

impl Location {
    /// Parse the `feed:` section's `location` against the source's `url`.
    ///
    /// # Errors
    ///
    /// A [`ConfigError`] naming the key, with the value redacted, for an
    /// empty value, one that is not a URL, a `memory://` URL, a scheme the
    /// source cannot build a store for, and a location at or under the
    /// source's prefix in the same bucket.
    pub(crate) fn parse(location: &str, source_url: &str) -> Result<Location, ConfigError> {
        let refuse = |why: String| ConfigError::Component {
            context: "feed".into(),
            message: format!("`location` {}: {why}", redact::url(location)),
        };
        if location.trim().is_empty() {
            return Err(ConfigError::Component {
                context: "feed".into(),
                message: "`location` must not be empty".into(),
            });
        }
        let url = Url::parse(location).map_err(|e| refuse(format!("is not a URL: {e}")))?;
        let (scheme, prefix) = ObjectStoreScheme::parse(&url)
            .map_err(|e| refuse(format!("names no object store: {e}")))?;
        match scheme {
            ObjectStoreScheme::AmazonS3 | ObjectStoreScheme::Local => {}
            ObjectStoreScheme::Memory => {
                return Err(refuse(
                    "memory:// creates a new empty store per component and cannot be shared".into(),
                ));
            }
            _ => {
                return Err(refuse(
                    "the source opens s3://, s3a://, an AWS https:// URL or file://; on another \
                     S3-compatible store, write s3://<bucket>/<prefix> and set the endpoint \
                     under the source's `store`"
                        .into(),
                ));
            }
        }
        if let Some((source_root, source_prefix)) = Url::parse(source_url)
            .ok()
            .and_then(|u| Some((root(&u)?, ObjectStoreScheme::parse(&u).ok()?.1)))
            && root(&url) == Some(source_root)
            && starts_with(&prefix, &source_prefix)
        {
            return Err(refuse(format!(
                "sits at or under the source's prefix {}, so the source would read its \
                 objects; put it outside the prefix",
                redact::url(source_url)
            )));
        }
        Ok(Location { url, prefix })
    }

    /// The location's URL, for building its store.
    pub(crate) fn url(&self) -> &Url {
        &self.url
    }

    /// The job fingerprint the lister writes.
    pub(crate) fn fingerprint(&self) -> Path {
        self.prefix.clone().join("fingerprint")
    }

    /// The end marker a lister run writes after its last confirmed publish.
    #[cfg_attr(
        not(feature = "testing"),
        expect(
            dead_code,
            reason = "only the `testing`-gated `S3Source::list_once` lists for the feed"
        )
    )]
    pub(crate) fn end(&self) -> Path {
        self.prefix.clone().join("end")
    }

    /// The location for messages, without userinfo or query.
    pub(crate) fn shown(&self) -> impl fmt::Display + '_ {
        redact::url(self.url.as_str())
    }

    /// A fatal error about `object` at this location.
    pub(crate) fn fatal(&self, object: &Path, why: impl fmt::Display) -> SourceError {
        SourceError::Client {
            class: ErrorClass::Fatal,
            reason: format!(
                "{} at the job location {}: {why}",
                object_name(object),
                self.shown()
            ),
        }
    }
}

/// The store a URL names, as far as telling two URLs' stores apart needs.
#[derive(PartialEq, Eq)]
enum Root {
    Bucket(String),
    Local,
}

fn root(url: &Url) -> Option<Root> {
    match ObjectStoreScheme::parse(url).ok()?.0 {
        // The builder parses the bucket out of every S3 URL form, so
        // `object_store` stays the only parser of it. Building reads nothing
        // and sends nothing.
        ObjectStoreScheme::AmazonS3 => AmazonS3Builder::new()
            .with_url(url.as_str())
            .build()
            .ok()
            .and_then(|store| crate::source::scope_bucket(&store))
            .map(Root::Bucket),
        ObjectStoreScheme::Local => Some(Root::Local),
        _ => None,
    }
}

/// Whether `path`'s segments begin with every segment of `prefix`.
fn starts_with(path: &Path, prefix: &Path) -> bool {
    let mut parts = path.parts();
    prefix.parts().all(|want| parts.next() == Some(want))
}

fn object_name(object: &Path) -> &str {
    object.filename().unwrap_or("the location object")
}

/// The job fingerprint a bounded feed job's lister writes and every worker
/// compares: the unit format version, the source URL without userinfo or
/// query, `compression`, `split_target_bytes` in bytes and the framer's
/// resync delimiter. Byte-equal across every correctly configured instance.
pub(crate) fn feed_fingerprint(
    url: &str,
    compression: Compression,
    split_target_bytes: u64,
    delimiter: Option<u8>,
) -> String {
    use crate::split::DESCRIPTOR_VERSION;
    let delimiter = delimiter.map_or_else(|| "none".to_owned(), |d| format!("{d:02x}"));
    format!(
        "{FINGERPRINT_TAG}1:d{DESCRIPTOR_VERSION}:url={}:compression={compression:?}:\
         target={split_target_bytes}:delim={delimiter}",
        redact::url(url)
    )
}

/// Compare `stored`, the fingerprint at `location`, with this instance's.
///
/// # Errors
///
/// Fatal, printing both, when they differ; fatal with its own message when
/// `stored` is not a feed fingerprint at all.
pub(crate) fn compare(location: &Location, mine: &str, stored: &[u8]) -> Result<(), SourceError> {
    if stored == mine.as_bytes() {
        return Ok(());
    }
    let shown = String::from_utf8_lossy(stored);
    let object = location.fingerprint();
    if !stored.starts_with(FINGERPRINT_TAG.as_bytes()) {
        return Err(location.fatal(
            &object,
            format_args!(
                "it holds {shown:?}, which is not a spate-s3 feed fingerprint; another system \
                 or an incompatible build shares this location"
            ),
        ));
    }
    Err(location.fatal(
        &object,
        format_args!(
            "the job's fingerprint is {shown:?} and this instance's is {mine:?}; every \
             instance of one job agrees on `url`, `compression`, `split_target_bytes` and the \
             framer's delimiter, and a job with another configuration takes a new location"
        ),
    ))
}

/// Read the object at `object`, or `None` when there is none.
///
/// # Errors
///
/// Fatal for an object above [`LOCATION_OBJECT_MAX_BYTES`], refused before
/// its body is read; otherwise classed by [`classify`].
pub(crate) async fn read_object(
    store: &dyn ObjectStore,
    location: &Location,
    object: &Path,
) -> Result<Option<Bytes>, SourceError> {
    let got = match store.get(object).await {
        Ok(got) => got,
        // Checked before `classify`, which reads a missing object as fatal.
        Err(object_store::Error::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(io_error(location, object, &e)),
    };
    if got.meta.size > LOCATION_OBJECT_MAX_BYTES {
        return Err(location.fatal(
            object,
            format_args!(
                "it is {} bytes, above the {LOCATION_OBJECT_MAX_BYTES} a job location object \
                 holds",
                got.meta.size
            ),
        ));
    }
    got.bytes()
        .await
        .map(Some)
        .map_err(|e| io_error(location, object, &e))
}

/// An `object_store` failure on `object`, classed by [`classify`].
pub(crate) fn io_error(location: &Location, object: &Path, e: &object_store::Error) -> SourceError {
    SourceError::Client {
        class: classify(e),
        reason: format!(
            "{} at the job location {}: {}",
            object_name(object),
            location.shown(),
            reason(e)
        ),
    }
}

/// Wait for the lister's fingerprint at `location` and compare it with
/// `mine`.
///
/// Reads at each [`FINGERPRINT_POLL`] step of `clock` until a fingerprint is
/// there, `window` has passed, or `stop` is set, which is checked before each
/// read. A read still in flight at the deadline is abandoned.
///
/// # Errors
///
/// Fatal on a mismatch, on a fatal read error, and when no fingerprint
/// appears within `window`. A retryable read error is retried within the
/// window. A stop ends the wait with `Ok`.
pub(crate) async fn await_fingerprint(
    store: &dyn ObjectStore,
    location: &Location,
    mine: &str,
    clock: &dyn Clock,
    stop: &StopSignal,
    window: Duration,
) -> Result<(), SourceError> {
    let start = clock.now();
    let deadline = start + window;
    // Reads fall on a fixed schedule from `start`, so a late wakeup does
    // not shift every later read.
    let mut next = start;
    let object = location.fingerprint();
    let timed_out = || {
        location.fatal(
            &object,
            format_args!(
                "none appeared within {window:?}; the job's lister writes it before its first \
                 publish, so start the lister, or check that it names this location"
            ),
        )
    };
    loop {
        if stop.is_set() {
            return Ok(());
        }
        let read = Box::pin(read_object(store, location, &object));
        match select(read, clock.sleep_until(deadline)).await {
            Either::Left((Ok(Some(stored)), _)) => return compare(location, mine, &stored),
            Either::Left((Ok(None), _)) => {}
            Either::Left((Err(e), _)) if is_retryable(&e) => {
                tracing::debug!(error = %e, "reading the job fingerprint; retrying");
            }
            Either::Left((Err(e), _)) => return Err(e),
            Either::Right(((), _)) => return Err(timed_out()),
        }
        next = (next + FINGERPRINT_POLL).min(deadline);
        clock.sleep_until(next).await;
        if clock.now() >= deadline {
            return Err(timed_out());
        }
    }
}

fn is_retryable(e: &SourceError) -> bool {
    matches!(
        e,
        SourceError::Client {
            class: ErrorClass::Retryable,
            ..
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::HookStore;

    const SOURCE: &str = "s3://data-bucket/events/";

    fn refused(location: &str, source: &str) -> String {
        Location::parse(location, source).unwrap_err().to_string()
    }

    fn accepted(location: &str) -> Location {
        Location::parse(location, SOURCE).unwrap()
    }

    /// Pins the fingerprint for one configuration, that each field it
    /// covers changes it alone, and that the source URL's userinfo and query
    /// stay out of it.
    #[test]
    fn the_feed_fingerprint_changes_with_each_field() {
        let base = feed_fingerprint(SOURCE, Compression::Auto, 64 << 20, Some(b'\n'));
        assert_eq!(
            base,
            "spate-s3:feed1:d2:url=s3://data-bucket/events/:compression=Auto:\
             target=67108864:delim=0a"
        );
        for other in [
            feed_fingerprint(
                "s3://data-bucket/other/",
                Compression::Auto,
                64 << 20,
                Some(b'\n'),
            ),
            feed_fingerprint(SOURCE, Compression::Zstd, 64 << 20, Some(b'\n')),
            feed_fingerprint(SOURCE, Compression::Auto, 32 << 20, Some(b'\n')),
            feed_fingerprint(SOURCE, Compression::Auto, 64 << 20, Some(b'\t')),
            feed_fingerprint(SOURCE, Compression::Auto, 64 << 20, None),
        ] {
            assert_ne!(other, base);
        }
        assert!(
            feed_fingerprint(SOURCE, Compression::Auto, 64 << 20, None).ends_with(":delim=none")
        );
        let secret = feed_fingerprint(
            "s3://user:secret@data-bucket/events/?token=x",
            Compression::Auto,
            64 << 20,
            Some(b'\n'),
        );
        assert!(
            !secret.contains("secret") && !secret.contains("token=x"),
            "{secret}"
        );
    }

    /// Pins that an empty location, one that is not a URL and a `memory://`
    /// location are refused, each naming the key.
    #[test]
    fn a_location_is_refused_when_empty_unparsable_or_memory() {
        assert!(refused(" ", SOURCE).contains("must not be empty"));
        assert!(refused("no scheme here", SOURCE).contains("is not a URL"));
        assert!(refused("memory:///jobs/a/", SOURCE).contains("cannot be shared"));
        for location in [" ", "no scheme here", "memory:///jobs/a/"] {
            assert!(refused(location, SOURCE).contains("`location`"));
        }
    }

    /// Pins that a location on a scheme the source builds no store for is
    /// refused naming the key and without its userinfo, and that `s3://`,
    /// `s3a://`, an AWS `https://` URL and `file://` are accepted.
    #[test]
    fn a_location_on_a_scheme_the_source_cannot_build_is_refused() {
        let err = refused("https://u:secret@store:9000/ops/jobs/a/", SOURCE);
        assert!(
            err.contains("`location`") && err.contains("endpoint"),
            "{err}"
        );
        assert!(!err.contains("secret"), "{err}");
        for location in [
            "s3://ops/jobs/a/",
            "s3a://ops/jobs/a/",
            "https://ops.s3.eu-west-2.amazonaws.com/jobs/a/",
            "file:///var/spate/jobs/a/",
        ] {
            Location::parse(location, SOURCE).unwrap();
        }
    }

    /// Pins that a location at or under the source's prefix in its bucket is
    /// refused, that a sibling whose name only starts with the prefix's last
    /// segment and one in another bucket are accepted, and that a source at
    /// a bucket's root refuses every location in that bucket.
    #[test]
    fn a_location_at_or_under_the_source_prefix_is_refused() {
        for location in [
            "s3://data-bucket/events/",
            "s3://data-bucket/events",
            "s3://data-bucket/events/jobs/a/",
            "s3a://data-bucket/events/jobs/a/",
        ] {
            let err = refused(location, SOURCE);
            assert!(err.contains("outside the prefix"), "{location}: {err}");
        }
        Location::parse("s3://data-bucket/events-jobs/a/", SOURCE).unwrap();
        Location::parse("s3://ops/events/jobs/a/", SOURCE).unwrap();
        assert!(refused("s3://data-bucket/jobs/a/", "s3://data-bucket/").contains("outside"));
        assert!(refused("file:///data/events/jobs/", "file:///data/events/").contains("outside"));
        Location::parse("file:///data/jobs/", "file:///data/events/").unwrap();
    }

    /// Pins that a location with and without a trailing `/` names the same
    /// objects.
    #[test]
    fn a_trailing_slash_names_the_same_location() {
        let with = accepted("s3://ops/jobs/a/");
        let without = accepted("s3://ops/jobs/a");
        assert_eq!(with.fingerprint(), without.fingerprint());
        assert_eq!(with.end(), without.end());
        assert_eq!(with.fingerprint().as_ref(), "jobs/a/fingerprint");
        assert_eq!(with.end().as_ref(), "jobs/a/end");
    }

    /// Pins that an object above the cap is refused from its size, before
    /// its body is read: the stub's body panics when polled.
    #[tokio::test]
    async fn an_oversized_location_object_is_refused_before_its_body_is_read() {
        use futures_util::StreamExt as _;
        let store = HookStore {
            get: Some(|path| {
                let meta = object_store::ObjectMeta {
                    location: path.clone(),
                    last_modified: chrono::Utc::now(),
                    size: LOCATION_OBJECT_MAX_BYTES + 1,
                    e_tag: None,
                    version: None,
                };
                let body = futures_util::stream::once(async {
                    panic!("the body of an oversized object was read")
                })
                .boxed();
                Some(Ok(object_store::GetResult {
                    payload: object_store::GetResultPayload::Stream(body),
                    range: 0..meta.size,
                    meta,
                    attributes: Default::default(),
                    extensions: Default::default(),
                }))
            }),
            ..HookStore::default()
        };
        let location = accepted("s3://ops/jobs/a/");
        let err = read_object(&store, &location, &location.fingerprint())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("4097 bytes") && err.contains("fingerprint"),
            "{err}"
        );
    }

    /// Pins that an object exactly at the cap is read.
    #[tokio::test]
    async fn a_location_object_at_the_cap_is_read() {
        use object_store::ObjectStoreExt as _;
        let store = HookStore::default();
        let location = accepted("s3://ops/jobs/a/");
        let body = vec![b'x'; LOCATION_OBJECT_MAX_BYTES as usize];
        store
            .inner
            .put(&location.fingerprint(), body.clone().into())
            .await
            .unwrap();
        let read = read_object(&store, &location, &location.fingerprint())
            .await
            .unwrap();
        assert_eq!(read.as_deref(), Some(&body[..]));
    }

    /// Pins that a fatal read error ends the wait at once, with no retry on
    /// the clock: the wait settles on its first poll on a frozen clock.
    #[test]
    fn a_fatal_location_error_ends_the_wait_at_once() {
        use futures_util::FutureExt as _;
        let store = HookStore {
            get: Some(|path| {
                Some(Err(object_store::Error::PermissionDenied {
                    path: path.to_string(),
                    source: "denied".into(),
                }))
            }),
            ..HookStore::default()
        };
        let location = accepted("s3://ops/jobs/a/");
        let clock = spate_core::clock::tokio::TestClock::frozen();
        let settled = await_fingerprint(
            &store,
            &location,
            "mine",
            clock.as_ref(),
            &StopSignal::default(),
            SETTLING_WINDOW,
        )
        .now_or_never()
        .expect("a fatal error ends the wait without waiting on the clock");
        let err = settled.unwrap_err();
        assert!(!is_retryable(&err), "{err}");
        assert!(err.to_string().contains("s3://ops/jobs/a/"), "{err}");
    }

    /// Pins that a stored fingerprint that differs is printed escaped,
    /// beside this instance's.
    #[test]
    fn a_mismatch_prints_the_stored_fingerprint_escaped() {
        let location = accepted("s3://ops/jobs/a/");
        let mine = feed_fingerprint(SOURCE, Compression::Auto, 64 << 20, Some(b'\n'));
        let stored = "spate-s3:feed1:d2:url=s3://elsewhere/\nINJECTED";
        let err = compare(&location, &mine, stored.as_bytes())
            .unwrap_err()
            .to_string();
        assert!(err.contains(r"elsewhere/\nINJECTED"), "{err}");
        assert!(!err.contains("\nINJECTED"), "{err}");
        assert!(
            err.contains(&mine) && err.contains("s3://ops/jobs/a/"),
            "{err}"
        );
        compare(&location, &mine, mine.as_bytes()).unwrap();
    }

    /// Pins that a stored body that is no feed fingerprint gets its own
    /// message, apart from a mismatch.
    #[test]
    fn an_unrecognised_fingerprint_body_is_refused_apart_from_a_mismatch() {
        let location = accepted("s3://ops/jobs/a/");
        let mine = feed_fingerprint(SOURCE, Compression::Auto, 64 << 20, Some(b'\n'));
        let err = compare(&location, &mine, b"{\"owner\":\"another tool\"}")
            .unwrap_err()
            .to_string();
        assert!(err.contains("not a spate-s3 feed fingerprint"), "{err}");
        assert!(err.contains("fingerprint at the job location"), "{err}");
        let mismatch = compare(&location, &mine, b"spate-s3:feed1:other")
            .unwrap_err()
            .to_string();
        assert!(
            !mismatch.contains("not a spate-s3 feed fingerprint"),
            "{mismatch}"
        );
    }

    /// Pins that a location the store answers 403 fails fatally, naming the
    /// location and the status.
    #[tokio::test]
    async fn a_location_answering_403_fails_naming_it() {
        let store =
            crate::test_servers::store_at(&crate::test_servers::status_server("403 Forbidden"));
        let location = accepted("s3://ops/jobs/a/");
        let err = read_object(&store, &location, &location.fingerprint())
            .await
            .unwrap_err();
        let SourceError::Client { class, reason } = &err else {
            panic!("not a client error: {err}");
        };
        assert_eq!(*class, ErrorClass::Fatal, "{err}");
        assert!(
            reason.contains("s3://ops/jobs/a/") && reason.contains("403"),
            "{reason}"
        );
    }
}
