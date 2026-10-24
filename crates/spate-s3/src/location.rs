//! The job location of a bounded feed job: an object-store prefix holding
//! the job fingerprint, which the lister writes and every worker reads, the
//! lister's checkpoint, and the end marker a lister run deletes before it
//! publishes and writes once the feed confirmed every unit of the run.
//!
//! [`Location::parse`] checks the `feed:` section's `location` against the
//! source URL. [`feed_fingerprint`] is the identity a worker and the lister
//! compare, and [`await_fingerprint`] is a worker's startup wait for it.
//! [`read_end`] is a worker's read of the end marker, which its end check
//! compares at the start and the end of a settling window.

use crate::config::Compression;
use crate::error::{bounded_reason, classify};
use bytes::Bytes;
use futures_util::StreamExt as _;
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

/// The step between a waiting worker's reads of the fingerprint.
pub(crate) const FINGERPRINT_POLL: Duration = Duration::from_secs(5);

/// The largest object a location read accepts. Every object here is a
/// fingerprint, one object key or an end marker, at most a few kilobytes, so
/// a larger one is not this job's.
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
    /// empty value, one that is not a URL, one with userinfo, a query or a
    /// fragment, any scheme but `s3`, `s3a` and `file`, an `s3` or `s3a`
    /// location naming no bucket, a `file` location naming a host, a prefix
    /// with an empty segment, a control character or bytes that are not
    /// UTF-8, and a location at or
    /// under the source's prefix in the same bucket.
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
        // The location's store reads none of these, `object_store` quotes the
        // whole URL in its errors, and a fragment would silently name a
        // shorter prefix.
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(refuse(
                "carries userinfo, a query or a fragment, which name nothing in the location's \
                 store; set credentials under the source's `store`"
                    .into(),
            ));
        }
        // Only these forms name the bucket in the same place for every
        // store; an `https://` URL's bucket depends on how its host is read.
        match url.scheme() {
            "s3" | "s3a" | "file" => {}
            "memory" => {
                return Err(refuse(
                    "memory:// creates a new empty store per component and cannot be shared".into(),
                ));
            }
            _ => {
                return Err(refuse(
                    "write it as s3://<bucket>/<prefix>, s3a://<bucket>/<prefix> or file://, with \
                     the endpoint and region under the source's `store`"
                        .into(),
                ));
            }
        }
        if url.scheme() != "file" && url.host_str().is_none_or(str::is_empty) {
            return Err(refuse("names no bucket".into()));
        }
        if url.scheme() == "file" && url.host_str().is_some_and(|h| !h.is_empty()) {
            return Err(refuse(
                "names a host; write a local path as file:///<path>".into(),
            ));
        }
        // With the scheme and host checked, only the path fails here.
        // `object_store`'s own error quotes the URL, so it stays out.
        let (_, prefix) = ObjectStoreScheme::parse(&url).map_err(|_| {
            refuse(
                "has a prefix with an empty segment, a control character or bytes that are not \
                 UTF-8"
                    .into(),
            )
        })?;
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

    /// The end marker, which a lister run deletes before its first publish
    /// and writes after the feed confirmed every unit of the run and the
    /// last checkpoint. Every worker reads it to end the job.
    pub(crate) fn end(&self) -> Path {
        self.prefix.clone().join("end")
    }

    /// The lister's checkpoint: the last listed key whose units, and those of
    /// every key listed before it, the feed has confirmed.
    pub(crate) fn checkpoint(&self) -> Path {
        self.prefix.clone().join("checkpoint")
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
pub(crate) fn starts_with(path: &Path, prefix: &Path) -> bool {
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
/// Fatal for an object above [`LOCATION_OBJECT_MAX_BYTES`]: refused from its
/// stated size before its body is read, or once its body runs past the cap
/// when the stated size understates it. Otherwise classed by [`classify`].
pub(crate) async fn read_object(
    store: &dyn ObjectStore,
    location: &Location,
    object: &Path,
) -> Result<Option<Bytes>, SourceError> {
    let got = match store.get(object).await {
        Ok(got) => got,
        // Checked before `classify`, which reads a missing object as fatal.
        // A missing bucket answers 404 as well and never fills, so it stays
        // a failed read; `object_store` keeps the store's code only in text.
        Err(e @ object_store::Error::NotFound { .. })
            if !e.to_string().contains("<Code>NoSuchBucket</Code>") =>
        {
            return Ok(None);
        }
        Err(e) => return Err(read_error(location, object, &e)),
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
    // `meta.size` comes from the response's `content-length`, which a chunked
    // body need not honour, so the body is counted as it arrives.
    let mut body = Vec::new();
    let mut stream = got.into_stream();
    while let Some(chunk) = stream.next().await {
        // A body failing mid-stream is a transport error, never a refused
        // read, so it carries no permission hint.
        let chunk = chunk.map_err(|e| io_error(location, object, &e))?;
        if body.len() + chunk.len() > LOCATION_OBJECT_MAX_BYTES as usize {
            return Err(location.fatal(
                object,
                format_args!(
                    "its body runs past the {LOCATION_OBJECT_MAX_BYTES} bytes a job location \
                     object holds"
                ),
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(Some(Bytes::from(body)))
}

/// Read the end marker and return the run it names, or `None` when there is
/// none.
///
/// # Errors
///
/// Fatal for a body that is not `run=`, 32 lowercase hex digits and a
/// newline, printing it escaped. Otherwise as [`read_object`].
pub(crate) async fn read_end(
    store: &dyn ObjectStore,
    location: &Location,
) -> Result<Option<String>, SourceError> {
    let object = location.end();
    let Some(body) = read_object(store, location, &object).await? else {
        return Ok(None);
    };
    let run = std::str::from_utf8(&body)
        .ok()
        .and_then(|body| body.strip_prefix("run="))
        .and_then(|rest| rest.strip_suffix('\n'))
        .filter(|run| {
            run.len() == 32
                && run
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        });
    match run {
        Some(run) => Ok(Some(run.to_owned())),
        None => Err(location.fatal(
            &object,
            format_args!(
                "it holds {:?}, which is not a lister run's end marker; another system or an \
                 incompatible build shares this location",
                String::from_utf8_lossy(&body)
            ),
        )),
    }
}

/// An `object_store` failure on `object`, classed by [`classify`].
pub(crate) fn io_error(location: &Location, object: &Path, e: &object_store::Error) -> SourceError {
    error_with(location, object, e, "")
}

/// [`io_error`] for a read, which on AWS S3 a 403 can mean the object is
/// absent: AWS answers a read of an absent object 403 to a caller without
/// `s3:ListBucket`.
pub(crate) fn read_error(
    location: &Location,
    object: &Path,
    e: &object_store::Error,
) -> SourceError {
    let hint = if matches!(e, object_store::Error::PermissionDenied { .. }) {
        "; on AWS S3 an absent object reads as 403 without `s3:ListBucket` on its bucket"
    } else {
        ""
    };
    error_with(location, object, e, hint)
}

fn error_with(
    location: &Location,
    object: &Path,
    e: &object_store::Error,
    hint: &str,
) -> SourceError {
    let text = bounded_reason(e);
    SourceError::Client {
        class: classify(e),
        reason: format!(
            "{} at the job location {}: {text}{hint}",
            object_name(object),
            location.shown(),
        ),
    }
}

/// Wait for the lister's fingerprint at `location` and compare it with
/// `mine`.
///
/// Reads at each [`FINGERPRINT_POLL`] step of `clock` until a fingerprint is
/// there, `window` has passed, or `stop` is set. The stop and the deadline
/// are checked at each step, a read in flight included, which is abandoned.
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
    // The last retryable read error, so a timeout reports what kept the
    // fingerprint from being read.
    let mut last_error: Option<SourceError> = None;
    // Whether any read has completed, so a read in flight at the deadline is
    // reported as the store's silence only when no read was answered.
    let mut answered = false;
    let timed_out = |last_error: &Option<SourceError>| match last_error {
        Some(e) => location.fatal(
            &object,
            format_args!("no read succeeded within {window:?}; the last failed: {e}"),
        ),
        None => location.fatal(
            &object,
            format_args!(
                "none appeared within {window:?}; the job's lister writes it before its first \
                 publish, so start the lister, or check that it names this location"
            ),
        ),
    };
    loop {
        if stop.is_set() {
            return Ok(());
        }
        let mut read = Box::pin(read_object(store, location, &object));
        // A read in flight is checked against the stop and the deadline at
        // each poll step, so neither waits on a store that does not answer.
        let outcome = loop {
            let tick = (next + FINGERPRINT_POLL).min(deadline);
            match select(read, clock.sleep_until(tick)).await {
                Either::Left((outcome, _)) => break outcome,
                Either::Right(((), pending)) => {
                    next = tick;
                    if clock.now() >= deadline {
                        if answered {
                            return Err(timed_out(&last_error));
                        }
                        return Err(location.fatal(
                            &object,
                            format_args!("the store answered no read of it within {window:?}"),
                        ));
                    }
                    if stop.is_set() {
                        return Ok(());
                    }
                    read = pending;
                }
            }
        };
        answered = true;
        match outcome {
            Ok(Some(stored)) => return compare(location, mine, &stored),
            Ok(None) => last_error = None,
            Err(e) if is_retryable(&e) => {
                tracing::warn!(error = %e, "reading the job fingerprint failed; retrying");
                last_error = Some(e);
            }
            Err(e) => return Err(e),
        }
        next = (next + FINGERPRINT_POLL).min(deadline);
        clock.sleep_until(next).await;
        if clock.now() >= deadline {
            return Err(timed_out(&last_error));
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
    use spate_core::clock::tokio::TestClock;

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
        let redacted = feed_fingerprint(
            "s3://user:hunter2@data-bucket/events/?token=x",
            Compression::Auto,
            64 << 20,
            Some(b'\n'),
        );
        assert!(
            !redacted.contains("hunter2") && !redacted.contains("token=x"),
            "{redacted}"
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

    /// Pins that a location on any scheme but `s3`, `s3a` and `file` is
    /// refused naming the forms it takes, `https://` included, and that those
    /// three are accepted.
    #[test]
    fn a_location_on_a_scheme_the_source_cannot_build_is_refused() {
        for location in [
            "https://store:9000/ops/jobs/a/",
            "https://ops.s3.eu-west-2.amazonaws.com/jobs/a/",
            "https://0123abcd.r2.cloudflarestorage.com/ops/jobs/a/",
            "http://store:9000/ops/jobs/a/",
            "gs://ops/jobs/a/",
            "sftp://ops-host/jobs/a/",
        ] {
            let err = refused(location, SOURCE);
            assert!(
                err.contains("`location`") && err.contains("s3://<bucket>/<prefix>"),
                "{location}: {err}"
            );
        }
        for location in [
            "s3://ops/jobs/a/",
            "s3a://ops/jobs/a/",
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

    /// Pins that a location carrying userinfo or a query is refused without
    /// printing either, and that a scheme `object_store` does not recognise
    /// is refused without its error, which quotes the URL.
    #[test]
    fn a_location_with_credentials_in_it_is_refused_without_printing_them() {
        for location in [
            "s3://user:hunter2@ops/jobs/a/",
            "s3://hunter2@ops/jobs/a/",
            "s3://:hunter2@ops/jobs/a/",
            "s3://ops/jobs/a/?X-Amz-Signature=tok3n",
            "sftp://user:hunter2@ops-host/jobs/a/?sig=tok3n",
        ] {
            let err = refused(location, SOURCE);
            assert!(err.contains("`location`"), "{err}");
            assert!(!err.contains("hunter2") && !err.contains("tok3n"), "{err}");
        }
        assert!(refused("s3://user:hunter2@ops/jobs/a/", SOURCE).contains("userinfo"));
        assert!(refused("s3://ops/jobs/run#1/", SOURCE).contains("fragment"));
    }

    /// Pins that an S3 location that names no bucket is refused when the
    /// location is parsed, before any store is built.
    #[test]
    fn an_s3_url_with_no_bucket_the_client_parses_is_refused() {
        for location in ["s3:///jobs/a/", "s3a:///jobs/a/"] {
            let err = refused(location, SOURCE);
            assert!(err.contains("no bucket"), "{location}: {err}");
        }
    }

    /// Pins that a `file` location naming a host, and a prefix
    /// `object_store` cannot read as a path, are refused for that cause.
    #[test]
    fn a_location_whose_host_or_prefix_the_store_cannot_read_is_refused() {
        assert!(refused("file://host/jobs/a/", SOURCE).contains("names a host"));
        for location in [
            "s3://ops/jobs//a/",
            "s3://ops/jobs/%01/a/",
            "s3://ops/jobs/%FF/a/",
        ] {
            let err = refused(location, SOURCE);
            assert!(err.contains("empty segment"), "{location}: {err}");
        }
    }

    /// Pins the same-bucket refusal against a source URL that names its
    /// bucket in an `https://` form, which a comparison of raw hosts would
    /// miss.
    #[test]
    fn a_location_naming_the_source_bucket_in_another_form_is_refused() {
        for source in [
            "https://data-bucket.s3.eu-west-2.amazonaws.com/events/",
            "https://s3.eu-west-2.amazonaws.com/data-bucket/events/",
        ] {
            let err = refused("s3://data-bucket/events/jobs/a/", source);
            assert!(err.contains("outside the prefix"), "{source}: {err}");
            Location::parse("s3://data-bucket/elsewhere/jobs/a/", source).unwrap();
        }
    }

    /// Pins that a location with and without a trailing `/` names the same
    /// objects.
    #[test]
    fn a_trailing_slash_names_the_same_location() {
        let with = accepted("s3://ops/jobs/a/");
        let without = accepted("s3://ops/jobs/a");
        assert_eq!(with.fingerprint(), without.fingerprint());
        assert_eq!(with.end(), without.end());
        assert_eq!(with.checkpoint(), without.checkpoint());
        assert_eq!(with.fingerprint().as_ref(), "jobs/a/fingerprint");
        assert_eq!(with.end().as_ref(), "jobs/a/end");
        assert_eq!(with.checkpoint().as_ref(), "jobs/a/checkpoint");
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

    /// Pins that a body running past the cap is refused as it arrives, when
    /// the response's `content-length` understates it.
    #[tokio::test]
    async fn a_body_past_the_cap_is_refused_whatever_its_stated_length() {
        let store = HookStore {
            get: Some(|path| {
                let meta = object_store::ObjectMeta {
                    location: path.clone(),
                    last_modified: chrono::Utc::now(),
                    size: 10,
                    e_tag: None,
                    version: None,
                };
                let chunks = (0..4).map(|_| Ok(bytes::Bytes::from(vec![b'x'; 2048])));
                Some(Ok(object_store::GetResult {
                    payload: object_store::GetResultPayload::Stream(Box::pin(
                        futures_util::stream::iter(chunks),
                    )),
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
        assert!(err.contains("runs past"), "{err}");
        assert!(
            err.len() < 400,
            "the error quotes the body: {} bytes",
            err.len()
        );
    }

    /// Pins that a body exactly at the cap, arriving in several chunks, is
    /// read whole.
    #[tokio::test]
    async fn a_body_at_the_cap_in_several_chunks_is_read() {
        let store = HookStore {
            get: Some(|path| {
                let half = LOCATION_OBJECT_MAX_BYTES / 2;
                let meta = object_store::ObjectMeta {
                    location: path.clone(),
                    last_modified: chrono::Utc::now(),
                    size: LOCATION_OBJECT_MAX_BYTES,
                    e_tag: None,
                    version: None,
                };
                let chunks = (0..2).map(move |_| Ok(bytes::Bytes::from(vec![b'x'; half as usize])));
                Some(Ok(object_store::GetResult {
                    payload: object_store::GetResultPayload::Stream(Box::pin(
                        futures_util::stream::iter(chunks),
                    )),
                    range: 0..meta.size,
                    meta,
                    attributes: Default::default(),
                    extensions: Default::default(),
                }))
            }),
            ..HookStore::default()
        };
        let location = accepted("s3://ops/jobs/a/");
        let read = read_object(&store, &location, &location.fingerprint())
            .await
            .unwrap()
            .expect("the object is there");
        assert_eq!(read.len() as u64, LOCATION_OBJECT_MAX_BYTES);
    }

    /// Pins that a body failing mid-stream keeps the store's class, so the
    /// wait retries a reset read.
    #[tokio::test]
    async fn a_body_failing_mid_stream_is_retryable() {
        let store = HookStore {
            get: Some(|path| {
                let meta = object_store::ObjectMeta {
                    location: path.clone(),
                    last_modified: chrono::Utc::now(),
                    size: 64,
                    e_tag: None,
                    version: None,
                };
                let chunks = [
                    Ok(bytes::Bytes::from_static(b"spate-s3:")),
                    Err(object_store::Error::Generic {
                        store: "scripted",
                        source: "connection reset".into(),
                    }),
                ];
                Some(Ok(object_store::GetResult {
                    payload: object_store::GetResultPayload::Stream(Box::pin(
                        futures_util::stream::iter(chunks),
                    )),
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
            .unwrap_err();
        assert!(is_retryable(&err), "{err}");
    }

    /// Pins that the `s3:ListBucket` hint rides on a read's 403 only: never
    /// on another read error, and never on a refused write.
    #[test]
    fn only_a_refused_read_names_s3_list_bucket() {
        let location = accepted("s3://ops/jobs/a/");
        let object = location.fingerprint();
        let denied = || object_store::Error::PermissionDenied {
            path: "jobs/a/fingerprint".into(),
            source: "403 Forbidden".into(),
        };
        let reset = object_store::Error::Generic {
            store: "S3",
            source: "connection reset".into(),
        };
        assert!(
            read_error(&location, &object, &denied())
                .to_string()
                .contains("s3:ListBucket")
        );
        assert!(
            !read_error(&location, &object, &reset)
                .to_string()
                .contains("s3:ListBucket")
        );
        assert!(
            !io_error(&location, &object, &denied())
                .to_string()
                .contains("s3:ListBucket")
        );
    }

    /// Pins that a store's error text reaches a location error cut to a
    /// bound, whatever the size of the store's answer.
    #[test]
    fn a_store_error_of_any_size_is_cut_to_a_bound() {
        let location = accepted("s3://ops/jobs/a/");
        let huge = object_store::Error::Generic {
            store: "S3",
            source: "x".repeat(1 << 20).into(),
        };
        let shown = read_error(&location, &location.fingerprint(), &huge).to_string();
        assert!(shown.len() < 1024, "{} bytes", shown.len());
        assert!(shown.ends_with('…'), "{shown}");
    }

    /// Pins the cut at exactly 512 characters of multibyte text, which a cut
    /// by bytes would split mid-character.
    #[test]
    fn a_store_error_is_cut_at_512_characters() {
        let location = accepted("s3://ops/jobs/a/");
        let long = object_store::Error::Generic {
            store: "S3",
            source: "é".repeat(600).into(),
        };
        let shown = read_error(&location, &location.fingerprint(), &long).to_string();
        let text = shown
            .split_once("s3://ops/jobs/a/: ")
            .map(|(_, text)| text)
            .expect("the store's text follows the location");
        assert_eq!(text.chars().count(), 512 + 1, "{text}");
        assert!(text.ends_with("é…"), "{text}");
    }

    /// Pins that a missing bucket fails the read, apart from a missing
    /// object, which reads as absent.
    #[tokio::test]
    async fn a_missing_bucket_fails_the_read() {
        let store = HookStore {
            get: Some(|path| {
                Some(Err(object_store::Error::NotFound {
                    path: path.to_string(),
                    source: "404 Not Found: <Error><Code>NoSuchBucket</Code></Error>".into(),
                }))
            }),
            ..HookStore::default()
        };
        let location = accepted("s3://opps/jobs/a/");
        let err = read_object(&store, &location, &location.fingerprint())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("NoSuchBucket"), "{err}");
        let empty = HookStore::default();
        let read = read_object(&empty, &location, &location.fingerprint()).await;
        assert!(matches!(read, Ok(None)), "{read:?}");
    }

    /// Pins that an end marker parses as the run it names, and that a
    /// missing marker reads as `None`.
    #[tokio::test]
    async fn an_end_marker_parses_as_its_run_id() {
        use object_store::ObjectStoreExt as _;
        let store = HookStore::default();
        let location = accepted("s3://ops/jobs/a/");
        assert_eq!(read_end(&store, &location).await.unwrap(), None);
        let run = "0123456789abcdef0123456789abcdef";
        store
            .inner
            .put(&location.end(), format!("run={run}\n").into())
            .await
            .unwrap();
        assert_eq!(
            read_end(&store, &location).await.unwrap().as_deref(),
            Some(run)
        );
    }

    /// Pins that a body that is not a lister run's end marker ends the
    /// worker, naming the object and the location and printing the body
    /// escaped: an upper-case or short run, a missing or extra newline, bytes
    /// after it, an empty body, and one over the location object cap.
    #[tokio::test]
    async fn a_foreign_end_body_ends_the_worker() {
        use object_store::ObjectStoreExt as _;
        let run = "0123456789abcdef0123456789abcdef";
        let bodies: Vec<Vec<u8>> = vec![
            b"run=ABC\n".to_vec(),
            format!("run={}\n", run.to_uppercase()).into_bytes(),
            format!("run={run}").into_bytes(),
            format!("run={run}\n\n").into_bytes(),
            format!("run={run}\nrest").into_bytes(),
            format!("run={run}0\n").into_bytes(),
            format!("run={}\n", &run[1..]).into_bytes(),
            format!("run={}g\n", &run[1..]).into_bytes(),
            Vec::new(),
            b"\xff\xfe".to_vec(),
            vec![b'r'; 5000],
        ];
        for body in bodies {
            let store = HookStore::default();
            let location = accepted("s3://ops/jobs/a/");
            store
                .inner
                .put(&location.end(), body.clone().into())
                .await
                .unwrap();
            let err = read_end(&store, &location).await.unwrap_err();
            assert!(!is_retryable(&err), "{body:?}: {err}");
            let shown = err.to_string();
            assert!(
                shown.contains("end") && shown.contains("s3://ops/jobs/a/"),
                "{shown}"
            );
            assert!(!shown.contains('\n'), "the body prints escaped: {shown}");
        }
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
        let clock = TestClock::frozen();
        let settled = await_fingerprint(
            &store,
            &location,
            "mine",
            clock.as_ref(),
            &StopSignal::default(),
            spate_core::feed::DEFAULT_SETTLING_WINDOW,
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

    /// Pins that a location error on a long store error names the TLS
    /// rejection behind it after the cut, with the hint after that.
    #[test]
    fn a_long_store_error_keeps_its_tls_rejection_at_the_job_location() {
        let location = accepted("s3://ops/jobs/a/");
        let e = object_store::Error::Generic {
            store: "S3",
            source: Box::new(crate::test_servers::LongTls(rustls::Error::AlertReceived(
                rustls::AlertDescription::HandshakeFailure,
            ))),
        };
        for shown in [
            read_error(&location, &location.fingerprint(), &e).to_string(),
            io_error(&location, &location.fingerprint(), &e).to_string(),
        ] {
            assert!(shown.contains("…: "), "{shown}");
            assert!(shown.contains("HandshakeFailure"), "{shown}");
            assert!(shown.len() < 1024, "{} bytes", shown.len());
        }
    }
}
