//! The S3 listing's handling of keys `object_store` cannot address.
//!
//! `object_store` turns each listed key into a [`Path`], and a key that does
//! not parse fails the whole `ListObjectsV2` page, losing its continuation
//! token. A key that parses to a different string (a leading or trailing `/`)
//! would be read under that other key. [`UnaddressableConnector`] wraps the S3
//! client: it takes each such object out of a listing page before
//! `object_store` parses it and records it in [`UnaddressableKeys`], so the
//! listing returns every other key, and the planner and the lister still read
//! this one as a unit of its own.

use crate::fetch::ObjectEntry;
use bytes::Bytes;
use object_store::ClientOptions;
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpResponseBody, HttpService,
    ReqwestConnector,
};
use object_store::path::Path;
use std::sync::{Arc, Mutex};

const CONTENTS_OPEN: &[u8] = b"<Contents>";
const CONTENTS_CLOSE: &[u8] = b"</Contents>";

/// Whether `object_store` reads `key` under `key` itself.
///
/// `false` for a key [`Path::parse`] rejects (an empty, `.` or `..` segment,
/// an ASCII control character) and for one it parses to a different string
/// (a leading or trailing `/`).
pub(crate) fn addressable(key: &str) -> bool {
    Path::parse(key).is_ok_and(|path| path.as_ref() == key)
}

/// The objects [`UnaddressableConnector`] took out of listing pages, in page
/// order. Shared between the connector and the planner or lister that drains
/// it.
#[derive(Clone, Debug, Default)]
pub(crate) struct UnaddressableKeys(Arc<Mutex<Vec<ObjectEntry>>>);

impl UnaddressableKeys {
    /// Remove and return every recorded object.
    pub(crate) fn take(&self) -> Vec<ObjectEntry> {
        std::mem::take(&mut *self.lock())
    }

    fn record(&self, entries: Vec<ObjectEntry>) {
        self.lock().extend(entries);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<ObjectEntry>> {
        // A panic while holding the lock leaves a whole `Vec`; nothing is
        // half-written.
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// An [`HttpConnector`] that builds `object_store`'s own client and filters
/// the S3 listing responses it returns. Every other request and response
/// passes through unchanged.
#[derive(Debug)]
pub(crate) struct UnaddressableConnector {
    keys: UnaddressableKeys,
}

impl UnaddressableConnector {
    pub(crate) fn new(keys: UnaddressableKeys) -> Self {
        Self { keys }
    }
}

impl HttpConnector for UnaddressableConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let inner = ReqwestConnector::default().connect(options)?;
        Ok(HttpClient::new(FilteringService {
            inner,
            keys: self.keys.clone(),
        }))
    }
}

#[derive(Debug)]
struct FilteringService {
    inner: HttpClient,
    keys: UnaddressableKeys,
}

#[async_trait::async_trait]
impl HttpService for FilteringService {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        let listing = req.method().as_str() == "GET"
            && req
                .uri()
                .query()
                .is_some_and(|q| q.split('&').any(|pair| pair == "list-type=2"));
        let response = self.inner.execute(req).await?;
        if !listing || !response.status().is_success() {
            return Ok(response);
        }
        let (mut parts, body) = response.into_parts();
        let page = body.bytes().await?;
        let page = match filter_page(&page) {
            Some((filtered, removed)) => {
                self.keys.record(removed);
                // The body is shorter than the length the store sent.
                parts.headers.remove("content-length");
                Bytes::from(filtered)
            }
            None => page,
        };
        Ok(HttpResponse::from_parts(
            parts,
            HttpResponseBody::from(page),
        ))
    }
}

/// One `<Contents>` element, decoded the way `object_store` decodes it.
#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Contents {
    key: String,
    size: u64,
    last_modified: String,
    #[serde(rename = "ETag")]
    e_tag: Option<String>,
}

/// Remove each `<Contents>` element whose key is not [`addressable`] from a
/// `ListBucketResult` page.
///
/// Returns the page without them and the removed objects, or `None` when
/// nothing was removed. The rest of the page, continuation token included, is
/// copied byte for byte. An element that does not decode is kept, so
/// `object_store` reports it as it would without this filter.
fn filter_page(page: &[u8]) -> Option<(Vec<u8>, Vec<ObjectEntry>)> {
    let mut out = Vec::with_capacity(page.len());
    let mut removed = Vec::new();
    let mut rest = page;
    while let Some(start) = find(rest, CONTENTS_OPEN) {
        let Some(len) = find(&rest[start..], CONTENTS_CLOSE) else {
            break;
        };
        let end = start + len + CONTENTS_CLOSE.len();
        out.extend_from_slice(&rest[..start]);
        match unaddressable(&rest[start..end]) {
            Some(entry) => removed.push(entry),
            None => out.extend_from_slice(&rest[start..end]),
        }
        rest = &rest[end..];
    }
    if removed.is_empty() {
        return None;
    }
    out.extend_from_slice(rest);
    Some((out, removed))
}

/// The object an element names, when its key is not [`addressable`].
fn unaddressable(element: &[u8]) -> Option<ObjectEntry> {
    // The same decoder `object_store` runs over the page, so the key tested
    // is the key it would parse.
    let contents: Contents = quick_xml::de::from_str(std::str::from_utf8(element).ok()?).ok()?;
    if addressable(&contents.key) {
        return None;
    }
    let last_modified_ms = chrono::DateTime::parse_from_rfc3339(&contents.last_modified)
        .map_or(0, |t| t.timestamp_millis());
    Some(ObjectEntry {
        key: contents.key,
        size: contents.size,
        etag: contents.e_tag,
        last_modified_ms,
    })
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn contents(key: &str) -> String {
        format!(
            "<Contents><Key>{key}</Key><LastModified>2026-10-11T13:38:17.000Z</LastModified>\
             <ETag>&quot;e-{key}&quot;</ETag><Size>7</Size><StorageClass>STANDARD</StorageClass>\
             </Contents>"
        )
    }

    fn page(keys: &[&str], token: Option<&str>) -> String {
        let body: String = keys.iter().map(|k| contents(k)).collect();
        let token = token.map_or_else(String::new, |t| {
            format!("<NextContinuationToken>{t}</NextContinuationToken>")
        });
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult \
             xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>probe</Name>\
             <Prefix></Prefix><KeyCount>{}</KeyCount><MaxKeys>1000</MaxKeys>\
             <IsTruncated>{}</IsTruncated>{body}{token}</ListBucketResult>",
            keys.len(),
            !token.is_empty(),
        )
    }

    /// The keys `object_store` rejects or rewrites are unaddressable; keys it
    /// escapes on the wire are not.
    #[test]
    fn addressable_matches_object_store() {
        for key in [
            "d//dbl.ndjson",
            "c-dot/./dot.ndjson",
            "c-dotdot/../up.ndjson",
            "c-tab/tab\tx.ndjson",
            "c-soh/x\u{1}y.ndjson",
            "c-del/x\u{7f}y.ndjson",
            "/data/lead.ndjson",
            "dir/",
            "/",
        ] {
            assert!(!addressable(key), "{key:?}");
        }
        for key in ["a.ndjson", "p/q%25r.ndjson", "p/é #[x].ndjson", "a.b/..c"] {
            assert!(addressable(key), "{key:?}");
        }
    }

    /// Only the elements naming unaddressable keys leave the page; the
    /// continuation token and every other byte stay. Regression for #1082
    /// and #1116.
    #[test]
    fn filter_removes_only_unaddressable_elements() {
        let input = page(
            &["a.ndjson", "d//dbl.ndjson", "/lead.ndjson", "z.ndjson"],
            Some("tok&amp;1"),
        );
        let (filtered, removed) = filter_page(input.as_bytes()).expect("two keys removed");
        assert_eq!(
            String::from_utf8(filtered).unwrap(),
            input
                .replace(&contents("d//dbl.ndjson"), "")
                .replace(&contents("/lead.ndjson"), "")
        );
        assert_eq!(
            removed,
            [
                ObjectEntry {
                    key: "d//dbl.ndjson".into(),
                    size: 7,
                    etag: Some("\"e-d//dbl.ndjson\"".into()),
                    last_modified_ms: 1_791_725_897_000,
                },
                ObjectEntry {
                    key: "/lead.ndjson".into(),
                    size: 7,
                    etag: Some("\"e-/lead.ndjson\"".into()),
                    last_modified_ms: 1_791_725_897_000,
                },
            ]
        );
    }

    /// A key reaches the predicate unescaped, character references included.
    #[test]
    fn escaped_keys_are_tested_unescaped() {
        let input = page(&["c-soh/x&#x1;y.ndjson", "a&amp;b.ndjson"], None);
        let (_, removed) = filter_page(input.as_bytes()).expect("one key removed");
        let keys: Vec<_> = removed.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, ["c-soh/x\u{1}y.ndjson"]);
    }

    /// A response to a request that is not a `ListObjectsV2` listing passes
    /// through unchanged, even when its body reads as a listing page.
    #[test]
    fn an_object_read_passes_through_unfiltered() {
        let body = page(&["d//dbl.ndjson"], None);
        let url = crate::test_servers::sequence_server(vec![("200 OK", body.clone())]);
        let keys = UnaddressableKeys::default();
        let client = UnaddressableConnector::new(keys.clone())
            .connect(&ClientOptions::new().with_allow_http(true))
            .unwrap();
        let mut request = HttpRequest::new(Vec::<u8>::new().into());
        *request.uri_mut() = format!("{url}/b/data/x.ndjson").parse().unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let read = rt.block_on(async {
            let response = client.execute(request).await.unwrap();
            response.into_body().bytes().await.unwrap()
        });
        assert_eq!(std::str::from_utf8(&read).unwrap(), body);
        assert!(keys.take().is_empty());
    }

    /// The filtered page keeps the other keys and the continuation token, and
    /// every key it keeps parses as a `Path`.
    #[test]
    fn filtered_page_lists_in_object_store() {
        let input = page(&["a.ndjson", "d//dbl.ndjson", "z.ndjson"], Some("t"));
        let (filtered, _) = filter_page(input.as_bytes()).unwrap();
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct Listed {
            #[serde(default)]
            contents: Vec<Key>,
            next_continuation_token: Option<String>,
        }
        #[derive(serde::Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct Key {
            key: String,
        }
        let listed: Listed =
            quick_xml::de::from_str(std::str::from_utf8(&filtered).unwrap()).unwrap();
        let keys: Vec<_> = listed.contents.iter().map(|c| c.key.as_str()).collect();
        assert_eq!(keys, ["a.ndjson", "z.ndjson"]);
        assert_eq!(listed.next_continuation_token.as_deref(), Some("t"));
        for key in keys {
            assert!(Path::parse(key).is_ok());
        }
    }

    proptest! {
        /// A page of addressable keys is left alone.
        #[test]
        fn addressable_pages_pass_through(
            keys in proptest::collection::vec("[a-z0-9]{1,8}(/[a-z0-9._-]{1,8}){0,3}", 0..20)
        ) {
            let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
            prop_assume!(keys.iter().all(|k| addressable(k)));
            prop_assert!(filter_page(page(&keys, Some("t")).as_bytes()).is_none());
        }
    }
}
