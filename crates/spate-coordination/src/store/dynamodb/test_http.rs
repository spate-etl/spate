//! The SDK table against a local HTTP server that scripts each response:
//! error classification, timeouts, TLS and write-id resolution.

use super::sdk::{SdkTable, Settings, http_client, trust_roots};
use super::*;
use aws_config::SdkConfig;
use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::provider::future;
use aws_sdk_dynamodb::config::{
    BehaviorVersion, Credentials, ProvideCredentials, Region, SharedCredentialsProvider,
    SharedHttpClient,
};
use spate_core::clock::tokio::TestClock;
use spate_test_support::{TestCa, native_certs, serve_tls};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

const OP_TIMEOUT: Duration = Duration::from_secs(1);

/// One request the server read.
struct Request {
    /// The operation, from `X-Amz-Target`.
    op: String,
    body: serde_json::Value,
}

enum Reply {
    Json(u16, String),
    Raw(u16, &'static str, String),
    Hang,
}

type Script = Arc<dyn Fn(&Request) -> Reply + Send + Sync>;

async fn read_request<S: AsyncRead + Unpin>(stream: &mut S) -> Option<Request> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
            continue;
        };
        let head = String::from_utf8_lossy(&buf[..end]).to_lowercase();
        let header = |name: &str| {
            head.lines()
                .find_map(|l| l.strip_prefix(name).map(|v| v.trim().to_string()))
        };
        let len: usize = header("content-length:").map_or(0, |v| v.parse().unwrap());
        while buf.len() < end + 4 + len {
            let n = stream.read(&mut chunk).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        // The body keeps its case; only the head was lowered.
        let op = header("x-amz-target:")
            .and_then(|t| t.rsplit('.').next().map(str::to_string))
            .unwrap_or_default();
        let body = serde_json::from_slice(&buf[end + 4..end + 4 + len]).unwrap_or_default();
        return Some(Request { op, body });
    }
}

/// Answers each request on `stream` from `script`, counting them in `hits`.
async fn answer<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    script: Script,
    hits: Arc<AtomicUsize>,
) {
    while let Some(request) = read_request(&mut stream).await {
        hits.fetch_add(1, Ordering::SeqCst);
        let (status, content_type, body) = match script(&request) {
            Reply::Json(status, body) => (status, "application/x-amz-json-1.0", body),
            Reply::Raw(status, content_type, body) => (status, content_type, body),
            Reply::Hang => {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                return;
            }
        };
        let response = format!(
            "HTTP/1.1 {status} X\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        );
        if stream.write_all(response.as_bytes()).await.is_err() {
            return;
        }
    }
}

/// A plain HTTP server on `127.0.0.1` answering from `script`; returns its
/// URL and a count of the requests it read.
async fn serve(script: Script) -> (String, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let hits = Arc::new(AtomicUsize::new(0));
    let counted = hits.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(answer(stream, script.clone(), counted.clone()));
        }
    });
    (url, hits)
}

fn always(reply: impl Fn() -> Reply + Send + Sync + 'static) -> Script {
    Arc::new(move |_: &Request| reply())
}

fn error_body(code: &str) -> String {
    format!(r#"{{"__type":"com.amazonaws.dynamodb.v20120810#{code}","message":"scripted"}}"#)
}

fn settings(endpoint: &str, credentials: SharedCredentialsProvider) -> (SdkConfig, Settings) {
    settings_within(endpoint, credentials, OP_TIMEOUT)
}

fn settings_within(
    endpoint: &str,
    credentials: SharedCredentialsProvider,
    op_timeout: Duration,
) -> (SdkConfig, Settings) {
    let settings = Settings {
        table: "spate-test".into(),
        region: Some("eu-west-1".into()),
        endpoint: Some(endpoint.into()),
        op_timeout,
        credentials: None,
    };
    let sdk = SdkConfig::builder()
        .behavior_version(BehaviorVersion::v2026_01_12())
        .region(Region::new("eu-west-1"))
        .credentials_provider(credentials)
        .build();
    (sdk, settings)
}

fn static_credentials() -> SharedCredentialsProvider {
    SharedCredentialsProvider::new(Credentials::new("test", "test", None, None, "static"))
}

fn table_at(endpoint: &str) -> SdkTable {
    let (sdk, settings) = settings(endpoint, static_credentials());
    SdkTable::new(&sdk, &settings).expect("sdk table")
}

fn table_over(endpoint: &str, http: SharedHttpClient) -> SdkTable {
    let (sdk, settings) = settings(endpoint, static_credentials());
    let sdk = sdk.into_builder().http_client(http).build();
    SdkTable::new(&sdk, &settings).expect("sdk table")
}

fn class(e: &StoreError) -> &'static str {
    match e {
        StoreError::Fatal(_) => "fatal",
        StoreError::Retryable(_) => "retryable",
    }
}

/// Each service answer classifies as ADR-0047 says: a rejected credential,
/// signature or request, and any 401 or 403, is Fatal after one request;
/// throttling, server errors, conflicts and unknown answers are Retryable,
/// after the SDK's retries for the kinds it retries.
#[tokio::test]
async fn service_codes_classify_per_adr_0047() {
    let json = |status, code: &str| {
        let body = error_body(code);
        always(move || Reply::Json(status, body.clone()))
    };
    let cases: Vec<(&str, Script, &str, usize)> = vec![
        (
            "AccessDenied",
            json(400, "AccessDeniedException"),
            "fatal",
            1,
        ),
        (
            "UnrecognizedClient",
            json(400, "UnrecognizedClientException"),
            "fatal",
            1,
        ),
        (
            "InvalidSignature",
            json(400, "InvalidSignatureException"),
            "fatal",
            1,
        ),
        (
            "MissingAuthenticationToken",
            json(400, "MissingAuthenticationTokenException"),
            "fatal",
            1,
        ),
        (
            "IncompleteSignature",
            json(400, "IncompleteSignatureException"),
            "fatal",
            1,
        ),
        (
            "ExpiredToken",
            json(400, "ExpiredTokenException"),
            "fatal",
            1,
        ),
        (
            "ResourceNotFound",
            json(400, "ResourceNotFoundException"),
            "fatal",
            1,
        ),
        ("Validation", json(400, "ValidationException"), "fatal", 1),
        (
            "ItemCollectionSizeLimitExceeded",
            json(400, "ItemCollectionSizeLimitExceededException"),
            "fatal",
            1,
        ),
        (
            "403 without a code",
            always(|| Reply::Raw(403, "text/html", "<html>no</html>".into())),
            "fatal",
            1,
        ),
        (
            "401 without a code",
            always(|| Reply::Json(401, String::new())),
            "fatal",
            1,
        ),
        (
            "Throttling",
            json(400, "ThrottlingException"),
            "retryable",
            3,
        ),
        (
            "ProvisionedThroughputExceeded",
            json(400, "ProvisionedThroughputExceededException"),
            "retryable",
            3,
        ),
        (
            "RequestLimitExceeded",
            json(400, "RequestLimitExceeded"),
            "retryable",
            3,
        ),
        (
            "InternalServerError",
            json(500, "InternalServerError"),
            "retryable",
            3,
        ),
        (
            "503 without a code",
            always(|| Reply::Json(503, String::new())),
            "retryable",
            3,
        ),
        (
            "TransactionConflict",
            json(400, "TransactionConflictException"),
            "retryable",
            1,
        ),
        (
            "an unknown code",
            json(400, "SomethingNewException"),
            "retryable",
            1,
        ),
        (
            "an unparsable 200",
            always(|| Reply::Json(200, "not json".into())),
            "retryable",
            1,
        ),
    ];
    let runs = cases
        .into_iter()
        .map(|(name, script, want, requests)| async move {
            let (url, hits) = serve(script).await;
            let err = table_at(&url).get("job#d", "k").await.unwrap_err();
            let got = (class(&err), hits.load(Ordering::SeqCst));
            assert_eq!(got, (want, requests), "{name}: {err}");
        });
    futures_util::future::join_all(runs).await;
}

/// A credential provider that cannot load credentials.
#[derive(Debug)]
struct Unreachable;

impl ProvideCredentials for Unreachable {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        future::ProvideCredentials::ready(Err(CredentialsError::provider_error(
            "the instance metadata service is unreachable",
        )))
    }
}

/// A credential fetch that fails is Retryable, sends nothing, and carries
/// the provider's reason.
#[tokio::test]
async fn a_credential_fetch_failure_is_retryable() {
    let (url, hits) = serve(always(|| Reply::Json(200, "{}".into()))).await;
    let (sdk, settings) = settings(&url, SharedCredentialsProvider::new(Unreachable));
    let table = SdkTable::new(&sdk, &settings).unwrap();
    let err = table.get("job#d", "k").await.unwrap_err();
    assert!(matches!(err, StoreError::Retryable(_)), "{err}");
    assert!(
        err.to_string()
            .contains("the instance metadata service is unreachable"),
        "{err}"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 0);
}

/// Without a region the store fails fatally before any request, naming the
/// setting.
#[tokio::test]
async fn a_missing_region_is_fatal_at_startup() {
    let (url, hits) = serve(always(|| Reply::Json(200, "{}".into()))).await;
    let (_, settings) = settings(&url, static_credentials());
    let sdk = SdkConfig::builder()
        .behavior_version(BehaviorVersion::v2026_01_12())
        .credentials_provider(static_credentials())
        .build();
    let err = match SdkTable::new(&sdk, &settings) {
        Err(e) => e,
        Ok(table) => table.get("job#d", "k").await.unwrap_err(),
    };
    assert!(matches!(err, StoreError::Fatal(_)), "{err}");
    assert!(err.to_string().contains("dynamodb.region"), "{err}");
    assert_eq!(hits.load(Ordering::SeqCst), 0);
}

/// Against a server that never answers, every call returns Retryable
/// before `op_timeout`, the deadline the coordinator puts on it.
#[tokio::test]
async fn the_sdk_timeout_fires_inside_op_timeout() {
    let op_timeout = Duration::from_secs(2);
    let (url, _) = serve(always(|| Reply::Hang)).await;
    let (sdk, settings) = settings_within(&url, static_credentials(), op_timeout);
    let table = SdkTable::new(&sdk, &settings).unwrap();
    // Concurrent, since one call can finish in time by the luck of its
    // retry backoff.
    let calls = (0..16).map(|_| async {
        let started = std::time::Instant::now();
        let err = table.get("job#d", "k").await.unwrap_err();
        (err, started.elapsed())
    });
    for (err, elapsed) in futures_util::future::join_all(calls).await {
        assert!(matches!(err, StoreError::Retryable(_)), "{err}");
        assert!(elapsed < op_timeout, "returned after {elapsed:?}: {err}");
    }
}

/// A server whose certificate no trusted root signed is rejected fatally,
/// and one signed by a root in the system store is reached.
#[tokio::test]
async fn a_rejected_certificate_is_fatal() {
    let ca = TestCa::new("dynamodb-test-ca");
    let hits = Arc::new(AtomicUsize::new(0));
    let counted = hits.clone();
    let addr = serve_tls(ca.server_config(None), move |tls| {
        answer(
            tls,
            always(|| Reply::Json(200, "{}".into())),
            counted.clone(),
        )
    })
    .await;
    let url = format!("https://{addr}");

    let untrusted = http_client(|| native_certs(vec![])).await.unwrap();
    let err = table_over(&url, untrusted)
        .get("job#d", "k")
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Fatal(_)), "{err}");
    assert!(err.to_string().contains("UnknownIssuer"), "{err}");
    assert_eq!(hits.load(Ordering::SeqCst), 0);

    let root = ca.der();
    let trusted = http_client(move || native_certs(vec![root])).await.unwrap();
    let found = table_over(&url, trusted).get("job#d", "k").await.unwrap();
    assert_eq!(found, None);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

/// An empty system store falls back to the bundled roots, and a store with
/// a parsable certificate is used alone.
#[test]
fn an_empty_system_store_falls_back_to_the_bundled_roots() {
    let count = |pem: &str| pem.matches("-----BEGIN CERTIFICATE-----").count();
    let (roots, fallback) = trust_roots(native_certs(vec![]));
    assert!(fallback);
    assert_eq!(
        count(&roots),
        webpki_root_certs::TLS_SERVER_ROOT_CERTS.len()
    );

    let ca = TestCa::new("dynamodb-test-ca");
    let garbage = rustls::pki_types::CertificateDer::from(vec![1, 2, 3]);
    let (roots, fallback) = trust_roots(native_certs(vec![garbage, ca.der()]));
    assert!(!fallback);
    assert_eq!(count(&roots), 1);
}

/// A store over `table` with a frozen clock and wall.
fn store_over(table: SdkTable) -> DynamoDbStore {
    let table: Arc<dyn Table> = Arc::new(table);
    DynamoDbStore::build(
        DynamoDbConfig::new("spate-test", "job"),
        Duration::from_secs(10),
        OP_TIMEOUT,
        TestClock::frozen(),
        Box::new(|| 1),
        Box::new(move || {
            let table = table.clone();
            Box::pin(async move { Ok(table) })
        }),
    )
    .expect("store")
}

/// A write whose first attempt got a 500 and whose retry met the item that
/// attempt left resolves as won: the retry carries the same write id, and
/// the failed condition returns the item.
#[tokio::test]
async fn a_retried_write_whose_first_attempt_landed_wins() {
    let writes = Arc::new(AtomicUsize::new(0));
    let seen = writes.clone();
    let script: Script = Arc::new(move |request: &Request| match request.op.as_str() {
        "describetable" => Reply::Json(
            200,
            r#"{"Table":{"TableStatus":"ACTIVE",
                "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                             {"AttributeName":"sk","KeyType":"RANGE"}],
                "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},
                                        {"AttributeName":"sk","AttributeType":"S"}]}}"#
                .into(),
        ),
        "describetimetolive" => Reply::Json(
            200,
            r#"{"TimeToLiveDescription":{"TimeToLiveStatus":"ENABLED","AttributeName":"x"}}"#
                .into(),
        ),
        "updateitem" if request.body["Key"]["sk"]["S"] == "meta" => Reply::Json(200, "{}".into()),
        "updateitem" if seen.fetch_add(1, Ordering::SeqCst) == 0 => {
            Reply::Json(500, error_body("InternalServerError"))
        }
        "updateitem" if request.body["ReturnValuesOnConditionCheckFailure"] == "ALL_OLD" => {
            let w = &request.body["ExpressionAttributeValues"][":w"]["B"];
            Reply::Json(
                400,
                format!(
                    r#"{{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
                        "message":"The conditional request failed",
                        "Item":{{"v":{{"N":"6"}},"w":{{"B":{w}}}}}}}"#
                ),
            )
        }
        _ => Reply::Json(400, error_body("ConditionalCheckFailedException")),
    });
    let (url, _) = serve(script).await;
    let store = store_over(table_at(&url));
    let outcome = store
        .update(Keyspace::Durable, "split.a", b"v".to_vec(), Revision(5))
        .await
        .unwrap();
    assert_eq!(outcome, CasOutcome::Won(Revision(6)));
    assert_eq!(
        writes.load(Ordering::SeqCst),
        2,
        "one attempt and one retry"
    );
}
