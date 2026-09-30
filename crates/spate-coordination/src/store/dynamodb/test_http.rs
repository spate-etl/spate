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
    /// A response head whose body never arrives.
    Stall,
    /// A 200 with this JSON body, sent after the delay.
    Delayed(Duration, &'static str),
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
            Reply::Delayed(delay, body) => {
                tokio::time::sleep(delay).await;
                (200, "application/x-amz-json-1.0", body.to_string())
            }
            Reply::Stall => {
                let _ = stream
                    .write_all(b"HTTP/1.1 200 X\r\ncontent-length: 1\r\n\r\n")
                    .await;
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
        roots: || native_certs(vec![]),
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
    SdkTable::new(&sdk, &settings)
}

fn table_over(endpoint: &str, http: SharedHttpClient) -> SdkTable {
    let (sdk, settings) = settings(endpoint, static_credentials());
    let sdk = sdk.into_builder().http_client(http).build();
    SdkTable::new(&sdk, &settings)
}

fn class(e: &StoreError) -> &'static str {
    match e {
        StoreError::Fatal(_) => "fatal",
        StoreError::Retryable(_) => "retryable",
    }
}

/// Each service answer classifies as ADR-0047 and the store's own rules
/// say: a rejected credential, signature or request, and a 401 or 403, is
/// Fatal after one request;
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
    let table = SdkTable::new(&sdk, &settings);
    let err = table.get("job#d", "k").await.unwrap_err();
    assert!(matches!(err, StoreError::Retryable(_)), "{err}");
    assert!(
        err.to_string()
            .contains("the instance metadata service is unreachable"),
        "{err}"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 0);
}

/// Names the endpoint for the child run of a test in
/// [`in_bare_environment`].
const BARE_TARGET: &str = "SPATE_TEST_DYNAMODB_BARE_TARGET";

const BARE_OP_TIMEOUT: Duration = Duration::from_secs(2);

/// Runs the test `name` of this module in a child whose environment holds
/// only `PATH`, `HOME` and the AWS config files at `dir`, instance metadata
/// at `metadata`, `BARE_TARGET` set to `target`, and `extra`.
fn in_bare_environment(
    name: &str,
    dir: &std::path::Path,
    target: &str,
    metadata: &str,
    extra: &[(&str, &str)],
) {
    let (_, module) = module_path!().split_once("::").unwrap();
    spate_test_support::run_in_child(&format!("{module}::{name}"), |child| {
        child
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("HOME", dir)
            .env("AWS_CONFIG_FILE", dir.join("config"))
            .env("AWS_SHARED_CREDENTIALS_FILE", dir.join("credentials"))
            .env("AWS_EC2_METADATA_SERVICE_ENDPOINT", metadata)
            .env(BARE_TARGET, target)
            .envs(extra.iter().copied());
        // Under a coverage run the child writes its profile where the
        // parent's does, or its coverage is lost.
        if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
            child.env("LLVM_PROFILE_FILE", profile);
        }
        child
    });
}

/// The servers a bare child talks to, on a runtime that outlives it.
struct Bare {
    rt: tokio::runtime::Runtime,
    table: String,
    table_hits: Arc<AtomicUsize>,
    metadata: String,
    metadata_hits: Arc<AtomicUsize>,
}

impl Bare {
    fn new(table: Script, metadata: Script) -> Bare {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let (table, table_hits) = rt.block_on(serve(table));
        let (metadata, metadata_hits) = rt.block_on(serve(metadata));
        Bare {
            rt,
            table,
            table_hits,
            metadata,
            metadata_hits,
        }
    }

    /// Serves `script` on the same runtime; returns its URL and hit count.
    fn serve(&self, script: Script) -> (String, Arc<AtomicUsize>) {
        self.rt.block_on(serve(script))
    }

    fn run(&self, name: &str, dir: &std::path::Path, extra: &[(&str, &str)]) {
        in_bare_environment(name, dir, &self.table, &self.metadata, extra);
    }
}

/// A store at `target` that takes its credentials from the AWS provider
/// chain and trusts only the bundled roots.
fn bare_store(target: &str) -> DynamoDbStore {
    let (_, settings) = settings_within(target, static_credentials(), BARE_OP_TIMEOUT);
    let settings = Arc::new(settings);
    DynamoDbStore::build(
        DynamoDbConfig::new("spate-test", "job"),
        Duration::from_secs(10),
        BARE_OP_TIMEOUT,
        TestClock::frozen(),
        Box::new(|| 1),
        Box::new(move || {
            let settings = settings.clone();
            Box::pin(async move { super::sdk::connect(&settings).await })
        }),
    )
    .unwrap()
}

fn ttl_on() -> Reply {
    Reply::Json(
        200,
        r#"{"TimeToLiveDescription":{"TimeToLiveStatus":"ENABLED","AttributeName":"x"}}"#.into(),
    )
}

fn not_called() -> Reply {
    Reply::Json(500, error_body("InternalServerError"))
}

/// Serves a startup and absent reads, and instance metadata answering
/// `metadata`, then runs the test `name` in [`in_bare_environment`] with
/// `dir`, and returns how many requests reached the table.
fn serve_to_bare_child(name: &str, dir: &std::path::Path, metadata: fn() -> Reply) -> usize {
    let updates = Arc::new(AtomicUsize::new(0));
    let bare = Bare::new(
        startup_script(ttl_on, not_called, updates),
        always(metadata),
    );
    bare.run(name, dir, &[]);
    bare.table_hits.load(Ordering::SeqCst)
}

/// A `credential_process` that takes a second loads, and the first
/// operation succeeds, with an `op_timeout` of two seconds.
#[test]
fn a_slow_credential_process_loads_inside_op_timeout() {
    if let Ok(target) = std::env::var(BARE_TARGET) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let store = bare_store(&target);
        let got = rt.block_on(store.get(Keyspace::Durable, "k"));
        assert!(matches!(got, Ok(None)), "{got:?}");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("credentials.sh");
    std::fs::write(
        &script,
        "sleep 1\necho '{\"Version\":1,\"AccessKeyId\":\"test\",\"SecretAccessKey\":\"test\"}'\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("config"),
        format!("[default]\ncredential_process = sh {}\n", script.display()),
    )
    .unwrap();
    let hits = serve_to_bare_child(
        "a_slow_credential_process_loads_inside_op_timeout",
        dir.path(),
        || Reply::Hang,
    );
    assert!(hits > 0);
}

/// A provider chain with no source reports its reason as a Retryable
/// error inside `op_timeout`.
#[test]
fn a_chain_with_no_source_reports_why() {
    if let Ok(target) = std::env::var(BARE_TARGET) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let store = bare_store(&target);
        let err = rt.block_on(store.get(Keyspace::Durable, "k")).unwrap_err();
        assert!(matches!(err, StoreError::Retryable(_)), "{err}");
        assert!(
            err.to_string().contains("no credentials found in chain"),
            "{err}"
        );
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let hits = serve_to_bare_child("a_chain_with_no_source_reports_why", dir.path(), || {
        Reply::Hang
    });
    assert_eq!(hits, 0);
}

/// A startup check the coordinator's deadline cuts runs again on the next
/// operation over the same connection, and the `credential_process` the
/// first attempt ran is not run again.
#[test]
fn a_cut_startup_check_keeps_the_loaded_credentials() {
    if let Ok(target) = std::env::var(BARE_TARGET) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let store = crate::store::metered::Metered::new(bare_store(&target), BARE_OP_TIMEOUT, None);
        let first = rt.block_on(store.get(Keyspace::Durable, "k"));
        assert!(
            matches!(&first, Err(StoreError::Retryable(e)) if e.contains("timed out after")),
            "{first:?}"
        );
        let second = rt.block_on(store.get(Keyspace::Durable, "k"));
        assert!(matches!(second, Ok(None)), "{second:?}");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let script = dir.path().join("credentials.sh");
    std::fs::write(
        &script,
        format!(
            "echo spawn >> {}\necho '{{\"Version\":1,\"AccessKeyId\":\"test\",\"SecretAccessKey\":\"test\"}}'\n",
            dir.path().join("spawns").display()
        ),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("config"),
        format!("[default]\ncredential_process = sh {}\n", script.display()),
    )
    .unwrap();
    // The first attempt's `DescribeTable` takes a second and its
    // `DescribeTimeToLive` never finishes, so the deadline cuts the check
    // after the credentials loaded.
    let described = Arc::new(AtomicUsize::new(0));
    let ttl_described = Arc::new(AtomicUsize::new(0));
    let table: Script = Arc::new(move |request: &Request| match request.op.as_str() {
        "describetable" if described.fetch_add(1, Ordering::SeqCst) == 0 => {
            Reply::Delayed(Duration::from_secs(1), ACTIVE_TABLE)
        }
        "describetable" => Reply::Json(200, ACTIVE_TABLE.into()),
        "describetimetolive" if ttl_described.fetch_add(1, Ordering::SeqCst) == 0 => Reply::Stall,
        "describetimetolive" => ttl_on(),
        _ => Reply::Json(200, "{}".into()),
    });
    let bare = Bare::new(table, always(|| Reply::Hang));
    bare.run(
        "a_cut_startup_check_keeps_the_loaded_credentials",
        dir.path(),
        &[],
    );
    let spawns = std::fs::read_to_string(dir.path().join("spawns")).unwrap();
    assert_eq!(spawns.lines().count(), 1, "credential_process runs");
}

/// Every startup attempt against a malformed shared config file names the
/// parse error, and a file fixed between attempts is read on the next one.
#[test]
fn a_malformed_config_is_named_on_every_attempt_until_fixed() {
    if let Ok(target) = std::env::var(BARE_TARGET) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let store = bare_store(&target);
        for attempt in 1..=2 {
            let got = rt.block_on(store.get(Keyspace::Durable, "k"));
            assert!(
                matches!(&got, Err(StoreError::Retryable(e)) if e.contains("could not parse profile file")),
                "attempt {attempt}: {got:?}"
            );
        }
        let config = std::env::var("AWS_CONFIG_FILE").unwrap();
        let credentials = std::env::var("AWS_SHARED_CREDENTIALS_FILE").unwrap();
        std::fs::write(config, "[default]\n").unwrap();
        std::fs::write(
            credentials,
            "[default]\naws_access_key_id = test\naws_secret_access_key = test\n",
        )
        .unwrap();
        let got = rt.block_on(store.get(Keyspace::Durable, "k"));
        assert!(matches!(got, Ok(None)), "after the fix: {got:?}");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("config"),
        "[default\ncredential_process = \n",
    )
    .unwrap();
    let updates = Arc::new(AtomicUsize::new(0));
    let bare = Bare::new(
        startup_script(ttl_on, not_called, updates),
        always(|| Reply::Json(404, String::new())),
    );
    bare.run(
        "a_malformed_config_is_named_on_every_attempt_until_fixed",
        dir.path(),
        &[],
    );
}

/// With no region configured and a provider chain that names none,
/// connecting fails fatally before any request, naming the setting.
#[test]
fn a_missing_region_is_fatal_at_startup() {
    if let Ok(target) = std::env::var(BARE_TARGET) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (_, mut settings) = settings_within(&target, static_credentials(), BARE_OP_TIMEOUT);
        settings.region = None;
        let err = rt.block_on(super::sdk::connect(&settings)).unwrap_err();
        assert!(matches!(err, StoreError::Fatal(_)), "{err}");
        assert!(err.to_string().contains("dynamodb.region"), "{err}");
        assert!(!err.to_string().contains("timed out"), "{err}");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let hits = serve_to_bare_child("a_missing_region_is_fatal_at_startup", dir.path(), || {
        Reply::Json(404, String::new())
    });
    assert_eq!(hits, 0);
}

/// The region lookup reaches instance metadata through the proxy
/// `HTTP_PROXY` names, over the store's own client.
#[test]
fn the_region_lookup_goes_through_the_proxy_the_environment_names() {
    if let Ok(target) = std::env::var(BARE_TARGET) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (_, mut settings) = settings_within(&target, static_credentials(), BARE_OP_TIMEOUT);
        settings.region = None;
        let err = rt.block_on(super::sdk::connect(&settings)).unwrap_err();
        assert!(err.to_string().contains("dynamodb.region"), "{err}");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let bare = Bare::new(
        always(not_called),
        always(|| Reply::Json(404, String::new())),
    );
    let (proxy, proxied) = bare.serve(always(|| Reply::Json(404, String::new())));
    bare.run(
        "the_region_lookup_goes_through_the_proxy_the_environment_names",
        dir.path(),
        &[("HTTP_PROXY", &proxy)],
    );
    let hits = (
        proxied.load(Ordering::SeqCst),
        bare.metadata_hits.load(Ordering::SeqCst),
    );
    assert!(
        hits.0 > 0 && hits.1 == 0,
        "(proxy, metadata) requests: {hits:?}"
    );
}

/// With no region configured and instance metadata that never finishes
/// an answer, connecting fails fatally inside `op_timeout`, naming the
/// setting.
#[test]
fn a_region_lookup_that_hangs_is_fatal_inside_op_timeout() {
    if let Ok(target) = std::env::var(BARE_TARGET) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (_, mut settings) = settings_within(&target, static_credentials(), BARE_OP_TIMEOUT);
        settings.region = None;
        let (err, elapsed) = rt.block_on(async {
            let started = tokio::time::Instant::now();
            let err = super::sdk::connect(&settings).await.unwrap_err();
            (err, started.elapsed())
        });
        assert!(matches!(err, StoreError::Fatal(_)), "{err}");
        assert!(err.to_string().contains("dynamodb.region"), "{err}");
        assert!(err.to_string().contains("timed out after 500ms"), "{err}");
        assert!(elapsed < BARE_OP_TIMEOUT, "Fatal after {elapsed:?}: {err}");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let hits = serve_to_bare_child(
        "a_region_lookup_that_hangs_is_fatal_inside_op_timeout",
        dir.path(),
        || Reply::Stall,
    );
    assert_eq!(hits, 0);
}

/// Against a server that never answers, every call returns Retryable
/// before `op_timeout`, the deadline the coordinator puts on it.
#[tokio::test(start_paused = true)]
async fn the_sdk_timeout_fires_inside_op_timeout() {
    let op_timeout = Duration::from_secs(2);
    let (url, _) = serve(always(|| Reply::Hang)).await;
    let (sdk, settings) = settings_within(&url, static_credentials(), op_timeout);
    let table = SdkTable::new(&sdk, &settings);
    // Concurrent, since one call can finish in time by the luck of its
    // retry backoff.
    let calls = (0..16).map(|_| async {
        let started = tokio::time::Instant::now();
        let err = table.get("job#d", "k").await.unwrap_err();
        (err, started.elapsed())
    });
    for (err, elapsed) in futures_util::future::join_all(calls).await {
        assert!(matches!(err, StoreError::Retryable(_)), "{err}");
        assert!(elapsed < op_timeout, "returned after {elapsed:?}: {err}");
    }
}

/// An attempt that gets no response head ends early enough for a retry to
/// succeed inside `op_timeout`.
#[tokio::test]
async fn a_hung_attempt_is_retried_inside_op_timeout() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let counted = attempts.clone();
    let (url, _) = serve(Arc::new(move |_: &Request| {
        if counted.fetch_add(1, Ordering::SeqCst) == 0 {
            Reply::Hang
        } else {
            Reply::Json(200, "{}".into())
        }
    }))
    .await;
    let (sdk, settings) = settings_within(&url, static_credentials(), Duration::from_secs(4));
    let table = SdkTable::new(&sdk, &settings);
    assert_eq!(table.get("job#d", "k").await.unwrap(), None);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
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

/// A `DescribeTable` answer for a table the store can use.
const ACTIVE_TABLE: &str = r#"{"Table":{"TableStatus":"ACTIVE",
    "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                 {"AttributeName":"sk","KeyType":"RANGE"}],
    "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},
                            {"AttributeName":"sk","AttributeType":"S"}]}}"#;

/// A store over `table` with a frozen clock and wall.
fn store_over(table: SdkTable) -> DynamoDbStore {
    store_with(table, DynamoDbConfig::new("spate-test", "job"))
}

fn store_with(table: SdkTable, config: DynamoDbConfig) -> DynamoDbStore {
    let table: Arc<dyn Table> = Arc::new(table);
    DynamoDbStore::build(
        config,
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
        "describetable" => Reply::Json(200, ACTIVE_TABLE.into()),
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

/// Answers every request with `reply` and keeps each request body.
fn recorded(reply: &'static str) -> (Script, Arc<Mutex<Vec<serde_json::Value>>>) {
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let kept = bodies.clone();
    let script: Script = Arc::new(move |request: &Request| {
        kept.lock().unwrap().push(request.body.clone());
        Reply::Json(200, reply.into())
    });
    (script, bodies)
}

/// The attributes an `UpdateItem` body's expression sets, with their
/// values, and the ones it removes, by attribute name.
fn assigned(body: &serde_json::Value) -> (HashMap<String, serde_json::Value>, Vec<String>) {
    let name = |token: &str| {
        let token = token.trim();
        body["ExpressionAttributeNames"][token]
            .as_str()
            .unwrap_or(token)
            .to_string()
    };
    let expr = body["UpdateExpression"]
        .as_str()
        .expect("an update expression");
    let (set, remove) = expr.split_once(" REMOVE ").unwrap_or((expr, ""));
    let set = set.strip_prefix("SET ").expect("a SET clause");
    let mut clauses = Vec::new();
    let (mut depth, mut start) = (0, 0);
    for (i, c) in set.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                clauses.push(&set[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    clauses.push(&set[start..]);
    let sets = clauses
        .into_iter()
        .map(|clause| {
            let (target, value) = clause.split_once('=').expect("an assignment");
            let value = value.trim();
            let value = body["ExpressionAttributeValues"]
                .get(value)
                .cloned()
                .unwrap_or_else(|| value.into());
            (name(target), value)
        })
        .collect();
    let removed = remove
        .split(',')
        .filter(|t| !t.trim().is_empty())
        .map(name)
        .collect();
    (sets, removed)
}

/// Every write stores its write id in `w`, which `decode` reads, and the
/// collection time in `x`, which the table's TTL reads. A durable create
/// clears the `x` of a tombstone it replaces.
#[tokio::test]
async fn writes_store_the_write_id_and_the_collection_time() {
    use base64::Engine as _;
    let (script, bodies) = recorded(r#"{"Attributes":{"v":{"N":"1"}}}"#);
    let (url, _) = serve(script).await;
    let table = table_at(&url);
    let w: WriteId = [7; 16];
    let id = serde_json::json!({ "B": base64::engine::general_purpose::STANDARD.encode(w) });
    let b = b"v".to_vec();
    let writes = [
        Write::CreateDurable {
            b: b.clone(),
            w,
            now_ms: 5,
        },
        Write::Put {
            v: 2,
            b: b.clone(),
            w,
            x: Some(9),
            cond: Cond::Absent,
        },
        Write::Put {
            v: 3,
            b,
            w,
            x: None,
            cond: Cond::LiveVersionIs(2),
        },
        Write::Tombstone {
            expected: Some(3),
            w,
            x: 11,
        },
    ];
    for write in writes {
        table.write("job#d", "k", write).await.unwrap();
    }
    let bodies = bodies.lock().unwrap();
    let [create, leased, durable, tombstone] = bodies.as_slice() else {
        panic!("{bodies:?}");
    };

    let (sets, removed) = assigned(create);
    assert_eq!(sets.get("w"), Some(&id), "{create}");
    assert!(removed.iter().any(|r| r == "t"), "{create}");
    assert!(removed.iter().any(|r| r == "x"), "{create}");

    let (sets, _) = assigned(leased);
    assert_eq!(sets.get("w"), Some(&id), "{leased}");
    assert_eq!(
        sets.get("x"),
        Some(&serde_json::json!({ "N": "9" })),
        "{leased}"
    );

    let (sets, _) = assigned(durable);
    assert_eq!(sets.get("w"), Some(&id), "{durable}");
    assert!(!sets.contains_key("x"), "{durable}");

    let (sets, _) = assigned(tombstone);
    assert_eq!(sets.get("w"), Some(&id), "{tombstone}");
    assert_eq!(
        sets.get("x"),
        Some(&serde_json::json!({ "N": "11" })),
        "{tombstone}"
    );
    assert_eq!(
        sets.get("t"),
        Some(&serde_json::json!({ "BOOL": true })),
        "{tombstone}"
    );
}

/// A point read is strongly consistent, and a query asks for the
/// consistency its caller chose.
#[tokio::test]
async fn reads_ask_for_the_consistency_their_caller_needs() {
    let (script, bodies) = recorded("{}");
    let (url, _) = serve(script).await;
    let table = table_at(&url);
    table.get("job#d", "k").await.unwrap();
    for consistent in [true, false] {
        table
            .query(Query {
                pk: "job#e".into(),
                prefix: None,
                consistent,
                start: None,
                filter_tombs: false,
            })
            .await
            .unwrap();
    }
    let bodies = bodies.lock().unwrap();
    let read = |i: usize| bodies[i]["ConsistentRead"].as_bool().unwrap_or(false);
    assert_eq!(
        (read(0), read(1), read(2)),
        (true, true, false),
        "{bodies:?}"
    );
}

/// A listing longer than one page reads every page, each starting after
/// the key the last one ended on.
#[tokio::test]
async fn a_listing_reads_every_page() {
    const KEYS: [&str; 5] = ["a", "b", "c", "d", "e"];
    let queries = Arc::new(AtomicUsize::new(0));
    let counted = queries.clone();
    let script: Script = Arc::new(move |request: &Request| match request.op.as_str() {
        "describetable" => Reply::Json(200, ACTIVE_TABLE.into()),
        "describetimetolive" => ttl_on(),
        "query" => {
            // Ends a listing that never advances.
            if counted.fetch_add(1, Ordering::SeqCst) >= 10 {
                return Reply::Json(400, error_body("ValidationException"));
            }
            let after = request.body["ExclusiveStartKey"]["sk"]["S"].as_str();
            let page: Vec<&str> = KEYS
                .into_iter()
                .filter(|k| after.is_none_or(|a| *k > a))
                .take(2)
                .collect();
            let items: Vec<serde_json::Value> = page
                .iter()
                .map(|k| {
                    serde_json::json!({
                        "pk": { "S": "job#d" }, "sk": { "S": k },
                        "v": { "N": "1" }, "b": { "B": "dg==" },
                    })
                })
                .collect();
            let mut out = serde_json::json!({ "Items": items });
            if let Some(last) = page.last().filter(|k| **k != KEYS[KEYS.len() - 1]) {
                out["LastEvaluatedKey"] = serde_json::json!({
                    "pk": { "S": "job#d" }, "sk": { "S": last },
                });
            }
            Reply::Json(200, out.to_string())
        }
        _ => Reply::Json(200, "{}".into()),
    });
    let (url, _) = serve(script).await;
    let store = store_over(table_at(&url));
    let listed: Vec<String> = store
        .list(Keyspace::Durable, "")
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.key)
        .collect();
    assert_eq!(listed, KEYS);
    assert_eq!(queries.load(Ordering::SeqCst), 3);
}

/// Names the endpoint for the child run of
/// [`requests_go_through_the_proxy_the_environment_names`].
const PROXY_TARGET: &str = "SPATE_TEST_DYNAMODB_PROXY_TARGET";

/// A request goes through the proxy `HTTP_PROXY` names. The client runs in a
/// child process, so the variable reaches no other test.
#[tokio::test(flavor = "multi_thread")]
async fn requests_go_through_the_proxy_the_environment_names() {
    if let Ok(target) = std::env::var(PROXY_TARGET) {
        let http = http_client(|| native_certs(vec![])).await.unwrap();
        let found = table_over(&target, http).get("job#d", "k").await.unwrap();
        assert_eq!(found, None);
        return;
    }
    let (proxy, proxy_hits) = serve(always(|| Reply::Json(200, "{}".into()))).await;
    let (target, target_hits) = serve(always(|| Reply::Json(200, "{}".into()))).await;
    let (_, module) = module_path!().split_once("::").unwrap();
    let name = format!("{module}::requests_go_through_the_proxy_the_environment_names");
    tokio::task::block_in_place(|| {
        spate_test_support::run_in_child(&name, |child| {
            child
                .env_remove("http_proxy")
                .env_remove("NO_PROXY")
                .env_remove("no_proxy")
                .env("HTTP_PROXY", &proxy)
                .env(PROXY_TARGET, &target)
        });
    });
    let hits = (
        proxy_hits.load(Ordering::SeqCst),
        target_hits.load(Ordering::SeqCst),
    );
    assert_eq!(hits, (1, 0), "(proxy, endpoint) requests");
}

/// An HTTP proxy that answers `CONNECT` by tunnelling to the named host;
/// returns its URL and a count of the tunnels it opened.
async fn serve_connect_proxy() -> (String, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let tunnels = Arc::new(AtomicUsize::new(0));
    let counted = tunnels.clone();
    tokio::spawn(async move {
        while let Ok((mut client, _)) = listener.accept().await {
            let counted = counted.clone();
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                while !head.ends_with(b"\r\n\r\n") {
                    if client.read(&mut byte).await.ok()? == 0 {
                        return None;
                    }
                    head.push(byte[0]);
                }
                let head = String::from_utf8_lossy(&head).to_string();
                let authority = head
                    .strip_prefix("CONNECT ")?
                    .split(' ')
                    .next()?
                    .to_string();
                let mut upstream = tokio::net::TcpStream::connect(authority).await.ok()?;
                counted.fetch_add(1, Ordering::SeqCst);
                client
                    .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                    .await
                    .ok()?;
                tokio::io::copy_bidirectional(&mut client, &mut upstream)
                    .await
                    .ok();
                Some(())
            });
        }
    });
    (url, tunnels)
}

/// Name the endpoint and the root it is signed by for the child run of
/// [`https_requests_tunnel_through_the_proxy_and_keep_the_roots`].
const TLS_PROXY_TARGET: &str = "SPATE_TEST_DYNAMODB_TLS_PROXY_TARGET";
const TLS_PROXY_ROOT: &str = "SPATE_TEST_DYNAMODB_TLS_PROXY_ROOT";

/// A request to an HTTPS endpoint tunnels through the proxy `HTTPS_PROXY`
/// or `ALL_PROXY` names and verifies the endpoint against the store's
/// roots, and `NO_PROXY` sends it direct.
#[tokio::test(flavor = "multi_thread")]
async fn https_requests_tunnel_through_the_proxy_and_keep_the_roots() {
    use base64::Engine as _;
    if let (Ok(target), Ok(root)) = (
        std::env::var(TLS_PROXY_TARGET),
        std::env::var(TLS_PROXY_ROOT),
    ) {
        let der = base64::engine::general_purpose::STANDARD
            .decode(root)
            .unwrap();
        let root = rustls::pki_types::CertificateDer::from(der);
        let http = http_client(move || native_certs(vec![root])).await.unwrap();
        let found = table_over(&target, http).get("job#d", "k").await.unwrap();
        assert_eq!(found, None);
        return;
    }
    let ca = TestCa::new("dynamodb-proxy-ca");
    let target_hits = Arc::new(AtomicUsize::new(0));
    let counted = target_hits.clone();
    let addr = serve_tls(ca.server_config(None), move |tls| {
        answer(
            tls,
            always(|| Reply::Json(200, "{}".into())),
            counted.clone(),
        )
    })
    .await;
    let target = format!("https://{addr}");
    let (proxy, tunnels) = serve_connect_proxy().await;
    let root = base64::engine::general_purpose::STANDARD.encode(ca.der().as_ref());
    let (_, module) = module_path!().split_once("::").unwrap();
    let name = format!("{module}::https_requests_tunnel_through_the_proxy_and_keep_the_roots");
    let run = |proxy_var: &str, no_proxy: Option<&str>| {
        tokio::task::block_in_place(|| {
            spate_test_support::run_in_child(&name, |child| {
                for var in [
                    "HTTP_PROXY",
                    "http_proxy",
                    "HTTPS_PROXY",
                    "https_proxy",
                    "ALL_PROXY",
                    "all_proxy",
                    "NO_PROXY",
                    "no_proxy",
                ] {
                    child.env_remove(var);
                }
                if let Some(hosts) = no_proxy {
                    child.env("NO_PROXY", hosts);
                }
                child
                    .env(proxy_var, &proxy)
                    .env(TLS_PROXY_TARGET, &target)
                    .env(TLS_PROXY_ROOT, &root)
            });
        });
        (
            tunnels.load(Ordering::SeqCst),
            target_hits.load(Ordering::SeqCst),
        )
    };
    assert_eq!(
        run("HTTPS_PROXY", None),
        (1, 1),
        "(tunnels, endpoint requests) through HTTPS_PROXY"
    );
    assert_eq!(
        run("HTTPS_PROXY", Some("127.0.0.1")),
        (1, 2),
        "(tunnels, endpoint requests) under NO_PROXY"
    );
    assert_eq!(
        run("ALL_PROXY", None),
        (2, 3),
        "(tunnels, endpoint requests) through ALL_PROXY"
    );
}

/// Answers a store's startup and a `GetItem` of an absent key, with the time
/// to live answers `describe_ttl` and `update_ttl` give, counting
/// `UpdateTimeToLive` calls in `updates`.
fn startup_script(
    describe_ttl: fn() -> Reply,
    update_ttl: fn() -> Reply,
    updates: Arc<AtomicUsize>,
) -> Script {
    Arc::new(move |request: &Request| match request.op.as_str() {
        "describetable" => Reply::Json(200, ACTIVE_TABLE.into()),
        "describetimetolive" => describe_ttl(),
        "updatetimetolive" => {
            updates.fetch_add(1, Ordering::SeqCst);
            update_ttl()
        }
        _ => Reply::Json(200, "{}".into()),
    })
}

/// Runs `f` on a runtime of its own and returns the WARN lines it logged.
fn warnings(f: impl AsyncFnOnce()) -> Vec<String> {
    spate_test::capture_logs(tracing::Level::WARN, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f());
    })
}

/// A policy that denies `DescribeTimeToLive` gets a warning, and the store
/// starts.
#[test]
fn a_denied_ttl_describe_warns_and_the_store_starts() {
    let lines = warnings(async || {
        let updates = Arc::new(AtomicUsize::new(0));
        let denied = || Reply::Json(400, error_body("AccessDeniedException"));
        let never = || Reply::Json(500, error_body("InternalServerError"));
        let (url, _) = serve(startup_script(denied, never, updates)).await;
        let store = store_over(table_at(&url));
        assert_eq!(store.get(Keyspace::Durable, "k").await.unwrap(), None);
    });
    assert!(
        lines.iter().any(|l| l.contains("time to live")),
        "{lines:?}"
    );
}

/// A worker whose `UpdateTimeToLive` meets TTL another worker enabled first
/// starts.
#[tokio::test]
async fn enabling_ttl_another_worker_enabled_starts_the_store() {
    let updates = Arc::new(AtomicUsize::new(0));
    let off = || {
        Reply::Json(
            200,
            r#"{"TimeToLiveDescription":{"TimeToLiveStatus":"DISABLED"}}"#.into(),
        )
    };
    let already = || {
        Reply::Json(
            400,
            r#"{"__type":"com.amazon.coral.validate#ValidationException",
                "message":"TimeToLive is already enabled"}"#
                .into(),
        )
    };
    let (url, _) = serve(startup_script(off, already, updates.clone())).await;
    let mut config = DynamoDbConfig::new("spate-test", "job");
    config.create_table = true;
    let store = store_with(table_at(&url), config);
    assert_eq!(store.get(Keyspace::Durable, "k").await.unwrap(), None);
    assert_eq!(updates.load(Ordering::SeqCst), 1);
}

/// An `UpdateTimeToLive` refused inside the service's change window, on a
/// table whose TTL is being disabled, logs the reason at WARN, and the
/// store starts.
#[test]
fn enabling_ttl_inside_the_change_window_starts_the_store() {
    let lines = warnings(async || {
        let updates = Arc::new(AtomicUsize::new(0));
        let disabling = || {
            Reply::Json(
                200,
                r#"{"TimeToLiveDescription":{"TimeToLiveStatus":"DISABLING"}}"#.into(),
            )
        };
        let window = || {
            Reply::Json(
                400,
                r#"{"__type":"com.amazon.coral.validate#ValidationException",
                    "message":"Time to live has been modified multiple times within a fixed interval"}"#
                    .into(),
            )
        };
        let (url, _) = serve(startup_script(disabling, window, updates.clone())).await;
        let mut config = DynamoDbConfig::new("spate-test", "job");
        config.create_table = true;
        let store = store_with(table_at(&url), config);
        let got = store.get(Keyspace::Durable, "k").await;
        assert_eq!(updates.load(Ordering::SeqCst), 1);
        assert!(matches!(got, Ok(None)), "{got:?}");
    });
    assert!(
        lines
            .iter()
            .any(|l| l.contains("time to live") && l.contains("modified multiple times")),
        "{lines:?}"
    );
}

/// A retryable `UpdateTimeToLive` failure fails the startup attempt, so
/// the next one enables time to live again.
#[tokio::test]
async fn a_retryable_ttl_enable_failure_fails_the_startup_attempt() {
    let updates = Arc::new(AtomicUsize::new(0));
    let off = || {
        Reply::Json(
            200,
            r#"{"TimeToLiveDescription":{"TimeToLiveStatus":"DISABLED"}}"#.into(),
        )
    };
    let unknown = || Reply::Json(400, error_body("SomethingNewException"));
    let (url, _) = serve(startup_script(off, unknown, updates.clone())).await;
    let mut config = DynamoDbConfig::new("spate-test", "job");
    config.create_table = true;
    let store = store_with(table_at(&url), config);
    let got = store.get(Keyspace::Durable, "k").await;
    assert!(
        matches!(&got, Err(StoreError::Retryable(e)) if e.contains("UpdateTimeToLive")),
        "{got:?}"
    );
    assert_eq!(updates.load(Ordering::SeqCst), 1);
}

/// A `CreateTable` that meets a table another worker is creating adopts it.
#[tokio::test]
async fn create_table_adopts_a_table_another_worker_is_creating() {
    let (url, _) = serve(always(|| {
        Reply::Json(400, error_body("ResourceInUseException"))
    }))
    .await;
    table_at(&url).create_table().await.unwrap();
}

/// `DescribeTable` reports replicas and a status the store cannot use.
#[tokio::test]
async fn describe_reports_replicas_and_an_unusable_status() {
    let (url, _) = serve(always(|| {
        Reply::Json(
            200,
            r#"{"Table":{"TableStatus":"DELETING",
                "KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},
                             {"AttributeName":"sk","KeyType":"RANGE"}],
                "AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},
                                        {"AttributeName":"sk","AttributeType":"S"}],
                "Replicas":[{"RegionName":"eu-west-2","ReplicaStatus":"ACTIVE"}]}}"#
                .into(),
        )
    }))
    .await;
    let shape = table_at(&url).describe().await.unwrap().expect("a table");
    assert_eq!(shape.status, super::table::Status::Other("DELETING".into()));
    assert_eq!(shape.replicas, 1);
}

/// Falling back to the bundled roots is logged at WARN.
#[test]
fn an_empty_system_store_warns() {
    let lines = warnings(async || {
        http_client(|| native_certs(vec![])).await.unwrap();
    });
    assert!(
        lines.iter().any(|l| l.contains("Mozilla root bundle")),
        "{lines:?}"
    );
}
