//! Test doubles for the SDK client's edges: an SQS endpoint that answers
//! every call in process, and a credentials provider whose loads follow a
//! script.

use crate::config::SqsFeedConfig;
use crate::queue::{BoxFuture, SdkQueue};
use aws_credential_types::Credentials;
use aws_credential_types::provider::{self, ProvideCredentials, SharedCredentialsProvider};
use aws_sdk_sqs::config::SharedHttpClient;
use aws_smithy_runtime_api::client::http::{
    HttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::watch;

/// The requests the stub answered, by kind.
#[derive(Debug, Default)]
pub(crate) struct Answered {
    pub(crate) extended: AtomicUsize,
    pub(crate) deleted: AtomicUsize,
}

/// A client whose every request `answer` handles in process. Paused tokio
/// time moves past a reply still crossing a socket, so a test that pauses
/// time answers through this.
pub(crate) fn in_process(
    answer: impl Fn(HttpRequest) -> HttpConnectorFuture + Send + Sync + 'static,
) -> SharedHttpClient {
    let connector = SharedHttpConnector::new(InProcess(Box::new(answer)));
    http_client_fn(move |_, _| connector.clone())
}

struct InProcess(Box<dyn Fn(HttpRequest) -> HttpConnectorFuture + Send + Sync>);

impl std::fmt::Debug for InProcess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InProcess")
    }
}

impl HttpConnector for InProcess {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        (self.0)(request)
    }
}

/// A JSON reply with `status` and `body`, and `headers` besides its
/// content type.
pub(crate) fn reply(
    status: u16,
    headers: &[(&'static str, &'static str)],
    body: String,
) -> HttpConnectorFuture {
    let mut response = HttpResponse::new(status.try_into().unwrap(), body.into());
    response
        .headers_mut()
        .insert("content-type", "application/x-amz-json-1.0");
    for (name, value) in headers {
        response.headers_mut().insert(*name, *value);
    }
    HttpConnectorFuture::ready(Ok(response))
}

/// An SQS endpoint: one message on the first receive and none after, and
/// every visibility change and delete accepted.
pub(crate) fn sqs_stub() -> (SharedHttpClient, Arc<Answered>) {
    let answered = Arc::new(Answered::default());
    let counts = Arc::clone(&answered);
    let received = AtomicUsize::new(0);
    let http = in_process(move |request| {
        let target = request
            .headers()
            .get("x-amz-target")
            .unwrap_or_default()
            .to_lowercase();
        let body: serde_json::Value = request
            .body()
            .bytes()
            .and_then(|b| serde_json::from_slice(b).ok())
            .unwrap_or(serde_json::Value::Null);
        let all_accepted = || {
            let ids: Vec<_> = body["Entries"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|e| serde_json::json!({"Id": e["Id"]}))
                .collect();
            serde_json::json!({"Successful": ids, "Failed": []}).to_string()
        };
        let body = match target.as_str() {
            "amazonsqs.receivemessage" => {
                if received.fetch_add(1, Ordering::SeqCst) == 0 {
                    r#"{"Messages":[{"MessageId":"m0","ReceiptHandle":"h0","Body":"b","Attributes":{"ApproximateReceiveCount":"1"}}]}"#.to_owned()
                } else {
                    r#"{"Messages":[]}"#.to_owned()
                }
            }
            "amazonsqs.changemessagevisibilitybatch" => {
                counts.extended.fetch_add(1, Ordering::SeqCst);
                all_accepted()
            }
            "amazonsqs.deletemessagebatch" => {
                counts.deleted.fetch_add(1, Ordering::SeqCst);
                all_accepted()
            }
            _ => "{}".to_owned(),
        };
        reply(200, &[], body)
    });
    (http, answered)
}

/// One load: given its index, the future the provider returns.
type Script = dyn Fn(usize) -> BoxFuture<'static, provider::Result> + Send + Sync;

/// A credentials provider whose `n`th load runs `script(n)`, counting the
/// loads it starts and finishes.
pub(crate) struct ScriptedCredentials {
    script: Box<Script>,
    fallback: Option<Credentials>,
    started: watch::Sender<usize>,
    finished: watch::Sender<usize>,
}

impl ScriptedCredentials {
    pub(crate) fn new(
        script: impl Fn(usize) -> BoxFuture<'static, provider::Result> + Send + Sync + 'static,
    ) -> Arc<Self> {
        Self::with_fallback(script, None)
    }

    pub(crate) fn with_fallback(
        script: impl Fn(usize) -> BoxFuture<'static, provider::Result> + Send + Sync + 'static,
        fallback: Option<Credentials>,
    ) -> Arc<Self> {
        Arc::new(Self {
            script: Box::new(script),
            fallback,
            started: watch::Sender::new(0),
            finished: watch::Sender::new(0),
        })
    }

    /// The first load answers at once with keys already inside the SDK's
    /// refresh buffer, so the next call loads again; each later load
    /// takes `slow` on tokio time and answers with keys that never expire.
    pub(crate) fn slow_reloads(slow: Duration) -> Arc<Self> {
        Self::new(move |n| {
            Box::pin(async move {
                if n == 0 {
                    return Ok(stale_keys());
                }
                tokio::time::sleep(slow).await;
                Ok(keys(None))
            })
        })
    }

    pub(crate) fn provider(self: &Arc<Self>) -> SharedCredentialsProvider {
        SharedCredentialsProvider::from(Arc::clone(self) as Arc<dyn ProvideCredentials>)
    }

    pub(crate) fn started(&self) -> usize {
        *self.started.borrow()
    }

    pub(crate) fn finished(&self) -> usize {
        *self.finished.borrow()
    }

    /// Waits until `n` loads have finished, for ten minutes of tokio time.
    pub(crate) async fn until_finished(&self, n: usize) {
        let mut seen = self.finished.subscribe();
        tokio::time::timeout(Duration::from_secs(600), seen.wait_for(|s| *s >= n))
            .await
            .unwrap_or_else(|_| panic!("{n} loads never finished"))
            .unwrap();
    }
}

impl std::fmt::Debug for ScriptedCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScriptedCredentials")
            .finish_non_exhaustive()
    }
}

impl ProvideCredentials for ScriptedCredentials {
    fn provide_credentials<'a>(&'a self) -> provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        let mut n = 0;
        self.started.send_modify(|s| {
            n = *s;
            *s += 1;
        });
        let load = (self.script)(n);
        provider::future::ProvideCredentials::new(async move {
            let result = load.await;
            self.finished.send_modify(|f| *f += 1);
            result
        })
    }

    fn fallback_on_interrupt(&self) -> Option<Credentials> {
        self.fallback.clone()
    }
}

/// Keys with `expiry`, whose secret and token tests look for in output.
pub(crate) fn keys(expiry: Option<SystemTime>) -> Credentials {
    Credentials::new(
        "AKIDSCRIPTED",
        "s3cr3t",
        Some("t0k3n".to_owned()),
        expiry,
        "scripted",
    )
}

/// Keys whose expiry is inside the SDK's refresh buffer at any clock.
pub(crate) fn stale_keys() -> Credentials {
    keys(Some(SystemTime::UNIX_EPOCH + Duration::from_secs(60)))
}

/// An [`SdkQueue`] over `http` that loads through `credentials`, at
/// `visibility`.
pub(crate) async fn queue_with(
    http: SharedHttpClient,
    visibility: Duration,
    credentials: SharedCredentialsProvider,
) -> (SdkQueue, SqsFeedConfig) {
    let endpoint = "http://sqs.test";
    let config = SqsFeedConfig {
        queue_url: format!("{endpoint}/000000000000/jobs"),
        visibility,
        region: Some("eu-west-2".to_owned()),
        endpoint: Some(endpoint.to_owned()),
    };
    let step = std::sync::Mutex::new("starting");
    let mut queue = crate::client::sdk_queue(&config, Some(credentials), &step)
        .await
        .unwrap();
    queue.client = aws_sdk_sqs::Client::from_conf(
        queue.client.config().to_builder().http_client(http).build(),
    );
    (queue, config)
}
