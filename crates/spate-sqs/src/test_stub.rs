//! Test doubles for the SDK client's edges: an SQS endpoint that answers
//! every call, and a credentials provider whose loads follow a script.

use crate::config::SqsFeedConfig;
use crate::queue::{BoxFuture, SdkQueue};
use aws_credential_types::Credentials;
use aws_credential_types::provider::{self, ProvideCredentials, SharedCredentialsProvider};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::sync::watch;

/// The requests the stub answered, by kind.
#[derive(Debug, Default)]
pub(crate) struct Answered {
    pub(crate) extended: AtomicUsize,
    pub(crate) deleted: AtomicUsize,
}

/// An SQS endpoint on loopback: one message on the first receive and none
/// after, and every visibility change and delete accepted.
pub(crate) async fn sqs_stub() -> (String, Arc<Answered>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let answered = Arc::new(Answered::default());
    let counts = Arc::clone(&answered);
    let received = Arc::new(AtomicUsize::new(0));
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            tokio::spawn(serve(tcp, Arc::clone(&counts), Arc::clone(&received)));
        }
    });
    (format!("http://{addr}"), answered)
}

async fn serve(
    mut tcp: tokio::net::TcpStream,
    answered: Arc<Answered>,
    received: Arc<AtomicUsize>,
) {
    let mut buf = Vec::new();
    loop {
        let head_end = loop {
            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break i + 4;
            }
            if !read_more(&mut tcp, &mut buf).await {
                return;
            }
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).to_lowercase();
        let len: usize = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .map_or(0, |v| v.trim().parse().unwrap());
        while buf.len() < head_end + len {
            if !read_more(&mut tcp, &mut buf).await {
                return;
            }
        }
        let body: serde_json::Value = serde_json::from_slice(&buf[head_end..head_end + len])
            .unwrap_or(serde_json::Value::Null);
        buf.drain(..head_end + len);
        let all_accepted = || {
            let ids: Vec<_> = body["Entries"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|e| serde_json::json!({"Id": e["Id"]}))
                .collect();
            serde_json::json!({"Successful": ids, "Failed": []}).to_string()
        };
        let reply = if head.contains("amazonsqs.receivemessage") {
            if received.fetch_add(1, Ordering::SeqCst) == 0 {
                r#"{"Messages":[{"MessageId":"m0","ReceiptHandle":"h0","Body":"b","Attributes":{"ApproximateReceiveCount":"1"}}]}"#.to_owned()
            } else {
                r#"{"Messages":[]}"#.to_owned()
            }
        } else if head.contains("amazonsqs.changemessagevisibilitybatch") {
            answered.extended.fetch_add(1, Ordering::SeqCst);
            all_accepted()
        } else if head.contains("amazonsqs.deletemessagebatch") {
            answered.deleted.fetch_add(1, Ordering::SeqCst);
            all_accepted()
        } else {
            "{}".to_owned()
        };
        let out = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/x-amz-json-1.0\r\ncontent-length: {}\r\n\r\n{reply}",
            reply.len()
        );
        if tcp.write_all(out.as_bytes()).await.is_err() {
            return;
        }
    }
}

async fn read_more(tcp: &mut tokio::net::TcpStream, buf: &mut Vec<u8>) -> bool {
    let mut chunk = [0u8; 4096];
    let n = tcp.read(&mut chunk).await.unwrap_or(0);
    buf.extend_from_slice(&chunk[..n]);
    n > 0
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

/// An [`SdkQueue`] over `endpoint` that loads through `credentials`, at
/// `visibility`.
pub(crate) async fn queue_with(
    endpoint: &str,
    visibility: Duration,
    credentials: SharedCredentialsProvider,
) -> (SdkQueue, SqsFeedConfig) {
    let config = SqsFeedConfig {
        queue_url: format!("{endpoint}/000000000000/jobs"),
        visibility,
        region: Some("eu-west-2".to_owned()),
        endpoint: Some(endpoint.to_owned()),
    };
    let step = std::sync::Mutex::new("starting");
    let queue = crate::client::sdk_queue(&config, Some(credentials), &step)
        .await
        .unwrap();
    (queue, config)
}
