//! [`SdkTable`]: the table seam over the AWS SDK's DynamoDB client, and the
//! client it is built on.

use super::errors::{chain, classify};
use super::table::{
    BoxFuture, Cond, Created, Item, KeyAttr, Meta, Page, Query, Shape, Status, Table, Ttl, Write,
    WriteId, Written,
};
use crate::store::StoreError;
use aws_config::SdkConfig;
use aws_config::environment::region::EnvironmentVariableRegionProvider;
use aws_config::imds::region::ImdsRegionProvider;
use aws_config::meta::region::RegionProviderChain;
use aws_config::profile::region::ProfileFileRegionProvider;
use aws_config::provider_config::ProviderConfig;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::retry::{RetryConfig, RetryPartition};
use aws_sdk_dynamodb::config::timeout::TimeoutConfig;
use aws_sdk_dynamodb::config::{
    BehaviorVersion, Region, SharedCredentialsProvider, SharedHttpClient,
};
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, CancellationReason, ConditionCheck,
    KeySchemaElement, KeyType, ReturnValue, ReturnValuesOnConditionCheckFailure,
    ScalarAttributeType, TableStatus, TimeToLiveSpecification, TimeToLiveStatus, TransactWriteItem,
    Update,
};
use aws_smithy_http_client::ConnectorBuilder;
use aws_smithy_http_client::proxy::ProxyConfig;
use aws_smithy_http_client::tls::rustls_provider::CryptoMode;
use aws_smithy_http_client::tls::{Provider, TlsContext, TrustStore};
use rustls_native_certs::CertificateResult;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

type Attrs = HashMap<String, AttributeValue>;

/// The sort key of a job's meta item.
const META_SK: &str = "meta";

/// What a handle connects with.
#[derive(Clone, Debug)]
pub(super) struct Settings {
    pub(super) table: String,
    pub(super) region: Option<String>,
    pub(super) endpoint: Option<String>,
    pub(super) op_timeout: Duration,
    /// In place of the AWS provider chain.
    pub(super) credentials: Option<SharedCredentialsProvider>,
    /// The system trust store.
    pub(super) roots: fn() -> CertificateResult,
}

const NO_REGION: &str = "no AWS region: set dynamodb.region, or AWS_REGION in the environment";

/// Loads the AWS configuration and builds the table over it.
pub(super) async fn connect(settings: &Settings) -> Result<Arc<dyn Table>, StoreError> {
    let http = http_client(settings.roots).await?;
    let region = match &settings.region {
        Some(region) => Region::new(region.clone()),
        None => chain_region(&http, settings.op_timeout / 4).await?,
    };
    let mut loader = aws_config::defaults(BehaviorVersion::v2026_01_12())
        .http_client(http)
        .region(region);
    if let Some(credentials) = &settings.credentials {
        loader = loader.credentials_provider(credentials.clone());
    }
    let sdk = loader.load().await;
    Ok(Arc::new(SdkTable::new(&sdk, settings)))
}

/// The region the environment, the shared config file or instance metadata
/// names, looked up over `http` within `limit`.
///
/// The SDK's default region chain builds its own HTTP client, which reads
/// the system trust store on the calling thread and can block past `limit`.
///
/// # Errors
///
/// Fatal when the chain finds none or does not answer in time.
async fn chain_region(http: &SharedHttpClient, limit: Duration) -> Result<Region, StoreError> {
    let conf = ProviderConfig::default().with_http_client(http.clone());
    let chain = RegionProviderChain::first_try(EnvironmentVariableRegionProvider::new())
        .or_else(
            ProfileFileRegionProvider::builder()
                .configure(&conf)
                .build(),
        )
        .or_else(ImdsRegionProvider::builder().configure(&conf).build());
    match tokio::time::timeout(limit, chain.region()).await {
        Ok(Some(region)) => Ok(region),
        Ok(None) => Err(StoreError::Fatal(NO_REGION.into())),
        Err(_) => Err(StoreError::Fatal(format!(
            "{NO_REGION}; the provider chain's region lookup timed out after {limit:?}"
        ))),
    }
}

/// An HTTPS client that verifies servers against the certificates `system`
/// yields, or against the bundled Mozilla roots when none of them parse. It
/// goes through the proxies the standard environment variables name.
pub(super) async fn http_client(
    system: impl FnOnce() -> CertificateResult + Send + 'static,
) -> Result<SharedHttpClient, StoreError> {
    let (roots, fallback) = tokio::task::spawn_blocking(move || trust_roots(system()))
        .await
        .map_err(|e| StoreError::Fatal(format!("loading the system trust store: {e}")))?;
    if fallback {
        tracing::warn!(
            "the system trust store has no certificates; verifying DynamoDB against the \
             Mozilla root bundle"
        );
    }
    let tls = TlsContext::builder()
        .with_trust_store(TrustStore::empty().with_pem_certificate(roots))
        .build()
        .map_err(|e| StoreError::Fatal(format!("building the DynamoDB TLS context: {e}")))?;
    // The connector the SDK's default client builds, with this trust store.
    Ok(
        aws_smithy_http_client::Builder::new().build_with_connector_fn(
            move |settings, components| {
                let mut connector = ConnectorBuilder::default()
                    .tls_provider(Provider::Rustls(CryptoMode::AwsLc))
                    .tls_context(tls.clone())
                    .proxy_config(ProxyConfig::from_env());
                connector.set_connector_settings(settings.cloned());
                if let Some(components) = components {
                    connector.set_sleep_impl(components.sleep_impl());
                }
                connector.build()
            },
        ),
    )
}

/// The parsable certificates in `loaded` as one PEM bundle, or the Mozilla
/// roots when there are none, and whether it fell back.
///
/// The SDK panics on a certificate it cannot parse, so only those rustls
/// accepts are passed on.
pub(super) fn trust_roots(loaded: CertificateResult) -> (String, bool) {
    if !loaded.errors.is_empty() {
        let errors = loaded
            .errors
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ");
        tracing::debug!(%errors, "errors reading the system trust store");
    }
    let mut check = rustls::RootCertStore::empty();
    let mut bundle = String::new();
    for cert in loaded.certs {
        if check.add(cert.clone()).is_ok() {
            push_pem(&mut bundle, &cert);
        }
    }
    let fallback = bundle.is_empty();
    if fallback {
        for cert in webpki_root_certs::TLS_SERVER_ROOT_CERTS {
            push_pem(&mut bundle, cert);
        }
    }
    (bundle, fallback)
}

fn push_pem(out: &mut String, der: &[u8]) {
    use base64::Engine as _;
    let body = base64::engine::general_purpose::STANDARD.encode(der);
    out.push_str("-----BEGIN CERTIFICATE-----\n");
    for line in body.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).expect("base64 is ASCII"));
        out.push('\n');
    }
    out.push_str("-----END CERTIFICATE-----\n");
}

/// A table reached through the SDK. Every error it returns is classified.
#[derive(Debug)]
pub(super) struct SdkTable {
    client: Client,
    /// The client for [`Table::write_seed`], with an SDK retry quota of its own.
    seeding: Client,
    table: String,
}

impl SdkTable {
    /// A client over `sdk` whose attempts, retries and backoff all fit
    /// inside `settings.op_timeout`.
    ///
    /// It sets no attempt timeout: the SDK loads credentials inside an
    /// attempt, and a load that timeout cancels starts over on the next one.
    pub(super) fn new(sdk: &SdkConfig, settings: &Settings) -> SdkTable {
        let t = settings.op_timeout;
        let mut config = aws_sdk_dynamodb::config::Builder::from(sdk)
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_timeout(t * 9 / 10)
                    .connect_timeout(t / 4)
                    .read_timeout(t / 4)
                    .build(),
            )
            .retry_config(
                RetryConfig::standard()
                    .with_max_attempts(3)
                    .with_max_backoff(t / 4),
            );
        if let Some(endpoint) = &settings.endpoint {
            config = config.endpoint_url(endpoint);
        }
        let seeding = config
            .clone()
            .retry_partition(RetryPartition::custom("spate-seeding").build());
        SdkTable {
            client: Client::from_conf(config.build()),
            seeding: Client::from_conf(seeding.build()),
            table: settings.table.clone(),
        }
    }
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_string())
}

fn n(v: u64) -> AttributeValue {
    AttributeValue::N(v.to_string())
}

fn blob(v: impl Into<Vec<u8>>) -> AttributeValue {
    AttributeValue::B(Blob::new(v))
}

fn key(pk: &str, sk: &str) -> Attrs {
    HashMap::from([("pk".to_string(), s(pk)), ("sk".to_string(), s(sk))])
}

fn number(attrs: &Attrs, name: &str) -> Result<Option<u64>, StoreError> {
    match attrs.get(name) {
        None => Ok(None),
        Some(AttributeValue::N(v)) => v.parse().map(Some).map_err(|_| {
            StoreError::Fatal(format!(
                "the item attribute `{name}` holds {v:?}, not a u64"
            ))
        }),
        Some(other) => Err(StoreError::Fatal(format!(
            "the item attribute `{name}` holds {other:?}, not a number"
        ))),
    }
}

fn decode(attrs: &Attrs) -> Result<Item, StoreError> {
    let v = number(attrs, "v")?
        .ok_or_else(|| StoreError::Fatal("an item has no version attribute `v`".into()))?;
    let bytes = |name: &str| match attrs.get(name) {
        Some(AttributeValue::B(b)) => Some(b.clone().into_inner()),
        _ => None,
    };
    Ok(Item {
        v,
        b: bytes("b"),
        w: bytes("w").and_then(|w| WriteId::try_from(w).ok()),
        tomb: matches!(attrs.get("t"), Some(AttributeValue::Bool(true))),
        x: number(attrs, "x")?,
    })
}

/// The names and values of one expression.
#[derive(Default)]
struct Expr {
    names: HashMap<String, String>,
    values: HashMap<String, AttributeValue>,
}

impl Expr {
    /// Names each attribute `#<name>`.
    fn names(mut self, names: &[&str]) -> Self {
        for name in names {
            self.names.insert(format!("#{name}"), (*name).to_string());
        }
        self
    }

    fn value(mut self, name: &str, value: AttributeValue) -> Self {
        self.values.insert(name.to_string(), value);
        self
    }
}

/// The update, condition, returned attributes and expression of a write
/// that `UpdateItem` carries.
fn update(write: &Write) -> Option<(String, String, ReturnValue, Expr)> {
    Some(match write {
        Write::CreateDurable { b, w, now_ms } => (
            "SET #v = if_not_exists(#v, :now) + :one, #b = :b, #w = :w REMOVE #t, #x".into(),
            "attribute_not_exists(#v) OR attribute_exists(#t)".into(),
            ReturnValue::UpdatedNew,
            Expr::default()
                .names(&["v", "b", "w", "t", "x"])
                .value(":now", n(*now_ms))
                .value(":one", n(1))
                .value(":b", blob(b.clone()))
                .value(":w", blob(w.to_vec())),
        ),
        Write::Put { v, b, w, x, cond } => {
            let mut set = "SET #v = :v, #b = :b, #w = :w".to_string();
            let mut expr = Expr::default()
                .names(&["v", "b", "w"])
                .value(":v", n(*v))
                .value(":b", blob(b.clone()))
                .value(":w", blob(w.to_vec()));
            if let Some(x) = x {
                set.push_str(", #x = :x");
                expr = expr.names(&["x"]).value(":x", n(*x));
            }
            let condition = match cond {
                Cond::Absent => "attribute_not_exists(#v)",
                Cond::VersionIs(e) => {
                    expr = expr.value(":e", n(*e));
                    "#v = :e"
                }
                Cond::LiveVersionIs(e) => {
                    expr = expr.names(&["t"]).value(":e", n(*e));
                    "#v = :e AND attribute_not_exists(#t)"
                }
            };
            (set, condition.into(), ReturnValue::None, expr)
        }
        Write::Tombstone { expected, w, x } => {
            let mut expr = Expr::default()
                .names(&["v", "b", "w", "t", "x"])
                .value(":one", n(1))
                .value(":true", AttributeValue::Bool(true))
                .value(":x", n(*x))
                .value(":w", blob(w.to_vec()));
            let condition = match expected {
                Some(e) => {
                    expr = expr.value(":e", n(*e));
                    "#v = :e AND attribute_not_exists(#t)"
                }
                None => "attribute_exists(#v) AND attribute_not_exists(#t)",
            };
            (
                "SET #v = #v + :one, #t = :true, #x = :x, #w = :w REMOVE #b".into(),
                condition.into(),
                ReturnValue::UpdatedNew,
                expr,
            )
        }
        Write::Raise { v, x } => (
            "SET #v = :v, #x = :x".into(),
            "attribute_not_exists(#v) OR #v < :v".into(),
            ReturnValue::None,
            Expr::default()
                .names(&["v", "x"])
                .value(":v", n(*v))
                .value(":x", n(*x)),
        ),
        Write::Remove { .. } => return None,
    })
}

/// The item a failed condition returned, when `e` is that failure.
fn failed_against<E>(
    e: &SdkError<E, aws_sdk_dynamodb::config::http::HttpResponse>,
    item: impl Fn(&E) -> Option<Option<&Attrs>>,
) -> Option<Result<Written, StoreError>> {
    let old = item(e.as_service_error()?)?;
    Some(match old.map(decode).transpose() {
        Ok(old) => Ok(Written::Failed { old }),
        Err(e) => Err(e),
    })
}

impl SdkTable {
    async fn update_item(
        &self,
        client: &Client,
        pk: &str,
        sk: &str,
        write: &Write,
    ) -> Result<Written, StoreError> {
        let (update, condition, returns, expr) = update(write).expect("an UpdateItem write");
        let result = client
            .update_item()
            .table_name(&self.table)
            .set_key(Some(key(pk, sk)))
            .update_expression(update)
            .condition_expression(condition)
            .set_expression_attribute_names(Some(expr.names))
            .set_expression_attribute_values(Some(expr.values))
            .return_values(returns)
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
            .send()
            .await;
        match result {
            Ok(out) => {
                let v = match (write, out.attributes()) {
                    (Write::Put { v, .. }, _) => Some(*v),
                    (_, Some(attrs)) => number(attrs, "v")?,
                    (_, None) => None,
                };
                Ok(Written::Ok { v })
            }
            Err(e) => failed_against(&e, |e| match e {
                UpdateItemError::ConditionalCheckFailedException(ccf) => Some(ccf.item()),
                _ => None,
            })
            .unwrap_or_else(|| Err(classify(&format!("UpdateItem {sk}"), &e))),
        }
    }

    /// A `TransactWriteItems` of a `ConditionCheck` on the floor item and
    /// the put's `Update`, both returning the item a failed condition met.
    async fn transact_create(
        &self,
        pk: &str,
        floor_pk: &str,
        sk: &str,
        put: &Write,
    ) -> Result<Created, StoreError> {
        let Write::Put { v: base, .. } = put else {
            panic!("create_above takes a Write::Put");
        };
        let (update, condition, _, expr) = update(put).expect("an UpdateItem write");
        let check = ConditionCheck::builder()
            .table_name(&self.table)
            .set_key(Some(key(floor_pk, sk)))
            .condition_expression("attribute_not_exists(#v) OR #v < :v")
            .expression_attribute_names("#v", "v")
            .expression_attribute_values(":v", n(*base))
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
            .build()
            .expect("table, key and condition are set");
        let create = Update::builder()
            .table_name(&self.table)
            .set_key(Some(key(pk, sk)))
            .update_expression(update)
            .condition_expression(condition)
            .set_expression_attribute_names(Some(expr.names))
            .set_expression_attribute_values(Some(expr.values))
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
            .build()
            .expect("table, key and update are set");
        let result = self
            .client
            .transact_write_items()
            .transact_items(TransactWriteItem::builder().condition_check(check).build())
            .transact_items(TransactWriteItem::builder().update(create).build())
            .send()
            .await;
        let e = match result {
            Ok(_) => return Ok(Created::Ok),
            Err(e) => e,
        };
        if let Some(TransactWriteItemsError::TransactionCanceledException(cancelled)) =
            e.as_service_error()
        {
            let reasons = cancelled.cancellation_reasons();
            let failed = |i: usize| {
                reasons
                    .get(i)
                    .filter(|r| r.code() == Some("ConditionalCheckFailed"))
                    .map(CancellationReason::item)
            };
            if let Some(old) = failed(1) {
                let old = old.map(decode).transpose()?;
                return Ok(Created::Exists { old });
            }
            if let Some(Some(floor)) = failed(0) {
                return Ok(Created::Floor(decode(floor)?.v));
            }
        }
        Err(classify(&format!("TransactWriteItems {sk}"), &e))
    }

    async fn delete_item(
        &self,
        pk: &str,
        sk: &str,
        expected: Option<u64>,
    ) -> Result<Written, StoreError> {
        use aws_sdk_dynamodb::operation::delete_item::DeleteItemError;
        let mut call = self
            .client
            .delete_item()
            .table_name(&self.table)
            .set_key(Some(key(pk, sk)));
        if let Some(e) = expected {
            call = call
                .condition_expression("#v = :e")
                .expression_attribute_names("#v", "v")
                .expression_attribute_values(":e", n(e))
                .return_values_on_condition_check_failure(
                    ReturnValuesOnConditionCheckFailure::AllOld,
                );
        }
        match call.send().await {
            Ok(_) => Ok(Written::Ok { v: None }),
            Err(e) => failed_against(&e, |e| match e {
                DeleteItemError::ConditionalCheckFailedException(ccf) => Some(ccf.item()),
                _ => None,
            })
            .unwrap_or_else(|| Err(classify(&format!("DeleteItem {sk}"), &e))),
        }
    }

    async fn read_page(&self, query: Query) -> Result<Page, StoreError> {
        let mut key_condition = "#pk = :pk".to_string();
        let mut expr = Expr::default().names(&["pk"]).value(":pk", s(&query.pk));
        if let Some(prefix) = &query.prefix {
            key_condition.push_str(" AND begins_with(#sk, :p)");
            expr = expr.names(&["sk"]).value(":p", s(prefix));
        }
        let mut call = self
            .client
            .query()
            .table_name(&self.table)
            .consistent_read(query.consistent)
            .key_condition_expression(key_condition)
            .set_exclusive_start_key(query.start.as_deref().map(|sk| key(&query.pk, sk)));
        if query.filter_tombs {
            expr = expr.names(&["t"]);
            call = call.filter_expression("attribute_not_exists(#t)");
        }
        let out = call
            .set_expression_attribute_names(Some(expr.names))
            .set_expression_attribute_values(Some(expr.values))
            .send()
            .await
            .map_err(|e| classify(&format!("Query {}", query.pk), &e))?;
        let sort_key = |attrs: &Attrs| match attrs.get("sk") {
            Some(AttributeValue::S(sk)) => Ok(sk.clone()),
            _ => Err(StoreError::Fatal(
                "an item has no string sort key `sk`".into(),
            )),
        };
        let items = out
            .items()
            .iter()
            .map(|attrs| Ok((sort_key(attrs)?, decode(attrs)?)))
            .collect::<Result<_, StoreError>>()?;
        let next = out.last_evaluated_key().map(sort_key).transpose()?;
        Ok(Page { items, next })
    }
}

impl Table for SdkTable {
    fn write<'a>(
        &'a self,
        pk: &'a str,
        sk: &'a str,
        write: Write,
    ) -> BoxFuture<'a, Result<Written, StoreError>> {
        Box::pin(async move {
            match write {
                Write::Remove { expected } => self.delete_item(pk, sk, expected).await,
                write => self.update_item(&self.client, pk, sk, &write).await,
            }
        })
    }

    fn write_seed<'a>(
        &'a self,
        pk: &'a str,
        sk: &'a str,
        write: Write,
    ) -> BoxFuture<'a, Result<Written, StoreError>> {
        Box::pin(async move { self.update_item(&self.seeding, pk, sk, &write).await })
    }

    fn create_above<'a>(
        &'a self,
        pk: &'a str,
        floor_pk: &'a str,
        sk: &'a str,
        put: Write,
    ) -> BoxFuture<'a, Result<Created, StoreError>> {
        Box::pin(async move { self.transact_create(pk, floor_pk, sk, &put).await })
    }

    fn get<'a>(
        &'a self,
        pk: &'a str,
        sk: &'a str,
    ) -> BoxFuture<'a, Result<Option<Item>, StoreError>> {
        Box::pin(async move {
            let out = self
                .client
                .get_item()
                .table_name(&self.table)
                .set_key(Some(key(pk, sk)))
                .consistent_read(true)
                .send()
                .await
                .map_err(|e| classify(&format!("GetItem {sk}"), &e))?;
            out.item().map(decode).transpose()
        })
    }

    fn query(&self, query: Query) -> BoxFuture<'_, Result<Page, StoreError>> {
        Box::pin(self.read_page(query))
    }

    fn put_meta<'a>(
        &'a self,
        pk: &'a str,
        meta: Meta,
    ) -> BoxFuture<'a, Result<Option<Meta>, StoreError>> {
        Box::pin(async move {
            let result = self
                .client
                .update_item()
                .table_name(&self.table)
                .set_key(Some(key(pk, META_SK)))
                .update_expression("SET #l = :l, #y = :y")
                .condition_expression("attribute_not_exists(#l)")
                .expression_attribute_names("#l", "l")
                .expression_attribute_names("#y", "y")
                .expression_attribute_values(":l", n(meta.lease_ms))
                .expression_attribute_values(":y", n(meta.layout))
                .return_values_on_condition_check_failure(
                    ReturnValuesOnConditionCheckFailure::AllOld,
                )
                .send()
                .await;
            let e = match result {
                Ok(_) => return Ok(None),
                Err(e) => e,
            };
            let Some(UpdateItemError::ConditionalCheckFailedException(ccf)) = e.as_service_error()
            else {
                return Err(classify("UpdateItem meta", &e));
            };
            let held = ccf.item().ok_or_else(|| {
                StoreError::Fatal("the job's meta item came back without attributes".into())
            })?;
            let field = |name: &str| {
                number(held, name)?.ok_or_else(|| {
                    StoreError::Fatal(format!("the job's meta item has no `{name}`"))
                })
            };
            Ok(Some(Meta {
                lease_ms: field("l")?,
                layout: field("y")?,
            }))
        })
    }

    fn describe(&self) -> BoxFuture<'_, Result<Option<Shape>, StoreError>> {
        Box::pin(async move {
            let out = match self
                .client
                .describe_table()
                .table_name(&self.table)
                .send()
                .await
            {
                Ok(out) => out,
                Err(e)
                    if e.as_service_error()
                        .is_some_and(|e| e.is_resource_not_found_exception()) =>
                {
                    return Ok(None);
                }
                Err(e) => return Err(classify("DescribeTable", &e)),
            };
            let Some(table) = out.table() else {
                return Ok(None);
            };
            let status = match table.table_status() {
                Some(TableStatus::Creating) => Status::Creating,
                Some(TableStatus::Active) => Status::Active,
                Some(TableStatus::Updating) => Status::Updating,
                Some(other) => Status::Other(other.as_str().to_string()),
                None => Status::Other("of unknown status".into()),
            };
            let kind = |name: &str| {
                table
                    .attribute_definitions()
                    .iter()
                    .find(|d| d.attribute_name() == name)
                    .map_or_else(String::new, |d| d.attribute_type().as_str().to_string())
            };
            let keys = table
                .key_schema()
                .iter()
                .map(|k| KeyAttr {
                    name: k.attribute_name().to_string(),
                    hash: *k.key_type() == KeyType::Hash,
                    kind: kind(k.attribute_name()),
                })
                .collect();
            Ok(Some(Shape {
                status,
                keys,
                local_indexes: table.local_secondary_indexes().len(),
                global_indexes: table.global_secondary_indexes().len(),
                replicas: table.replicas().len(),
            }))
        })
    }

    fn create_table(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            let attr = |name: &str| {
                AttributeDefinition::builder()
                    .attribute_name(name)
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .expect("name and type are set")
            };
            let key = |name: &str, kind| {
                KeySchemaElement::builder()
                    .attribute_name(name)
                    .key_type(kind)
                    .build()
                    .expect("name and type are set")
            };
            match self
                .client
                .create_table()
                .table_name(&self.table)
                .billing_mode(BillingMode::PayPerRequest)
                .attribute_definitions(attr("pk"))
                .attribute_definitions(attr("sk"))
                .key_schema(key("pk", KeyType::Hash))
                .key_schema(key("sk", KeyType::Range))
                .send()
                .await
            {
                Ok(_) => Ok(()),
                Err(e)
                    if e.as_service_error()
                        .is_some_and(|e| e.is_resource_in_use_exception()) =>
                {
                    Ok(())
                }
                Err(e) => Err(classify("CreateTable", &e)),
            }
        })
    }

    fn describe_ttl(&self) -> BoxFuture<'_, Result<Ttl, StoreError>> {
        Box::pin(async move {
            let out = match self
                .client
                .describe_time_to_live()
                .table_name(&self.table)
                .send()
                .await
            {
                Ok(out) => out,
                Err(e) if e.code() == Some("AccessDeniedException") => {
                    return Ok(Ttl::Unknown(chain(&e)));
                }
                Err(e) => return Err(classify("DescribeTimeToLive", &e)),
            };
            let description = out.time_to_live_description();
            Ok(match description.and_then(|d| d.time_to_live_status()) {
                Some(TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling) => Ttl::On(
                    description
                        .and_then(|d| d.attribute_name())
                        .unwrap_or_default()
                        .to_string(),
                ),
                _ => Ttl::Off,
            })
        })
    }

    fn enable_ttl(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            let spec = TimeToLiveSpecification::builder()
                .enabled(true)
                .attribute_name("x")
                .build()
                .expect("enabled and attribute are set");
            match self
                .client
                .update_time_to_live()
                .table_name(&self.table)
                .time_to_live_specification(spec)
                .send()
                .await
            {
                Ok(_) => Ok(()),
                Err(e)
                    if e.code() == Some("ValidationException")
                        && e.message().is_some_and(|m| m.contains("already enabled")) =>
                {
                    Ok(())
                }
                Err(e) => Err(classify("UpdateTimeToLive", &e)),
            }
        })
    }
}
