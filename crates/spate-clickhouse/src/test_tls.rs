//! A local HTTPS server for TLS tests, and endpoints that trust a test CA.

use crate::writer::ClickHouseEndpoint;
use hyper::Response;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use rustls::{CertificateError, RootCertStore};
use std::convert::Infallible;
use std::error::Error;

pub(crate) use spate_test_support::TestCa;

/// Serves an empty `200 OK` to every request on `127.0.0.1`, over a
/// certificate `ca` signed, and returns the server's `https://` URL. With
/// `clients` set, the server requires a client certificate that CA signed.
pub(crate) async fn serve(ca: &TestCa, clients: Option<&TestCa>) -> String {
    let addr = spate_test_support::serve_tls(ca.server_config(clients), |tls| async move {
        let ok = service_fn(|_| async { Ok::<_, Infallible>(Response::new(String::new())) });
        let _ = http1::Builder::new()
            .serve_connection(TokioIo::new(tls), ok)
            .await;
    })
    .await;
    format!("https://{addr}")
}

/// An endpoint for `url` whose client trusts `ca` and nothing else.
pub(crate) fn endpoint_trusting(ca: &TestCa, url: &str) -> ClickHouseEndpoint {
    let mut roots = RootCertStore::empty();
    roots.add(ca.der()).unwrap();
    let client = crate::http::client(&crate::http::client_config(roots)).with_url(url);
    ClickHouseEndpoint::new(client, url.to_owned())
}

/// The error a `SELECT 1` to `url` fails with, from a client that trusts
/// `ca` and nothing else.
///
/// # Panics
///
/// Panics when the query succeeds.
pub(crate) async fn failed_query(ca: &TestCa, url: &str) -> clickhouse::error::Error {
    endpoint_trusting(ca, url)
        .client()
        .query("SELECT 1")
        .execute()
        .await
        .unwrap_err()
}

/// Whether `err`'s source chain holds rustls's unknown-issuer rejection.
pub(crate) fn is_unknown_issuer(err: &(dyn Error + 'static)) -> bool {
    matches!(
        spate_core::tls_rejection!(rustls, err),
        Some(rustls::Error::InvalidCertificate(
            CertificateError::UnknownIssuer
        ))
    )
}
