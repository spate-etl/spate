//! A plain-HTTP S3 stub serving one bucket from memory.

use std::collections::BTreeMap;
use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

/// Serves bucket `b` path-style: a `ListObjectsV2` request gets one page
/// listing `objects`, a GET of a listed key gets its body, and anything else
/// a 404. Listed without ETags, so every object is read with one plain GET.
pub(crate) fn s3_stub(objects: &[(&str, &[u8])]) -> String {
    let contents: String = objects
        .iter()
        .map(|(key, body)| {
            format!(
                "<Contents><Key>{key}</Key><LastModified>2026-10-11T13:38:17.000Z\
                 </LastModified><Size>{}</Size></Contents>",
                body.len()
            )
        })
        .collect();
    let page = Arc::new(format!(
        "<ListBucketResult><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"
    ));
    let bodies: Arc<BTreeMap<String, Vec<u8>>> = Arc::new(
        objects
            .iter()
            .map(|(key, body)| (format!("/b/{key}"), body.to_vec()))
            .collect(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a local port");
    let url = format!("http://{}", listener.local_addr().expect("local address"));
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let (page, bodies) = (Arc::clone(&page), Arc::clone(&bodies));
            std::thread::spawn(move || {
                let _ = answer(stream, &page, &bodies);
            });
        }
    });
    url
}

fn answer(
    mut stream: TcpStream,
    page: &str,
    bodies: &BTreeMap<String, Vec<u8>>,
) -> std::io::Result<()> {
    loop {
        let mut request = Vec::new();
        let mut buf = [0u8; 4096];
        while !request.windows(4).any(|w| w == b"\r\n\r\n") {
            let n = stream.read(&mut buf)?;
            if n == 0 {
                return Ok(());
            }
            request.extend_from_slice(&buf[..n]);
        }
        let head = String::from_utf8_lossy(&request);
        let target = head.split_whitespace().nth(1).unwrap_or_default();
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        let (status, body): (&str, &[u8]) = if query.split('&').any(|p| p == "list-type=2") {
            ("200 OK", page.as_bytes())
        } else if let Some(body) = bodies.get(path) {
            ("200 OK", body)
        } else {
            ("404 Not Found", b"<Error><Code>NoSuchKey</Code></Error>")
        };
        write!(
            stream,
            "HTTP/1.1 {status}\r\ncontent-type: application/xml\r\n\
             last-modified: Sun, 11 Oct 2026 13:38:17 GMT\r\ncontent-length: {}\r\n\r\n",
            body.len()
        )?;
        stream.write_all(body)?;
        stream.flush()?;
    }
}
