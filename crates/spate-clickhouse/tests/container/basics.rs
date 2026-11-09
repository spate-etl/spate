use super::*;

#[tokio::test]
#[ignore = "requires Docker"]
async fn multi_frame_batches_land_and_read_back_exactly() {
    let srv = server().await;
    let sink = sink_for(&srv.url).await;

    let expected = orders(0..1_000);
    let batch = sealed(&expected, "e2e-batch-1", 4);
    sink.writer
        .write_batch(&sink.endpoints[0][0], &batch)
        .await
        .expect("write");

    let mut got: Vec<Order> = srv
        .admin
        .query("SELECT ?fields FROM orders ORDER BY id")
        .fetch_all()
        .await
        .expect("read back");
    got.sort_by_key(|o| o.id);
    assert_eq!(got, expected, "typed read-back must match encoded rows");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn same_token_dedupes_different_token_inserts() {
    let srv = server().await;
    let sink = sink_for(&srv.url).await;
    let rows = orders(0..100);

    let batch = sealed(&rows, "dedup-proof", 2);
    for _ in 0..2 {
        sink.writer
            .write_batch(&sink.endpoints[0][0], &batch)
            .await
            .expect("write");
    }
    assert_eq!(
        count(&srv.admin).await,
        100,
        "identical batch + identical token must deduplicate"
    );

    let renamed = sealed(&rows, "dedup-proof-DIFFERENT", 2);
    sink.writer
        .write_batch(&sink.endpoints[0][0], &renamed)
        .await
        .expect("write");
    assert_eq!(
        count(&srv.admin).await,
        200,
        "same rows under a different token must insert"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn probe_reflects_connectivity() {
    let srv = server().await;
    let sink = sink_for(&srv.url).await;
    sink.writer
        .probe(&sink.endpoints[0][0])
        .await
        .expect("probe healthy server");

    // A server that goes away after the sink is built: the sink reads the
    // table's schema when it is built, so an endpoint that never answers
    // cannot produce one.
    let doomed = server().await;
    let unreachable = sink_for(&doomed.url).await;
    drop(doomed);
    assert!(
        unreachable
            .writer
            .probe(&unreachable.endpoints[0][0])
            .await
            .is_err()
    );
}

/// A setting the server refuses fails the sink when it is built, with the
/// server's message: an unknown name and a value that does not parse.
#[tokio::test]
#[ignore = "requires Docker"]
async fn a_refused_setting_fails_the_build() {
    let srv = server().await;
    for (setting, code) in [
        ("insert_qourum: \"2\"", "UNKNOWN_SETTING"),
        ("async_insert: \"maybe\"", "CANNOT_PARSE_BOOL"),
    ] {
        let err = try_sink_with::<Owned<Order>>(
            &srv.url,
            "orders",
            &format!("{SERVER_CREDENTIALS}settings: {{ {setting} }}\n"),
        )
        .await
        .expect_err(&format!("{setting} must not build"));
        assert!(
            matches!(err, spate_clickhouse::SchemaError::Settings { .. }),
            "{err:?}"
        );
        assert!(err.to_string().contains(code), "{setting}: {err}");
    }
}

/// A setting that leaves `SELECT 1` with no row still builds the sink, and
/// the insert carrying it lands.
#[tokio::test]
#[ignore = "requires Docker"]
async fn a_setting_that_empties_select_one_builds() {
    let srv = server().await;
    let sink = sink_with::<Owned<Order>>(
        &srv.url,
        "orders",
        &format!("{SERVER_CREDENTIALS}settings: {{ offset: \"5\" }}\n"),
    )
    .await;
    sink.writer
        .write_batch(&sink.endpoints[0][0], &sealed(&orders(0..10), "offset", 1))
        .await
        .expect("write");
    assert_eq!(count(&srv.admin).await, 10);
}
