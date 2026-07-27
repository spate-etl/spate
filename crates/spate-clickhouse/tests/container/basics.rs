use super::*;

#[tokio::test]
#[ignore = "requires Docker"]
async fn multi_frame_batches_land_and_read_back_exactly() {
    let srv = server().await;
    let sink = sink_for(&srv.url);

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
    let sink = sink_for(&srv.url);
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
    let sink = sink_for(&srv.url);
    sink.writer
        .probe(&sink.endpoints[0][0])
        .await
        .expect("probe healthy server");

    let unreachable = sink_for("http://127.0.0.1:1");
    assert!(
        unreachable
            .writer
            .probe(&unreachable.endpoints[0][0])
            .await
            .is_err()
    );
}
