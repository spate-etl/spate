use super::*;

/// Pins `SqsFeed::abandon`'s rustdoc on `hand_back`: `Ok` until a unit's
/// deadline, then `Gone`, and nothing sent either way.
#[tokio::test(start_paused = true)]
async fn hand_back_after_abandon_answers_ok_until_the_deadline() {
    let (feed, queue) = start(MINUTE);
    let (waker, park) = wake_channel();
    feed.set_waker(waker);
    queue.ready(Message::new("a"));
    queue.ready(Message::new("b"));
    let got = receive_n(&feed, &park, 2).await;
    feed.abandon();
    feed.hand_back(got[0].receipt).unwrap();
    tokio::time::advance(MINUTE + Duration::from_secs(1)).await;
    assert_eq!(
        feed.hand_back(got[1].receipt).unwrap_err().kind,
        FeedErrorKind::Gone
    );
    assert!(queue.visibility().is_empty());
}
