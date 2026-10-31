**A feed backend can label its families with the feed's operations** (`spate-core`)

`spate_core::metrics::FeedOp` names the calls a source makes on its feed, and `FeedOp::label` gives the `op` label value the `spate_feed_*` families use. A feed backend that counts something per call labels its own family with it, so the family joins the framework's on `op`. `FeedOp::ALL` lists every call in declaration order.
