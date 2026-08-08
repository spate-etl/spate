**Synthetic data source** (`spate-datagen`, `spate` feature `datagen`) — a
built-in storefront dataset of orders, payments and refunds, so a pipeline runs
with no broker, bucket or coordination store. Each lane owns a disjoint slice
of the order-id space and its own bounded ring of open orders, so a payment
always references an order on the same partition at a lower offset with no
coordination on the record path. `count` drains the pipeline to a clean exit;
`tick_interval: 0s` runs unthrottled. It keeps no durable progress — a restart
regenerates the whole stream — and is a demo and test source, not a production
one.
