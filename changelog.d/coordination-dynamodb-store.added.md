**A DynamoDB coordination store** (`spate-coordination`, `spate`)

A coordinated source can keep its splits, leases and plan in one DynamoDB
table. Enable the `coordination-dynamodb` feature on `spate` and select the
store with `coordination.store.dynamodb`, giving `table` and `job`; `region`,
`endpoint`, `create_table` and `poll_interval` are optional. Credentials come
from the AWS provider chain. The store's watches list the table every
`poll_interval`, and each worker judges lease expiry from its own reads. A
watching peer sees a dead worker's lease expire up to one lease plus two poll
intervals after its last heartbeat, plus the time its polls take; takeover then
waits `rebalance_delay`, up to two more poll intervals for the assignment, and
the lease's expiry in the new owner's own view. Code builds the store with
`DynamoDbStore::new` and runs it as a `DynamoDbCoordinator`. The store page lists the IAM actions and the table's
key schema.
