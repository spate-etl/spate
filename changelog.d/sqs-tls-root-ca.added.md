**The SQS feed trusts a private CA through `tls.root_ca`** (`spate-sqs`)

`feed: { sqs: { tls: { root_ca: <path> } } }` names a PEM bundle whose certificates the feed trusts in addition to the system trust store, for its SQS, region and credential calls. A file that cannot be read, holds no certificate or holds one that does not parse fails connecting at `feed.sqs.tls.root_ca`, before any certificate reaches the AWS SDK. The key is refused when `endpoint` is set and is not `https://`.
