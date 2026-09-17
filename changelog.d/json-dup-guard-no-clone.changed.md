**The duplicate-key guard stops copying every key** (`spate-json`) — with
`reject_duplicate_keys: true`, the structural pass copied each object key into
the set it checks, an allocation per key that only the duplicate-key error could
use. Keys are now moved in, halving the guard's allocations on clean input.
Detection at every depth, the error text and the `duplicate_key` label are
unchanged.
