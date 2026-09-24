**NATS store: a guarded delete of an absent key wins** (`spate-coordination`)

`NatsStore::delete` with an expected revision returns `CasOutcome::Won` when
the key is absent, whether it was never written, deleted or expired. This
matches `MemoryStore` and the `CoordinationStore::delete` documentation. In
previous versions it returned `CasOutcome::Lost`. As a result, a leader
retried the delete of a departed instance's assignment record on every step
until a watch event or the reconcile listing dropped it. Custom stores should
return `Won` in the same case.
