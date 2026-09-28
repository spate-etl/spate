**A fatal store error after startup stops the coordinator** (`spate-coordination`)

The coordinator stops with the store's error when a lease renewal, claim,
assignment write, seed, listing or release fails with an error the store reports
as fatal, such as a rejected credential. In previous versions only a commit did.
Every other path logged the error and retried on the next tick, so a worker
whose write permission was revoked kept running without doing any work.
