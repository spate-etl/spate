**The SQS feed's call retry backoff is jittered** (`spate-sqs`)

A receive, publish, delete or fail call that fails transiently now waits a random 80% to 100% of each step before it is sent again. The steps still start at 200 ms and double up to 5 s. A delete or fail batch is still given up once its next wait would reach the earliest deadline among its units, so a batch whose exact step would have reached that deadline can now be retried. In previous versions every call waited the exact step, so feeds whose calls failed together retried at the same instants.
