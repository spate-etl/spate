**The S3 source's read retry backoff is jittered** (`spate-s3`)

An object read or `HEAD` that fails transiently now waits a random 80% to 100% of each step before the next attempt. The steps still start at 200 ms and double up to 5 s over the eight attempts a read gets, so a read that keeps failing now poisons its unit after between 12.96 s and 16.2 s of waits, where before it took exactly 16.2 s. In previous versions every read waited the exact step, so workers whose reads failed together retried at the same instants. The lister's publish retries keep their exact schedule.
