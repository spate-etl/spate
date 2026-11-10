**A seeded run fails back a unit whose batch the sink abandoned** (`spate-s3`)

On a run over a seeded feed, a unit whose batch the sink abandons after a write error it does not retry is now failed back to the feed at a later commit tick. The feed delivers it again or dead-letters it at its cap, and the run goes on to end by itself. Previously the unit stayed held until a stop, or until `checkpoint.stalled_fail_after` failed the run, and a failed run left the caller unable to tell which units the sink had written. Runs on any other feed are unchanged.
