**A leader keeps updating its own assignment after an older-generation write to its record** (`spate-coordination`)

When the leader's own `assign.{instance}` record is overwritten at an older generation, the leader ignores it as an instruction and caches its revision. The next publish to its own key wins. In previous versions the revision stayed stale, every later publish to the key lost its compare-and-swap, and the leader could not assign itself a split that became available.
