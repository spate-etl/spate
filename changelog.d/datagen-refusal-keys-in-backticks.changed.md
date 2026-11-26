**Two datagen source refusals put other keys in backticks** (`spate-datagen`)

A zero `events_per_tick` now suggests setting `` `tick_interval` `` to 0s, and a `count` below `partitions` names `partitions` and `count` in backticks, where it named `source.datagen.partitions` in full. The same values are refused as before.
