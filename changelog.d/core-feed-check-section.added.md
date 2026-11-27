**A feed backend can refuse `feed:` keys it cannot honor on a feed built in code** (`spate-core`)

`Feed` has a new method with a default, `check_section`. The runtime hands it the pipeline's `feed:` section before the source opens, for a feed built from the section and for one passed to `Pipeline::feed` or `Pipeline::seeded_feed`, and fails startup with the refusal. The default accepts every section. A feed that wraps another should forward it to the inner one.

`MemoryFeed` refuses a `location` and a `settling_window` there. A passed feed that names the `memory` backend but is not a `MemoryFeed` is checked by its own `check_section`, so a section built in code with a `location` beside `memory` reaches it unrefused. A YAML file with that section is still refused when it loads.
