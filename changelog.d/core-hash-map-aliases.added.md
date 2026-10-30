**Seeded hash map aliases in `spate_core::hash`** (`spate-core`)

`FastMap`, `FastSet`, `QualityMap` and `QualitySet` are std maps and sets over hashers seeded per instance, for operators and connectors that keep a map on the record path. `Fast*` is for keys the pipeline produces, and `Quality*` for keys an outside party chooses.
