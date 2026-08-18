**Nameable `map_rec` bounds** (`spate-core`) — `MapFn` and `TryMapFn`, the two
traits that carry the bound on `ChainBuilder::map_rec` and `try_map_rec`, are
exported from `spate_core::ops` (`spate::ops` on the facade). A record family
that borrows the source buffer transforms through those two methods. Naming
the bound is what a helper generic over one stage needs.
