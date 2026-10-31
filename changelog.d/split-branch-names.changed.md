**Breaking:** **The split branch's bundle and handle are renamed** (`spate-core`)

The bundle that `ChainCtx::sink` returns for one split branch is `spate::ops::SplitTarget`, and the typed handle that `SplitBuilder::add` returns is `spate::ops::SplitBranch<F>`. In previous versions they were `spate::ops::SinkCtx` and `spate::ops::Sink<F>`, and the old names are removed. `ChainCtx::sink`, `SplitBuilder::add`, `SplitEmitter::emit`, `SplitTarget::new` and `SplitTarget::with_chunk` keep their names and behavior. A pipeline that passes `ctx.sink(..)` into `add` and emits the returned handle compiles unchanged.

To upgrade, change the names where code writes them: a split assembled by hand with `SinkCtx::new`, and a handle stored in a typed field or signature. The same paths apply without the facade, under `spate_core::ops`.
