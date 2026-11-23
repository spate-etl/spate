**Breaking:** **The datagen config enums carry the connector's name** (`spate-datagen`)

`Dataset`, `Encoding` and `Clock` are renamed `DatagenDataset`, `DatagenEncoding` and `DatagenClock`, so the clock enum no longer shares its name with core's `Clock` trait. Their variants and the `dataset`, `encoding` and `clock` keys are unchanged.
