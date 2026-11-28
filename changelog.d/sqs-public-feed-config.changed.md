**Breaking:** **The SQS feed builds from a public `SqsFeedConfig`** (`spate-sqs`)

`SqsFeed::connect` takes an `SqsFeedConfig` in place of the `feed:` section. Code that passed the section passes `SqsFeedConfig::from_section(&section)?` instead. Code that builds its pipeline without a file starts from `SqsFeedConfig::new(queue_url)` and sets the public fields it needs. `connect` checks the config with `SqsFeedConfig::validate` before it calls SQS, and refuses the values `from_section` refuses, as a configuration error.

`SqsFeedConfig` is `#[non_exhaustive]`, so code outside the crate builds it through `new` or `from_section`. The YAML keys and their defaults are unchanged.

The feed refuses an `attempt_cap` and a `settling_window` under a minute when the runtime or a lister run hands it the section, so a feed built from a config in code meets the same refusals as one built from the section.
