**`DurationBounds` can state an exclusive floor and whole seconds** (`spate-core`)

`DurationBounds::above(floor)` accepts anything longer than `floor` and refuses `floor` itself. `whole_seconds()` also refuses a value with a part below one second. `check_duration` states each rule in its refusal, such as `must be more than 5s and at most 1day; found 5s` or `must be from 3s to 12h in whole seconds; found 2s 500ms`. Bounds that already exist accept and refuse the same values as before.
