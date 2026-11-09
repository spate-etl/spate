**A wait that reports the last value it read** (`spate-test`)

`wait_for_value` polls a read until a predicate holds for its value, and returns that value. When it reaches its bound, the panic says the test hung, as `wait_until`'s does, and adds the last value read, so a hang shows the state the test was stuck on.
