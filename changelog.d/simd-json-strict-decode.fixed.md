**Stricter JSON validation with simd** (`spate-json`)

With the `simd` feature enabled, decoding now rejects a NUL byte after a number,
underscore separators in numbers, and nesting deeper than 1024 levels.
Previously, these malformed numbers were accepted and nesting had no depth
limit. These inputs now produce a malformed decode error, so pipelines that
accepted them may skip the affected payload or fail according to their
configured error policy.
