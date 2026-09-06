**Stricter `simd` decode validation** (`spate-json`) — a document with a
trailing NUL byte after a number or an underscore digit separator (neither
legal JSON) now surfaces as a malformed decode error under the `simd` feature,
where it previously decoded. Nesting deeper than 1024 also now errors instead
of recursing without bound. Both come from the `simd-json` upgrade and bring
its validation closer to `serde_json`'s.
