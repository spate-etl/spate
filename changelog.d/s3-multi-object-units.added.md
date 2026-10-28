**The S3 source reads a work unit that names several objects** (`spate-s3`)

On the feed path, a `SplitDescriptor` may name two or more objects. The S3 source reads them whole, one after another in descriptor order, on one lane, and acknowledges the unit once after the last is read. If one of them fails, the whole unit is failed back to the feed and is dead-lettered at its cap with every object it names. A descriptor that names no object, or more than `spate_s3::MAX_UNIT_MEMBERS` (4,096), is failed back. `spate_s3_source_objects_remaining` counts each object of such a unit.
