**Breaking:** **An S3 source metric is renamed to follow the naming rules** (`spate-s3`)

The S3 source now exports `spate_s3_source_remaining_objects`. In previous versions it was `spate_s3_source_objects_remaining`. Its value is unchanged. Dashboards and alerts that select this series need the new name.

The naming rules for metric families and label keys are written in full under Instrumenting connectors § Contracts.
