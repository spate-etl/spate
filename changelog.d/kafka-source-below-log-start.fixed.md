**A Kafka source under `latest` resumes a commit below the log start at the log start** (`spate-kafka`)

Under `auto.offset.reset: latest`, the default, a committed offset below the log start resumes at the log start. In previous versions the fetch out of range moved the partition to its end, and the next commit passed every record the log still held in between, though no sink had written them. A group that was stopped or lagging while retention moved the log start past its commit reached this.

A committed offset past the log end, which a log truncation can leave, also resumes at the log start, where it moved to the end before. The partition's retained records are read again.

Records already skipped are not recovered. A partition with no committed offset still starts at its end under `latest`.
