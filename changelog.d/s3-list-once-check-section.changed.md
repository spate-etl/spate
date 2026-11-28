**A lister run asks its feed to check the `feed:` section** (`spate-s3`)

`S3Source::list_once` hands the section to the feed's `check_section` before it sends any request, and fails with the feed's refusal, as a worker's run does at startup.
