**A stop before a bounded job ends says so in the log** (`spate-s3`)

A run that stops or fails before the worker sees its bounded job end logs one `info` line naming the bounded job. A stop exits `Completed` with units unread, and this line tells that run from one whose job ended.
