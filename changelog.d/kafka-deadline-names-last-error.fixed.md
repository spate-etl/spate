**The Kafka source's assignment deadline error names the last consumer error** (`spate-kafka`)

When `startup_timeout` or `assignment_timeout` passes, the error that stops
the pipeline ends with the last consumer error the source saw and how long
ago it arrived, for example
`last consumer error 4.976206s ago: consumer poll: … Connection refused`.
`AllBrokersDown` is skipped, so the error names the broker failure behind it.
In previous versions the error named only the deadline, the topic and the
brokers or the group, and the reason was only in the log. An accepted
assignment clears the recorded error.
