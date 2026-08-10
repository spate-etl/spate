**Coordinator links count toward broker health** (`spate-kafka`) — the
`spate_kafka_source_broker_up` and `spate_kafka_sink_broker_up` gauges report a
broker as up while any connection to it is up, including a logical coordinator
link. A broker serving only as group coordinator previously rendered 0 for the
life of the process — even while commits flowed through it — because the client
never reopens a regular connection it has no fetch-reason for, so brokers-up
panels undercounted after any coordinator-broker outage. ([#197])
