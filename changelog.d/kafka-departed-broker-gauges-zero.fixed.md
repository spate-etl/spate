**A broker that leaves the Kafka client's statistics reads down** (`spate-kafka`)

Its `broker_up` and latency gauges on the Kafka source, sink and feed go to `0`. In previous versions they kept their last values for the life of the process, so a broker that was up when it left read as up.
