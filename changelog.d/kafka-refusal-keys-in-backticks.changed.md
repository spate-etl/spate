**Two Kafka source refusals put the passthrough key in backticks** (`spate-kafka`)

`consumers` above 1 with a static member id now reads ``source.kafka.consumers: above 1 cannot be combined with `rdkafka."group.instance.id"`: …; set `consumers` to 1 or remove the static member id``. A `tls.root_ca` under a protocol other than `ssl` or `sasl_ssl` reads ``tls.root_ca: needs `rdkafka."security.protocol"` set to ssl or sasl_ssl, …``. The same values are refused as before.
