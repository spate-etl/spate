**The Kafka source refuses a subscription pattern as its topic** (`spate-kafka`)

A `source.kafka.topic` with a leading `^` fails pipeline load with an error that names the topic. Previously the pipeline opened and delivered no record, with no error. A pattern never read a record, so no working pipeline is affected.
