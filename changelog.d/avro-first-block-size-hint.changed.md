**Avro map decoding reserves its table from the first block header**
(`spate-avro`) — `AvroDatumDeserializer` reads a map's first block count before
serde builds the table, so a decoded `HashMap` starts at that block's size and
stops rehashing its way up. A map spanning several blocks still rehashes at each
later boundary. Malformed input claiming a large first block reserves against
that claim before failing: on a short payload the reservation is held to the
bytes present, and above that serde caps the entry *count*, which for a 32-byte
key/value pair works out near 2 MiB once the table rounds its bucket count up to
a power of two. A custom visitor that returns without reading an entry now
validates the block header rather than passing it unexamined. Arrays are
unchanged.
