**Avro map allocation** (`spate-avro`)

`AvroDatumDeserializer` now uses the first block's entry count to reserve space
when decoding a map into a `HashMap`. Previously, the map started with no
reserved space and grew as entries arrived. This reduces allocation and
rehashing while decoding the first block; later blocks may still require the
table to grow.

Malformed input can cause a reservation before decoding fails. The size hint
is limited by the bytes remaining in the payload, and serde also caps the
number of entries it reserves. For `HashMap<String, i64>`, a sufficiently large
malformed payload can cause an allocation of about 2 MiB.

A custom visitor that reads no entries now rejects a malformed first block
header or a count above the decoding budget. Previously, that visitor could
accept the datum without checking the header. Array decoding is unchanged.
