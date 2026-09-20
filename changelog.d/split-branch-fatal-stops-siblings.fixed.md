**Split branches stop receiving records after a fatal error** (`spate-core`)

Once a split-sink branch records a fatal error, the split now stops forwarding
records from that payload to every branch. Previously, other branches could
continue encoding records and sending chunks before the failure stopped the
pipeline. The batch still fails and is eligible for replay, but its remaining
records no longer cause additional writes through sibling branches.

Those records no longer increase the split's `spate_operator_records_out_total`
or a branch's `spate_operator_records_in_total`. They also cannot add fatal
errors in other branches or count as unmatched records through
`spate_operator_records_dropped_total{reason="unrouted"}` or the split's fatal
error counter. The split's `spate_operator_records_in_total` still counts
records it receives.
