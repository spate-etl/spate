use spate_clickhouse_derive::ClickHouseRow;

#[derive(ClickHouseRow)]
struct TupleRow(u64, String);

fn main() {}
