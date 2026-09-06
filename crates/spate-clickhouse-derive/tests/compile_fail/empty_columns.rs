use spate_clickhouse_derive::ClickHouseRow;

#[derive(ClickHouseRow)]
struct Empty {
    #[serde(skip)]
    internal_only: u64,
}

fn main() {}
