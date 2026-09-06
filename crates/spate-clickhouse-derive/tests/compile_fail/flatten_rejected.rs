use spate_clickhouse_derive::ClickHouseRow;

#[derive(ClickHouseRow)]
struct Order {
    id: u64,
    #[serde(flatten)]
    extra: Extra,
}

struct Extra {
    region: String,
}

fn main() {}
