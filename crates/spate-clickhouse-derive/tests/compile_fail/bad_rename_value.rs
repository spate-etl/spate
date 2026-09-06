use spate_clickhouse_derive::ClickHouseRow;

#[derive(ClickHouseRow)]
struct Order {
    id: u64,
    #[serde(rename = 123)]
    order_id: u64,
}

fn main() {}
