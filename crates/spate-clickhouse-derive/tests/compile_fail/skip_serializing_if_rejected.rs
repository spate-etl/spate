use spate_clickhouse_derive::ClickHouseRow;

#[derive(ClickHouseRow)]
struct Order {
    id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    note: Option<String>,
}

fn main() {}
