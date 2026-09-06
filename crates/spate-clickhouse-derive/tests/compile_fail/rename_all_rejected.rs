use spate_clickhouse_derive::ClickHouseRow;

#[derive(ClickHouseRow)]
#[serde(rename_all = "camelCase")]
struct Order {
    order_id: u64,
    customer_id: u64,
}

fn main() {}
