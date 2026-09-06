use spate_clickhouse_derive::ClickHouseRow;

#[derive(ClickHouseRow)]
struct Tags {
    id: u64,
    #[serde(rename = "tags.")]
    trailing_dot: u64,
    #[serde(rename = ".key")]
    leading_dot: u64,
    #[serde(rename = "a..b")]
    empty_segment: u64,
    #[serde(rename = "tags.k ey")]
    embedded_space: u64,
}

fn main() {}
