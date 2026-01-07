CREATE TABLE clickhouse_db.my_clickhouse_table
(
    id Int32,
    col1 Nullable(Int32),
    col2 Nullable(Int32)
)
ENGINE = MergeTree() ORDER BY id;

CREATE TABLE clickhouse_db.my_clickhouse_table_simple
(
    id Int32
)
ENGINE = MergeTree() ORDER BY id;