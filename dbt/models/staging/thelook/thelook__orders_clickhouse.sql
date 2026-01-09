CREATE TABLE IF NOT EXISTS thelook.orders_local ON CLUSTER 'default'
(
    order_id UInt64,
    user_id UInt64,
    status String,
    gender LowCardinality(String),
    created_at DateTime,
    returned_at Nullable(DateTime),
    shipped_at Nullable(DateTime),
    delivered_at Nullable(DateTime),
    num_of_item UInt32
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (created_at, user_id, order_id);

CREATE TABLE IF NOT EXISTS thelook.orders ON CLUSTER 'default' AS thelook.orders_local
ENGINE = Distributed('default', 'thelook', 'orders_local', rand());

INSERT INTO thelook.orders_local 
(
    order_id, user_id, status, gender, created_at, 
    returned_at, shipped_at, delivered_at, num_of_item
)
SELECT 
    order_id, 
    user_id, 
    status, 
    gender, 
    substring(created_at, 1, 19) AS created_at,
    substring(returned_at, 1, 19) AS returned_at,
    substring(shipped_at, 1, 19) AS shipped_at,
    substring(delivered_at, 1, 19) AS delivered_at,
    num_of_item
FROM s3(
    'https://load-data-clickhouse.s3.ap-southeast-1.amazonaws.com/thelook_ecommerce.orders.csv', 
    '', 
    '', 
    'CSVWithNames'
)
SETTINGS max_partitions_per_insert_block = 10000;