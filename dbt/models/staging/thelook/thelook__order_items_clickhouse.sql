CREATE TABLE IF NOT EXISTS thelook.order_items_local ON CLUSTER 'default'
(
    id UInt64,
    order_id UInt64,
    user_id UInt64,
    product_id UInt64,
    inventory_item_id UInt64,
    status String,
    created_at DateTime,
    shipped_at Nullable(DateTime),
    delivered_at Nullable(DateTime),
    returned_at Nullable(DateTime),
    sale_price Float64,
    sys_effective_date DateTime DEFAULT created_at,
    sys_create_date DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (created_at, user_id, order_id, product_id, inventory_item_id);

CREATE TABLE IF NOT EXISTS thelook.order_items ON CLUSTER 'default' AS thelook.order_items_local
ENGINE = Distributed('default', 'thelook', 'order_items_local', rand());

INSERT INTO thelook.order_items 
(
    id, order_id, user_id, product_id, inventory_item_id, 
    status, created_at, shipped_at, delivered_at, returned_at, 
    sale_price
)
SELECT 
    id, 
    order_id, 
    user_id, 
    product_id, 
    inventory_item_id, 
    status, 
    -- Handling the ' UTC' suffix for all date columns
    substring(created_at, 1, 19) AS created_at,
    substring(shipped_at, 1, 19) AS shipped_at,
    substring(delivered_at, 1, 19) AS delivered_at,
    substring(returned_at, 1, 19) AS returned_at,
    sale_price
FROM s3(
    'https://load-data-clickhouse.s3.ap-southeast-1.amazonaws.com/thelook_ecommerce.order_items.csv', 
    '', 
    '', 
    'CSVWithNames'
)
SETTINGS max_partitions_per_insert_block = 10000;