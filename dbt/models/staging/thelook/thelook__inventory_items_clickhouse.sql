CREATE TABLE IF NOT EXISTS thelook.inventory_items_local ON CLUSTER 'default'
(
    id UInt64,
    product_id UInt64,
    created_at DateTime,
    sold_at Nullable(DateTime),
    cost Float64,
    product_category String,
    product_name String,
    product_brand String,
    product_retail_price Float64,
    product_department String,
    product_sku String,
    product_distribution_center_id UInt32,
    sys_effective_date DateTime DEFAULT created_at,
    sys_create_date DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (created_at, product_id);

CREATE TABLE IF NOT EXISTS thelook.inventory_items ON CLUSTER 'default' AS thelook.inventory_items_local
ENGINE = Distributed('default', 'thelook', 'inventory_items_local', rand());

INSERT INTO thelook.inventory_items 
(
    id, product_id, created_at, sold_at, cost, 
    product_category, product_name, product_brand, 
    product_retail_price, product_department, 
    product_sku, product_distribution_center_id
)
SELECT 
    id, 
    product_id, 
    parseDateTimeBestEffortOrZero(created_at_str) AS created_at,
    parseDateTimeBestEffortOrNull(sold_at_str) AS sold_at,
    cost,
    product_category, 
    product_name, 
    product_brand, 
    product_retail_price, 
    product_department, 
    product_sku, 
    product_distribution_center_id
FROM s3(
    'https://load-data-clickhouse.s3.ap-southeast-1.amazonaws.com/thelook_ecommerce.inventory_items.csv', 
    '', 
    '', 
    'CSVWithNames',
    'id UInt32, product_id UInt32, created_at_str String, sold_at_str String, cost Decimal(18, 4), product_category LowCardinality(String), product_name String, product_brand LowCardinality(String), product_retail_price Decimal(18, 4), product_department LowCardinality(String), product_sku String, product_distribution_center_id UInt32'
);