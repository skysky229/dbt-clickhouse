CREATE TABLE thelook.products_local ON CLUSTER default (
    id UInt32,
    cost Decimal(18, 4),
    category LowCardinality(String),
    name String,
    brand LowCardinality(String),
    retail_price Decimal(18, 4),
    department LowCardinality(String),
    sku String,
    distribution_center_id UInt32,
    /* System columns */
    sys_effective_date DateTime DEFAULT now(),
    sys_create_date DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY distribution_center_id
ORDER BY (distribution_center_id, category, id);

CREATE TABLE IF NOT EXISTS thelook.products ON CLUSTER default AS thelook.products_local
ENGINE = Distributed('default', thelook, products_local, rand());

INSERT INTO thelook.products (id, cost, category, name, brand, retail_price, department, sku, distribution_center_id)
SELECT * 

FROM s3(
    'https://load-data-clickhouse.s3.ap-southeast-1.amazonaws.com/thelook_ecommerce.users.csv', 
    '', 
    '', 
    'CSVWithNames'
);