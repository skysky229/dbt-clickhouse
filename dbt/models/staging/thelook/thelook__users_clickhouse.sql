CREATE TABLE thelook.users_local ON CLUSTER default (
    id UInt32,
    first_name String,
    last_name String,
    email String,
    age UInt8,
    gender FixedString(1),
    state LowCardinality(String),
    street_address String,
    postal_code String,
    city LowCardinality(String),
    country LowCardinality(String),
    latitude Float64,
    longitude Float64,
    traffic_source LowCardinality(String),
    created_at DateTime,
    /* System columns */
    sys_effective_date DateTime DEFAULT now(),
    sys_create_date DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY (toYYYYMM(created_at), traffic_source)
ORDER BY (created_at, traffic_source, id);

CREATE TABLE IF NOT EXISTS thelook.users ON CLUSTER default AS thelook.users_local
ENGINE = Distributed('default', thelook, users_local, rand());

INSERT INTO thelook.users_local 
(
    id, first_name, last_name, email, age, gender, state, street_address, 
    postal_code, city, country, latitude, longitude, traffic_source, created_at
)
SELECT 
    id, first_name, last_name, email, age, gender, state, street_address, 
    postal_code, city, country, latitude, longitude, traffic_source,
    substring(created_at, 1, 19) AS created_at
FROM s3(
    'https://load-data-clickhouse.s3.ap-southeast-1.amazonaws.com/thelook_ecommerce.users.csv', 
    '', 
    '', 
    'CSVWithNames',
) 
SETTINGS max_partitions_per_insert_block = 1000;