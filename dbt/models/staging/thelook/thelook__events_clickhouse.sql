CREATE TABLE IF NOT EXISTS thelook.events_local ON CLUSTER 'default'
(
    id UInt64,
    user_id UInt64,
    sequence_number UInt32,
    session_id UUID,
    created_at DateTime,
    ip_address IPv4,
    city String,
    state String,
    postal_code String,
    browser String,
    traffic_source String,
    uri String,
    event_type String,
    sys_effective_date DateTime DEFAULT created_at,
    sys_create_date DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (created_at, user_id);

CREATE TABLE IF NOT EXISTS thelook.events ON CLUSTER 'default' AS events_local
ENGINE = Distributed('default', thelook, events_local, rand());


INSERT INTO thelook.events 
(
    id, user_id, sequence_number, session_id, created_at, 
    ip_address, city, state, postal_code, browser, 
    traffic_source, uri, event_type
)
SELECT 
    id, 
    user_id, 
    sequence_number, 
    session_id, 
    substring(created_at, 1, 19) AS created_at,
    ip_address, 
    city, 
    state, 
    postal_code, 
    browser, 
    traffic_source, 
    uri, 
    event_type
FROM s3(
    'https://load-data-clickhouse.s3.ap-southeast-1.amazonaws.com/thelook_ecommerce.events.csv', 
    '', 
    '', 
    'CSVWithNames'
);
