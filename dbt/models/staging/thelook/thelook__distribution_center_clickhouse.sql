CREATE TABLE thelook.distribution_centers_local ON CLUSTER default (
    id UInt32,
    name String,
    latitude Float32,
    longitude Float32,
    sys_effective_date DateTime DEFAULT now(),
    sys_create_date DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY toYYYYMMDD(sys_effective_date)
ORDER BY name;

CREATE TABLE IF NOT EXISTS thelook.distribution_centers ON CLUSTER default AS thelook.distribution_centers_local
ENGINE = Distributed('default', thelook, distribution_centers_local, rand());

INSERT INTO thelook.distribution_centers (id, name, latitude, longitude)
SELECT * FROM s3('https://load-data-clickhouse.s3.ap-southeast-1.amazonaws.com/thelook_ecommerce.distribution_centers.csv', '', '', 'CSVWithNames');