
# Homework

I create a Mage Pipeline week_2\mage-zoomcamp\magic-zoomcamp\pipelines\web_to_gcs in order to upload a parquet file in my bucket .

- https://storage.cloud.google.com/mage-zoomcamp-sf879/green/green_tripdata_2022.parquet

## Init Tables
```

CREATE OR REPLACE EXTERNAL TABLE `cedar-unison-413811.ny_taxi.EXTERNAL_TABLE_green_taxi_2022` OPTIONS (
        format = 'PARQUET',
        uris = ['gs://mage-zoomcamp-sf879/green/*']
    );


CREATE OR REPLACE TABLE `cedar-unison-413811.ny_taxi.NORMAL_TABLE_green_taxi_2022`
AS SELECT * FROM        `cedar-unison-413811.ny_taxi.EXTERNAL_TABLE_green_taxi_2022`;

CREATE OR REPLACE TABLE `cedar-unison-413811.ny_taxi.PARTITIONED_TABLE_green_taxi_2022`
PARTITION BY DATE(DAY_lpep_pickup_datetime)
 AS (
  SELECT DATETIME(DATE(TIMESTAMP_MILLIS(CAST(lpep_pickup_datetime/1000000 AS INT)))) AS DAY_lpep_pickup_datetime, * FROM `cedar-unison-413811.ny_taxi.NORMAL_TABLE_green_taxi_2022`
);
```

## Results:

1) 
```

SELECT COUNT(*) FROM `cedar-unison-413811.ny_taxi.NORMAL_TABLE_green_taxi_2022`
```

2) 

```
SELECT COUNT(DISTINCT(PULocationID))
FROM `cedar-unison-413811.ny_taxi.NORMAL_TABLE_green_taxi_2022`;

SELECT COUNT(DISTINCT(PULocationID))
FROM `cedar-unison-413811.ny_taxi.EXTERNAL_TABLE_green_taxi_2022`;
```


3) 

```
SELECT COUNT(*) FROM `cedar-unison-413811.ny_taxi.NORMAL_TABLE_green_taxi_2022`
WHERE fare_amount  = 0
```

4) 


```
CREATE OR REPLACE TABLE `cedar-unison-413811.ny_taxi.OPT_PARTITIONED_TABLE_green_taxi_2022`
PARTITION BY DATE(DAY_lpep_pickup_datetime)
CLUSTER BY PUlocationID AS (
  SELECT DATETIME(TIMESTAMP_MILLIS(CAST(lpep_pickup_datetime/1000000 AS INT))) AS DAY_lpep_pickup_datetime, * FROM `cedar-unison-413811.ny_taxi.native_green_taxi_2022`
);
```


