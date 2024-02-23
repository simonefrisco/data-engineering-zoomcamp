{{ config(materialized='table') }}

with
    fhv_tripdata as (
        select *, 'Fhv' as service_type from {{ ref('stg_fhv_tripdata') }}
    ),
    dim_zones as (select * from {{ ref('dim_zones') }} where borough != 'Unknown')
select
    fhv_tripdata.pickup_locationid,
    fhv_tripdata.dropoff_locationid,

    -- timestamps
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,
    
    -- trip info
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.sr_flag,
    fhv_tripdata.Affiliated_base_number affiliated_base_number,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    pickup_zone.zone as dropoff_zone

from fhv_tripdata

inner join
    dim_zones as pickup_zone 
    on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join
    dim_zones as dropoff_zone
    on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid

WHERE fhv_tripdata.pickup_locationid IS NOT NULL AND fhv_tripdata.dropoff_locationid IS NOT NULL