{{ config(materialized='view') }}
 
with tripdata as 
(
  select *,
    row_number() over(partition by vendor_id, tpep_pickup_datetime) as rn
  from {{ source('staging','yellow_taxi') }}
  where vendor_id is not null 
)
select
   -- identifiers
    {{ dbt_utils.generate_surrogate_key(['vendor_id', 'tpep_pickup_datetime']) }} as tripid,    
    {{ dbt.safe_cast("vendor_id", api.Column.translate_type("integer")) }} as vendorid,
    {{ dbt.safe_cast("ratecode_id", api.Column.translate_type("integer")) }} as ratecodeid,
    {{ dbt.safe_cast("pu_location_id", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("do_location_id", api.Column.translate_type("integer")) }} as dropoff_locationid,

    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    {{ dbt.safe_cast("passenger_count", api.Column.translate_type("integer")) }} as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    -- yellow cabs are always street-hail
    1 as trip_type,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(0 as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    coalesce({{ dbt.safe_cast("payment_type", api.Column.translate_type("integer")) }},0) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description
from tripdata
where rn = 1

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var("is_test_run", env_var("IS_TEST_RUN", true)) %}

  limit 100

{% endif %}