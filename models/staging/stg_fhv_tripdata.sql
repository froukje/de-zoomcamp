{{ config(materialized='view') }}

select 
  cast(pulocationid as integer) as pickup_locationid,
  cast(dolocationid as integer) as dropoff_locationid,
  cast(pickup_datetime as timestamp) as pickup_datetime,
  cast(dropoff_datetime as timestamp) as dropoff_datetime,
  cast(dispatching_base_num as integer) as dispatching_base_num,
  cast(Affiliated_base_number as integer) as Affiliated_base_number,
from {{ source('staging','fhv_tripdata') }}

{% if var('is_test_run', default=false) %}
limit 100
{%endif%}