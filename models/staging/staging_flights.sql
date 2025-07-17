with source as (
    select * from {{ ref('flights_filtered') }}
)
select
    cast(flight_date as timestamp)                 as flight_date,
    cast(dep_time as integer)                      as dep_time,
    cast(sched_dep_time as integer)                as sched_dep_time,
    cast(dep_delay as integer)                     as dep_delay,
    cast(arr_time as integer)                      as arr_time,
    cast(sched_arr_time as integer)                as sched_arr_time,
    cast(arr_delay as integer)                     as arr_delay,
    cast(airline as varchar)                       as airline,
    cast(tail_number as varchar)                   as tail_number,
    cast(flight_number as integer)                 as flight_number,
    cast(origin as varchar)                        as origin,
    cast(dest as varchar)                          as dest,
    cast(air_time as integer)                      as air_time,
    cast(actual_elapsed_time as integer)           as actual_elapsed_time,
    cast(distance as integer)                      as distance,
    cast(cancelled as integer)                     as cancelled,
    cast(diverted as integer)                      as diverted
from source;
