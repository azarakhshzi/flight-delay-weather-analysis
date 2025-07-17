with flights as (
    select *
    from {{ ref('staging_flights') }}
),
weather as (
    select *
    from {{ ref('staging_weather_daily') }}
)
select
    f.flight_date,
    f.dep_time,
    f.sched_dep_time,
    f.dep_delay,
    f.arr_time,
    f.sched_arr_time,
    f.arr_delay,
    f.airline,
    f.tail_number,
    f.flight_number,
    f.origin,
    f.dest,
    f.air_time,
    f.actual_elapsed_time,
    f.distance,
    f.cancelled,
    f.diverted,
    w.temp           as daily_temp,
    w.precip         as daily_precip,
    w.weather_event  as daily_weather_event
from flights f
left join weather w
    on f.origin = w.airport_code
    and cast(f.flight_date as date) = cast(w.date as date)