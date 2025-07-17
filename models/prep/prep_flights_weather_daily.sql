with flights as (
    select *
    from {{ ref('staging_flights') }}
),
weather as (
    select *
    from {{ ref('prep_weather_daily') }}
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

    -- All weather columns, no aliases
    w.avg_temp_c,
    w.min_temp_c,
    w.max_temp_c,
    w.precipitation_mm,
    w.max_snow_mm,
    w.avg_wind_direction,
    w.avg_wind_speed_kmh,
    w.wind_peakgust_kmh,
    w.avg_pressure_hpa,
    w.sun_minutes,
    w.date_day,
    w.date_month,
    w.date_year,
    w.date_week,
    w.month_name,
    w.weekday_name,
    w.season
from flights f
left join weather w
    on f.origin = w.airport_code
    and cast(f.flight_date as date) = w.date