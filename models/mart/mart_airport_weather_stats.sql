with flights_weather as (
    select *
    from {{ ref('prep_flights_weather_daily') }}
),
airports as (
    select *
    from {{ ref('prep_airports') }}
)
select
    f.origin                   as airport_code,
    a.name                     as airport_name,
    a.city                     as airport_city,
    f.flight_date              as date,
    count(*)                   as num_flights,
    avg(f.dep_delay)           as avg_dep_delay,
    avg(f.arr_delay)           as avg_arr_delay,
    sum(f.cancelled)           as num_cancelled,
    max(f.avg_temp_c)          as avg_temp_c,
    max(f.min_temp_c)          as min_temp_c,
    max(f.max_temp_c)          as max_temp_c,
    max(f.precipitation_mm)    as precipitation_mm,
    max(f.max_snow_mm)         as max_snow_mm,
    max(f.avg_wind_direction)  as avg_wind_direction,
    max(f.avg_wind_speed_kmh)  as avg_wind_speed_kmh,
    max(f.wind_peakgust_kmh)   as wind_peakgust_kmh,
    max(f.avg_pressure_hpa)    as avg_pressure_hpa,
    max(f.sun_minutes)         as sun_minutes,
    max(f.season)              as season
from flights_weather f
left join airports a
    on f.origin = a.faa
group by
    f.origin, a.name, a.city, f.flight_date