
sql
Copy
Edit
with flights_weather as (
    select *
    from {{ ref('prep_flights_weather_daily') }}
),
airports as (
    select *
    from {{ ref('staging_airports') }}
)
select
    f.origin                as airport_code,
    a.name                  as airport_name,
    f.flight_date           as date,
    count(*)                as num_flights,
    avg(f.dep_delay)        as avg_dep_delay,
    avg(f.arr_delay)        as avg_arr_delay,
    sum(f.cancelled)        as num_cancelled,
    max(f.avg_temp_c)           as avg_temp_c,
    max(f.precipitation_mm)     as precipitation_mm,
    max(f.max_snow_mm)          as max_snow_mm,
    max(f.wind_peakgust_kmh)    as max_wind_peakgust_kmh,
    max(f.sun_minutes)          as max_sun_minutes,
    max(f.season)               as season,
    case 
        when f.flight_date = '2025-01-08' then 1
        else 0
    end as wildfire_main_day,
    case 
        when f.flight_date between '2025-01-07' and '2025-01-14' then 1
        else 0
    end as wildfire_affected_week
from flights_weather f
left join airports a
    on f.origin = a.faa
where f.origin in ('LAX', 'BUR', 'ONT', 'SNA', 'SMO')
group by
    f.origin, a.name, f.flight_date
order by
    f.origin, f.flight_date