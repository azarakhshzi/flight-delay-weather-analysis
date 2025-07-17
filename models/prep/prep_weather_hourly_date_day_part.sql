WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_hourly_weather')}}
),
add_features AS (
    SELECT *
		, timestamp::DATE AS date               -- only date (hours:minutes:seconds) as DATE data type
		, timestamp::time AS time                           -- only time (hours:minutes:seconds) as TIME data type
        , TO_CHAR(timestamp,'HH24:MI') as hour  -- time (hours:minutes) as TEXT data type
        , TO_CHAR(timestamp, 'FMmonth')::text AS month_name   -- month name as a TEXT
        , TO_CHAR(timestamp, 'FMday')::text AS weekday        -- weekday name as TEXT        
        , DATE_PART('day', timestamp) AS date_day
		, TO_CHAR(timestamp, 'FMMonth') AS date_month
		, TO_CHAR(timestamp, 'FMYYYY') AS date_year
		, DATE_PART('week', timestamp) AS cw
    FROM hourly_data
),
add_more_features AS (
    SELECT *
		,(CASE 
			WHEN time BETWEEN '00:00:00' AND '05:00:00' THEN 'night'
			WHEN time BETWEEN '06:00:00' AND '17:00:00' THEN 'day'
			WHEN time BETWEEN '18:00:00' AND '23:00:00' THEN 'evening'
		END) AS day_part
    FROM add_features
),
add_even_more_features AS (
    SELECT *
		,(CASE condition_code
    WHEN 1 THEN 'Clear'
    WHEN 2 THEN 'Fair'
    WHEN 3 THEN 'Cloudy'
    WHEN 4 THEN 'Overcast'
    WHEN 5 THEN 'Fog'
    WHEN 6 THEN 'Freezing Fog'
    WHEN 7 THEN 'Light Rain'
    WHEN 8 THEN 'Rain'
    WHEN 9 THEN 'Heavy Rain'
    WHEN 10 THEN 'Freezing Rain'
    WHEN 11 THEN 'Heavy Freezing Rain'
    WHEN 12 THEN 'Sleet'
    WHEN 13 THEN 'Heavy Sleet'
    WHEN 14 THEN 'Light Snowfall'
    WHEN 15 THEN 'Snowfall'
    WHEN 16 THEN 'Heavy Snowfall'
    WHEN 17 THEN 'Rain Shower'
    WHEN 18 THEN 'Heavy Rain Shower'
    WHEN 19 THEN 'Sleet Shower'
    WHEN 20 THEN 'Heavy Sleet Shower'
    WHEN 21 THEN 'Snow Shower'
    WHEN 22 THEN 'Heavy Snow Shower'
    WHEN 23 THEN 'Lightning'
    WHEN 24 THEN 'Hail'
    WHEN 25 THEN 'Thunderstorm'
    WHEN 26 THEN 'Heavy Thunderstorm'
    WHEN 27 THEN 'Storm'
    ELSE 'Unknown'
END) AS weather_condition,
(CASE 
    WHEN condition_code = 5 OR condition_code = 6 THEN true
    ELSE false
END) as is_foggy
    FROM add_more_features
)
SELECT *
FROM add_even_more_features