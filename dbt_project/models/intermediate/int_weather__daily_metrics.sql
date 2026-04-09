with daily as (
    select * from {{ ref('stg_weather__daily_forecast') }}
),

enriched as (
    select
        *,

        -- Temperature spread
        temperature_max_celsius - temperature_min_celsius
            as temperature_range_celsius,

        -- Convert to Fahrenheit
        (temperature_max_celsius * 9/5) + 32
            as temperature_max_fahrenheit,
        (temperature_min_celsius * 9/5) + 32
            as temperature_min_fahrenheit,

        -- Weather description from WMO code
        case
            when weather_code in (0)         then 'Clear Sky'
            when weather_code in (1, 2, 3)   then 'Partly Cloudy'
            when weather_code in (45, 48)    then 'Fog'
            when weather_code in (51, 53, 55, 56, 57) then 'Drizzle'
            when weather_code in (61, 63, 65, 66, 67) then 'Rain'
            when weather_code in (71, 73, 75, 77)     then 'Snow'
            when weather_code in (80, 81, 82)         then 'Rain Showers'
            when weather_code in (85, 86)             then 'Snow Showers'
            when weather_code in (95, 96, 99)         then 'Thunderstorm'
            else 'Unknown'
        end as weather_description,

        -- Flags
        case when precipitation_total_mm > 0 then true else false end
            as is_rainy_day,
        case when wind_speed_max_kmh > 50 then true else false end
            as is_high_wind_day

    from daily
)

select * from enriched