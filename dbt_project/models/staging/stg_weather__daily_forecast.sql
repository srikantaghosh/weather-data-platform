with source as (
    select * from {{ source('raw', 'RAW_WEATHER_FORECAST') }}
),

renamed as (
    select
        city,
        forecast_date,
        weather_code,
        latitude,
        longitude,

        temp_max_c                          as temperature_max_celsius,
        temp_min_c                          as temperature_min_celsius,
        (temp_max_c + temp_min_c) / 2       as temperature_avg_celsius,
        precipitation_mm                    as precipitation_total_mm,
        wind_max_kmh                        as wind_speed_max_kmh,

        ingested_at

    from source
)

select * from renamed