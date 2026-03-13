#his pulls weather data from Open-Meteo for multiple cities and loads it into Snowflake RAW schema

import requests
import json
from datetime import datetime
from dagster import asset, AssetExecutionContext
from dagster_snowflake import SnowflakeResource


CITIES = {
    "London":    {"lat": 51.51, "lon": -0.13},
    "New_York":  {"lat": 40.71, "lon": -74.01},
    "Tokyo":     {"lat": 35.68, "lon": 139.69},
    "Sydney":    {"lat": -33.87, "lon": 151.21},
    "Mumbai":    {"lat": 19.08, "lon": 72.88},
}

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_weather(city: str, lat: float, lon: float) -> list[dict]:
    """Fetch 7-day daily forecast from Open-Meteo."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "windspeed_10m_max",
            "weathercode",
        ],
        "timezone": "auto",
        "forecast_days": 7,
    }
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    daily = data["daily"]
    for i, date in enumerate(daily["time"]):
        rows.append({
            "city": city,
            "forecast_date": date,
            "temp_max_c": daily["temperature_2m_max"][i],
            "temp_min_c": daily["temperature_2m_min"][i],
            "precipitation_mm": daily["precipitation_sum"][i],
            "wind_max_kmh": daily["windspeed_10m_max"][i],
            "weather_code": daily["weathercode"][i],
            "latitude": lat,
            "longitude": lon,
            "ingested_at": datetime.utcnow().isoformat(),
        })
    return rows


@asset(
    group_name="ingestion",
    description="Raw weather forecasts from Open-Meteo API → Snowflake RAW schema",
)
def raw_weather_forecast(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> None:
    """Pull weather data for multiple cities and load into Snowflake."""
    all_rows = []
    for city, coords in CITIES.items():
        context.log.info(f"Fetching weather for {city}...")
        rows = fetch_weather(city, coords["lat"], coords["lon"])
        all_rows.extend(rows)

    context.log.info(f"Fetched {len(all_rows)} rows total")

    # Create table if not exists and insert
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("USE SCHEMA RAW")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS RAW_WEATHER_FORECAST (
                city            VARCHAR,
                forecast_date   DATE,
                temp_max_c      FLOAT,
                temp_min_c      FLOAT,
                precipitation_mm FLOAT,
                wind_max_kmh    FLOAT,
                weather_code    INT,
                latitude        FLOAT,
                longitude       FLOAT,
                ingested_at     TIMESTAMP_NTZ
            )
        """)

        # Truncate + reload pattern (idempotent)
        cursor.execute("TRUNCATE TABLE RAW_WEATHER_FORECAST")

        for row in all_rows:
            cursor.execute(
                """
                INSERT INTO RAW_WEATHER_FORECAST
                (city, forecast_date, temp_max_c, temp_min_c,
                 precipitation_mm, wind_max_kmh, weather_code,
                 latitude, longitude, ingested_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row["city"], row["forecast_date"],
                    row["temp_max_c"], row["temp_min_c"],
                    row["precipitation_mm"], row["wind_max_kmh"],
                    row["weather_code"], row["latitude"],
                    row["longitude"], row["ingested_at"],
                ),
            )

        context.log.info("Loaded data into RAW.RAW_WEATHER_FORECAST")