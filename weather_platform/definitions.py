from dagster import Definitions
from dagster_dbt import DbtCliResource

from weather_platform.assets.ingestion import raw_weather_forecast
from weather_platform.resources.snowflake import snowflake_resource

import os

defs = Definitions(
    assets=[raw_weather_forecast],
    resources={
        "snowflake": snowflake_resource,
    },
)