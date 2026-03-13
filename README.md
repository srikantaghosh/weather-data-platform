# Weather Data Platform

**End-to-end ELT pipeline** built with Dagster, dbt, and Snowflake — pulling live weather data from the Open-Meteo API.

## Architecture

```
Free API (Open-Meteo Weather)
        │
        ▼
   ┌─────────┐
   │  Dagster │  ← Python runtime / orchestrator
   │  (ELT)   │
   └────┬─────┘
        │  Raw JSON → Snowflake
        ▼
┌───────────────┐
│   Snowflake   │
│  RAW schema   │  ← Landing zone
└───────┬───────┘
        │
   ┌────▼────┐
   │   dbt   │  ← Transformation layer
   └────┬────┘
        │
        ▼
┌───────────────────────────────┐
│         Snowflake             │
│  STAGING → INTERMEDIATE →    │
│            MARTS              │
└───────────────────────────────┘
```

## Tech Stack

| Tool | Purpose |
|------|---------|
| **Open-Meteo API** | Free weather data (no API key needed) |
| **Dagster** | Orchestration & Python runtime |
| **Snowflake** | Cloud data warehouse |
| **dbt** | SQL transformation (staging → intermediate → marts) |
| **Git** | Version control with feature-branch workflow |

## Data Models

### Staging
- `stg_weather__daily_forecast` — cleaned and renamed raw weather data

### Intermediate
- `int_weather__daily_metrics` — enriched with Fahrenheit conversions, weather descriptions, alert flags

### Marts
- `mart_weather__city_summary` — aggregated 7-day summary per city
- `mart_weather__extreme_days` — days with extreme heat, freezing, heavy rain, or high winds

## Cities Tracked

London · New York · Tokyo · Sydney · Mumbai

## Setup

### Prerequisites
- Python 3.9+
- Snowflake account
- Git

### Installation

```bash
git clone https://github.com/srikantaghosh/weather-data-platform.git
cd weather-data-platform
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Configure Snowflake

```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
```

### Run dbt

```bash
cd dbt_project
dbt deps
dbt run
dbt test
```

### Run Dagster

```bash
dagster dev -w workspace.yaml
# Open http://localhost:3000
```

## Branch Strategy

```
main          ← Production (protected, PRs only)
  └── develop ← Integration branch
       ├── feature/dagster-ingestion
       ├── feature/dbt-models
       └── feature/scheduling
```

## License

Private — Internal use only.
