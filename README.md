# AirPulse Africa

> Real-time air quality monitoring across African cities — from sensor to dashboard.

Air pollution is a leading cause of death in West Africa, yet real-time data remains
inaccessible to most people. AirPulse Africa streams live PM2.5, NO2, PM10 and other
pollutant readings from OpenAQ sensors across 5 African countries, processes them through
a fully automated pipeline, and visualises WHO threshold breaches on an interactive dashboard.

---

## The Data Story

Our pipeline revealed stark air quality realities across Africa:

- **Nigeria (PM2.5)**: 64.7% of all readings exceed WHO safe limits — nearly 2 in every 3 hours
- **Ethiopia (PM2.5)**: 100% breach rate — every single reading above the WHO threshold of 15 µg/m³
- **South Africa (PM10)**: Peak readings hit 180 µg/m³ — 4x the WHO limit of 45 µg/m³
- **Ghana (PM2.5)**: Average of 16.72 µg/m³ — just above the safe threshold
- **Kenya**: Relatively cleaner air — 26.7% breach rate for PM2.5

---

## Architecture

```
OpenAQ API
    │
    ▼
Python Producer (polls every 60s)
    │
    ▼
Redpanda (Kafka-compatible broker)
    │  air-quality-readings topic
    ├──────────────────────┐
    ▼                      ▼
Consumer (S3)        Consumer (Snowflake)
    │                      │
    ▼                      ▼
AWS S3 Data Lake     Snowflake RAW layer
(raw JSON, partitioned  (AIR_QUALITY_READINGS)
 by country/date)          │
                           ▼
                      dbt transforms
                      (staging → mart)
                           │
                           ▼
                    Streamlit Dashboard
                    (2 tiles, live data)
                           │
                    Prefect Orchestration
                    (schedules dbt every 5 min)
```

All infrastructure provisioned via **Terraform** (IaC).

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Data source | OpenAQ v3 REST API | Real-time air quality sensor readings |
| Message broker | Redpanda | Kafka-compatible streaming broker (Docker) |
| Producer | Python + confluent-kafka | Polls OpenAQ, publishes to Redpanda topic |
| Consumer 1 | Python + boto3 | Streams topic → AWS S3 data lake |
| Consumer 2 | Python + snowflake-connector | Streams topic → Snowflake RAW table |
| Cloud | AWS (S3 + IAM) | Data lake storage |
| IaC | Terraform | Provisions S3 bucket, IAM user and policy |
| Data warehouse | Snowflake | Clustered tables for analytics |
| Transformations | dbt (dbt-snowflake) | Staging view + mart tables |
| Orchestration | Prefect v2 | Schedules dbt runs every 5 minutes |
| Dashboard | Streamlit | Interactive 2-tile dashboard |
| Dev environment | WSL2 + Docker Engine + VS Code | Ubuntu on Windows |

---

## Dataset

- **Source**: [OpenAQ v3 API](https://api.openaq.org/v3) — free, open air quality data
- **Countries**: Nigeria (NG), Ghana (GH), Kenya (KE), South Africa (ZA), Ethiopia (ET)
- **Pollutants**: PM2.5, PM10, NO2, CO, O3, SO2
- **Poll interval**: Every 60 seconds
- **WHO thresholds used** (24h mean, µg/m³):

| Pollutant | WHO Limit |
|---|---|
| PM2.5 | 15 µg/m³ |
| PM10 | 45 µg/m³ |
| NO2 | 25 µg/m³ |
| CO | 4,000 µg/m³ |
| O3 | 100 µg/m³ |
| SO2 | 40 µg/m³ |

---

## Dashboard

The Streamlit dashboard has two tiles:

**Tile 1 — Pollutant distribution by country** (bar chart)
Shows average pollutant concentration per country with WHO threshold reference line.
Filter by pollutant using the sidebar.

**Tile 2 — Pollutant trend over time** (line chart)
Shows hourly average readings over the last 72 hours for a selected country and pollutant.
Breach count displayed below the chart.

---

## Data Pipeline Details

### Streaming layer
- Redpanda runs as a single-node broker in Docker
- Topic `air-quality-readings` has 3 partitions, 1 replica
- Producer polls OpenAQ per country, extracts sensor measurements, publishes JSON records
- Two consumers run in parallel — one writes to S3, one inserts into Snowflake

### Data lake (S3)
Files are partitioned by country and date:
```
s3://airpulse-africa-datalake/raw/{country}/{year}/{month}/{day}/{parameter}_{location_id}_{timestamp}.json
```

### Data warehouse (Snowflake)
Raw table clustered by `(COUNTRY, PARAMETER, DATE_TRUNC('day', MEASURED_AT))` for
efficient upstream queries.

### dbt models
```
raw.AIR_QUALITY_READINGS          ← streamed by consumer
    │
    ▼
staging.stg_air_quality           ← view: cleans nulls, negatives, standardises names
    │
    ├──▶ mart.mart_pollutant_distribution   ← table: aggregates per country+pollutant (Tile 1)
    └──▶ mart.mart_quality_timeseries       ← table: hourly averages per country+pollutant (Tile 2)
```

Both mart tables are clustered by `(COUNTRY, PARAMETER)` for fast dashboard queries.

---

## How to Reproduce

### Prerequisites
- WSL2 with Docker Engine installed
- AWS account with IAM user (`airpulse-pipeline`) having S3 + IAM permissions
- Snowflake free trial account
- OpenAQ free API key from https://explore.openaq.org/register
- Terraform v1.7+

### Step 1 — Clone and set up environment
```bash
git clone https://github.com/yourusername/airpulse-africa.git
cd airpulse-africa
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### Step 2 — Configure environment variables
```bash
cp .env.example .env
# Edit .env with your OpenAQ API key, AWS credentials, and Snowflake credentials
nano .env
```

### Step 3 — Provision AWS infrastructure
```bash
cd terraform
terraform init && terraform apply -auto-approve
terraform output -raw iam_secret_access_key   # copy into .env
cd ..
```

### Step 4 — Set up Snowflake
Run this in your Snowflake worksheet:
```sql
CREATE DATABASE AIRPULSE;
CREATE SCHEMA AIRPULSE.RAW;
CREATE SCHEMA AIRPULSE.STAGING;
CREATE SCHEMA AIRPULSE.MART;
ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
```

### Step 5 — Start Redpanda
```bash
docker compose up -d
docker exec redpanda rpk topic create air-quality-readings --partitions 3 --replicas 1
```

### Step 6 — Run the pipeline (4 separate terminals)
```bash
# Terminal 1 — Producer
source .venv/bin/activate && export $(grep -v '^#' .env | xargs)
python producer/producer.py

# Terminal 2 — S3 consumer
source .venv/bin/activate && export $(grep -v '^#' .env | xargs)
python consumer/consumer_s3.py

# Terminal 3 — Snowflake consumer
source .venv/bin/activate && export $(grep -v '^#' .env | xargs)
python consumer/consumer_snowflake.py

# Terminal 4 — Prefect orchestration
source .venv/bin/activate && export $(grep -v '^#' .env | xargs)
python run_pipeline.py
```

### Step 7 — Run dbt
```bash
export $(grep -v '^#' .env | xargs)
cd dbt/airpulse && dbt run && dbt test
```

### Step 8 — Launch dashboard
```bash
source .venv/bin/activate && export $(grep -v '^#' .env | xargs)
streamlit run dashboard/app.py
# Open http://localhost:8501
```

---

## Project Structure

```
airpulse-africa/
├── .env                          # secrets — gitignored
├── .gitignore
├── README.md
├── requirements.txt
├── docker-compose.yml            # Redpanda broker + console
├── run_pipeline.py               # single-command pipeline trigger
│
├── terraform/                    # IaC — AWS S3 + IAM
│   ├── providers.tf
│   ├── variables.tf
│   ├── main.tf
│   └── outputs.tf
│
├── producer/
│   └── producer.py               # OpenAQ -> Redpanda
│
├── consumer/
│   ├── consumer_s3.py            # Redpanda -> S3
│   └── consumer_snowflake.py     # Redpanda -> Snowflake
│
├── dbt/airpulse/
│   ├── dbt_project.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── models/
│       ├── staging/
│       │   ├── schema.yml
│       │   └── stg_air_quality.sql
│       └── mart/
│           ├── mart_pollutant_distribution.sql
│           └── mart_quality_timeseries.sql
│
├── flows/
│   └── pipeline_flow.py          # Prefect orchestration
│
└── dashboard/
    └── app.py                    # Streamlit dashboard
```

---

## Evaluation Criteria Coverage

| Criterion | Implementation | Score |
|---|---|---|
| Problem description | Clearly defined — air pollution monitoring across Africa | 4/4 |
| Cloud | AWS S3 + IAM provisioned via Terraform | 4/4 |
| Streaming | Redpanda broker + Python producer + 2 consumers | 4/4 |
| Data warehouse | Snowflake with CLUSTER BY on COUNTRY, PARAMETER, DATE | 4/4 |
| Transformations | dbt staging view + 2 mart tables with tests | 4/4 |
| Dashboard | Streamlit with 2 tiles (bar + line) | 4/4 |
| Reproducibility | Step-by-step README with copy-paste commands | 4/4 |

---

## Future Work

- Real-time alerting when WHO thresholds are breached (email/SMS via AWS SNS)
- Expand to more African countries (currently 5)
- Add a geographic map tile showing pollution hotspots
- CI/CD pipeline with GitHub Actions
- Deploy Streamlit dashboard to Streamlit Community Cloud for public access
- Add more pollutants (lead, benzene) as OpenAQ expands sensor coverage

---

## Acknowledgements

- [OpenAQ](https://openaq.org) — open air quality data platform
- [Redpanda](https://redpanda.com) — Kafka-compatible streaming
- [dbt Labs](https://getdbt.com) — data transformation framework
- [DE Zoomcamp 2026](https://github.com/DataTalksClub/data-engineering-zoomcamp) — course and project guidelines