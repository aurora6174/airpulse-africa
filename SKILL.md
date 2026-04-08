---
name: airpulse-africa
description: >
  Complete step-by-step guide to building the AirPulse Africa DE Zoomcamp 2026 capstone project.
  End-to-end streaming pipeline: OpenAQ real-time air quality data -> Redpanda -> AWS S3 + Snowflake -> dbt -> Streamlit dashboard.
  Stack: Python, Redpanda (Kafka-compatible), Terraform, AWS S3, Snowflake, dbt-snowflake, Prefect v2, Streamlit.
  Dev environment: WSL2 + Docker Engine + VS Code + Claude Code.
  Goal: Full marks (28/28) across all DE Zoomcamp evaluation criteria.
  Use this skill for ANY task in this project: environment setup, producer, consumers, Terraform,
  dbt models, Prefect flows, Streamlit dashboard, README, or GitHub submission.
  Always load this skill before writing any code or config for this project.
---

# AirPulse Africa — DE Zoomcamp 2026 Capstone

> Single source of truth for the AirPulse Africa end-to-end streaming pipeline.
> Load this file at the start of every Claude Code session before writing any code or config.

---

## Project Overview

**Name**: AirPulse Africa
**Tagline**: Real-time air quality monitoring across African cities — from sensor to dashboard
**Problem statement**: Air pollution is a leading cause of death in West Africa yet real-time
data remains inaccessible to most people. This pipeline streams live PM2.5, NO2 and CO
readings from OpenAQ sensors, lands them in Snowflake, transforms them with dbt, and
visualises WHO threshold breaches on a Streamlit dashboard.
**Deadline**: June 1, 2026 (DE Zoomcamp 2026)
**Evaluation target**: Full marks (28/28) across all criteria

---

## Data Source — OpenAQ v3 API

- **Base URL**: https://api.openaq.org/v3
- **Auth**: Free API key — register at https://explore.openaq.org/register
- **Key endpoints**:
  - `GET /locations?country=NG&limit=100` — list sensor stations per country
  - `GET /measurements?location_id={id}&limit=100` — latest readings per station
  - `GET /parameters` — list all pollutant parameters
- **Pollutants tracked**: pm25, pm10, no2, co, o3, so2
- **Countries in scope**: NG (Nigeria), GH (Ghana), KE (Kenya), ZA (South Africa), ET (Ethiopia)
- **Poll interval**: 60 seconds (safe within free tier rate limits)
- **Rate limits**: 10 req/s on free tier
- **WHO thresholds** (24h mean, µg/m³):
  - PM2.5: 15 | PM10: 45 | NO2: 25 | CO: 4000 | O3: 100 | SO2: 40

---

## Tech Stack (Locked In)

| Layer | Tool | Notes |
|---|---|---|
| Data source | OpenAQ v3 REST API | Free API key, African sensors |
| Message broker | Redpanda | Docker, Kafka-compatible |
| Producer | Python | `requests` + `confluent-kafka` |
| Consumer | Python | `confluent-kafka` + `boto3` + `snowflake-connector-python` |
| Cloud | AWS (free tier) | Region: us-east-1 |
| IaC | Terraform | v1.7+ — S3 bucket + IAM |
| Data lake | AWS S3 | Raw JSON, partitioned by country/date |
| Data warehouse | Snowflake | Free 30-day trial |
| Transformations | dbt | `dbt-snowflake` adapter |
| Orchestration | Prefect | v2, local server or Prefect Cloud free tier |
| Dashboard | Streamlit | 2 tiles — bar + line chart |
| Dev environment | WSL2 + Docker Engine + VS Code + Claude Code | Ubuntu on Windows |

---

## Repository Structure

```
airpulse-africa/
├── .env                          # secrets — gitignored
├── .gitignore
├── README.md
├── requirements.txt
├── docker-compose.yml            # Redpanda broker + console
│
├── terraform/                    # Phase 3 — IaC
│   ├── providers.tf
│   ├── variables.tf
│   ├── main.tf
│   └── outputs.tf
│
├── producer/                     # Phase 1
│   └── producer.py               # OpenAQ -> Redpanda topic
│
├── consumer/                     # Phase 2
│   ├── consumer_s3.py            # topic -> S3 data lake
│   └── consumer_snowflake.py     # topic -> Snowflake RAW table
│
├── dbt/                          # Phase 4
│   ├── dbt_project.yml
│   ├── profiles.yml              # gitignored
│   └── models/
│       ├── staging/
│       │   ├── schema.yml
│       │   └── stg_air_quality.sql
│       └── mart/
│           ├── mart_pollutant_distribution.sql   # Tile 1
│           └── mart_quality_timeseries.sql       # Tile 2
│
├── prefect/                      # Phase 5
│   └── pipeline_flow.py
│
└── dashboard/                    # Phase 6
    └── app.py
```

---

## Environment Variables (.env)

```env
# OpenAQ
OPENAQ_API_KEY=your_openaq_api_key
OPENAQ_BASE_URL=https://api.openaq.org/v3
OPENAQ_COUNTRIES=NG,GH,KE,ZA,ET
OPENAQ_PARAMETERS=pm25,pm10,no2,co,o3
POLL_INTERVAL_SECONDS=60

# Redpanda / Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:19092
KAFKA_TOPIC=air-quality-readings

# AWS
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=airpulse-africa-datalake

# Snowflake
SNOWFLAKE_ACCOUNT=your_account_id
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=AIRPULSE
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=SYSADMIN
```

---

## Phase 0 — Environment Setup (WSL2)

### Step 1 — System packages
```bash
sudo apt update && sudo apt install python3-pip python3-venv unzip curl -y
```

### Step 2 — Project folder + Python venv
```bash
mkdir ~/airpulse-africa && cd ~/airpulse-africa
python3 -m venv .venv
source .venv/bin/activate
```

### Step 3 — requirements.txt
```
confluent-kafka==2.3.0
requests==2.31.0
boto3==1.34.0
snowflake-connector-python==3.6.0
python-dotenv==1.0.0
prefect==2.16.0
dbt-snowflake==1.7.0
streamlit==1.32.0
pandas==2.2.0
```
```bash
pip install -r requirements.txt
```

### Step 4 — Terraform
```bash
wget https://releases.hashicorp.com/terraform/1.7.5/terraform_1.7.5_linux_amd64.zip
unzip terraform_1.7.5_linux_amd64.zip && sudo mv terraform /usr/local/bin/
terraform --version
```

### Step 5 — AWS CLI + configure
```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip && sudo ./aws/install
aws configure    # enter Access Key ID, Secret, region=us-east-1, output=json
aws sts get-caller-identity   # verify correct account
```

### Step 6 — Verify Docker
```bash
docker --version
docker compose version
```

### Step 7 — OpenAQ API key
Register at https://explore.openaq.org/register, copy your key into .env.

---

## Phase 1 — Redpanda + Producer

### docker-compose.yml
```yaml
version: "3.8"
services:
  redpanda:
    image: redpandadata/redpanda:latest
    container_name: redpanda
    command:
      - redpanda start
      - --overprovisioned
      - --smp 1
      - --memory 512M
      - --reserve-memory 0M
      - --node-id 0
      - --check=false
      - --kafka-addr PLAINTEXT://0.0.0.0:29092,OUTSIDE://0.0.0.0:19092
      - --advertise-kafka-addr PLAINTEXT://redpanda:29092,OUTSIDE://localhost:19092
    ports:
      - "19092:19092"
      - "9644:9644"

  redpanda-console:
    image: redpandadata/console:latest
    container_name: redpanda-console
    ports:
      - "8080:8080"
    environment:
      KAFKA_BROKERS: redpanda:29092
    depends_on:
      - redpanda
```

```bash
docker compose up -d
docker exec redpanda rpk topic create air-quality-readings --partitions 3 --replicas 1
docker exec redpanda rpk topic list
```

### producer/producer.py
```python
import time, json, requests, os
from datetime import datetime, timezone
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

API_KEY    = os.getenv("OPENAQ_API_KEY")
BASE_URL   = os.getenv("OPENAQ_BASE_URL", "https://api.openaq.org/v3")
COUNTRIES  = os.getenv("OPENAQ_COUNTRIES", "NG,GH,KE").split(",")
PARAMETERS = os.getenv("OPENAQ_PARAMETERS", "pm25,pm10,no2").split(",")
TOPIC      = os.getenv("KAFKA_TOPIC", "air-quality-readings")
BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
INTERVAL   = int(os.getenv("POLL_INTERVAL_SECONDS", 60))

HEADERS = {"X-API-Key": API_KEY, "Accept": "application/json"}
producer = Producer({"bootstrap.servers": BOOTSTRAP})
WHO_THRESHOLDS = {"pm25": 15, "pm10": 45, "no2": 25, "co": 4000, "o3": 100, "so2": 40}

def fetch_readings(country: str) -> list:
    loc_resp = requests.get(
        f"{BASE_URL}/locations",
        headers=HEADERS,
        params={"country": country, "limit": 50},
        timeout=15,
    )
    loc_resp.raise_for_status()
    locations = loc_resp.json().get("results", [])

    readings = []
    for loc in locations[:10]:
        loc_id = loc.get("id")
        city   = loc.get("locality") or loc.get("name", "unknown")
        lat    = loc.get("coordinates", {}).get("latitude")
        lon    = loc.get("coordinates", {}).get("longitude")

        m_resp = requests.get(
            f"{BASE_URL}/measurements",
            headers=HEADERS,
            params={"location_id": loc_id, "limit": 20},
            timeout=15,
        )
        if m_resp.status_code != 200:
            continue

        for m in m_resp.json().get("results", []):
            param = m.get("parameter", "")
            value = m.get("value")
            if param not in PARAMETERS or value is None:
                continue
            threshold = WHO_THRESHOLDS.get(param)
            readings.append({
                "location_id":   loc_id,
                "location_name": loc.get("name", "unknown"),
                "city":          city,
                "country":       country,
                "latitude":      lat,
                "longitude":     lon,
                "parameter":     param,
                "value":         value,
                "unit":          m.get("unit", "µg/m³"),
                "who_threshold": threshold,
                "exceeds_who":   (value > threshold) if threshold else None,
                "measured_at":   m.get("date", {}).get("utc"),
                "ingested_at":   datetime.now(timezone.utc).isoformat(),
            })
    return readings

def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[OK] {msg.topic()} [{msg.partition()}] offset={msg.offset()}")

def produce():
    print(f"AirPulse producer started - polling every {INTERVAL}s")
    while True:
        for country in COUNTRIES:
            try:
                readings = fetch_readings(country)
                for r in readings:
                    producer.produce(
                        TOPIC,
                        key=f"{r['country']}_{r['parameter']}",
                        value=json.dumps(r),
                        callback=delivery_report,
                    )
                producer.flush()
                print(f"[{country}] Produced {len(readings)} readings")
            except Exception as e:
                print(f"[ERROR] {country}: {e}")
        time.sleep(INTERVAL)

if __name__ == "__main__":
    produce()
```

Verify:
```bash
docker exec redpanda rpk topic consume air-quality-readings --num 5
```

---

## Phase 2 — Consumers

### consumer/consumer_s3.py
```python
import json, boto3, os
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

s3        = boto3.client("s3")
BUCKET    = os.getenv("S3_BUCKET_NAME")
TOPIC     = os.getenv("KAFKA_TOPIC", "air-quality-readings")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "s3-consumer-group",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])

def upload(record: dict):
    dt  = datetime.now(timezone.utc)
    key = (
        f"raw/{record['country']}/{dt.strftime('%Y/%m/%d')}/"
        f"{record['parameter']}_{record['location_id']}_{dt.strftime('%H%M%S%f')}.json"
    )
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(record),
                  ContentType="application/json")
    print(f"[S3] {key}")

print("S3 consumer running...")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[ERROR] {msg.error()}")
            continue
        upload(json.loads(msg.value().decode("utf-8")))
finally:
    consumer.close()
```

### consumer/consumer_snowflake.py
```python
import json, os, snowflake.connector
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    role=os.getenv("SNOWFLAKE_ROLE"),
)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS RAW.AIR_QUALITY_READINGS (
    LOCATION_ID     INTEGER,
    LOCATION_NAME   VARCHAR,
    CITY            VARCHAR,
    COUNTRY         VARCHAR(2),
    LATITUDE        FLOAT,
    LONGITUDE       FLOAT,
    PARAMETER       VARCHAR,
    VALUE           FLOAT,
    UNIT            VARCHAR,
    WHO_THRESHOLD   FLOAT,
    EXCEEDS_WHO     BOOLEAN,
    MEASURED_AT     TIMESTAMP_TZ,
    INGESTED_AT     TIMESTAMP_TZ,
    INSERTED_AT     TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (COUNTRY, PARAMETER, DATE_TRUNC('day', MEASURED_AT));
""")

consumer = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
    "group.id": "snowflake-consumer-group",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([os.getenv("KAFKA_TOPIC", "air-quality-readings")])

INSERT = """
INSERT INTO RAW.AIR_QUALITY_READINGS
  (LOCATION_ID, LOCATION_NAME, CITY, COUNTRY, LATITUDE, LONGITUDE,
   PARAMETER, VALUE, UNIT, WHO_THRESHOLD, EXCEEDS_WHO, MEASURED_AT, INGESTED_AT)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

print("Snowflake consumer running...")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[ERROR] {msg.error()}")
            continue
        r = json.loads(msg.value().decode("utf-8"))
        cur.execute(INSERT, (
            r["location_id"], r["location_name"], r["city"], r["country"],
            r["latitude"], r["longitude"], r["parameter"], r["value"],
            r["unit"], r["who_threshold"], r["exceeds_who"],
            r["measured_at"], r["ingested_at"],
        ))
        print(f"[Snowflake] {r['country']} {r['parameter']} = {r['value']} {r['unit']}")
finally:
    cur.close()
    conn.close()
    consumer.close()
```

---

## Phase 3 — Terraform (AWS)

### terraform/providers.tf
```hcl
terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}
provider "aws" { region = var.aws_region }
```

### terraform/variables.tf
```hcl
variable "aws_region"  { default = "us-east-1" }
variable "bucket_name" { default = "airpulse-africa-datalake" }
variable "project_tag" { default = "airpulse-africa" }
```

### terraform/main.tf
```hcl
resource "aws_s3_bucket" "datalake" {
  bucket        = var.bucket_name
  force_destroy = true
  tags = { Project = var.project_tag }
}

resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_iam_user" "airpulse" {
  name = "airpulse-pipeline"
  tags = { Project = var.project_tag }
}

resource "aws_iam_access_key" "airpulse" {
  user = aws_iam_user.airpulse.name
}

resource "aws_iam_user_policy" "s3_access" {
  name = "airpulse-s3-policy"
  user = aws_iam_user.airpulse.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:PutObject","s3:GetObject","s3:ListBucket","s3:DeleteObject"]
      Resource = [
        aws_s3_bucket.datalake.arn,
        "${aws_s3_bucket.datalake.arn}/*"
      ]
    }]
  })
}
```

### terraform/outputs.tf
```hcl
output "s3_bucket_name"        { value = aws_s3_bucket.datalake.bucket }
output "iam_access_key_id"     { value = aws_iam_access_key.airpulse.id }
output "iam_secret_access_key" {
  value     = aws_iam_access_key.airpulse.secret
  sensitive = true
}
```

```bash
cd terraform
terraform init && terraform plan && terraform apply -auto-approve
terraform output -raw iam_secret_access_key   # copy into .env
```

---

## Phase 4 — dbt (Snowflake)

### Snowflake setup (run once in Snowflake UI Worksheets)
```sql
CREATE DATABASE AIRPULSE;
CREATE SCHEMA AIRPULSE.RAW;
CREATE SCHEMA AIRPULSE.STAGING;
CREATE SCHEMA AIRPULSE.MART;
CREATE WAREHOUSE COMPUTE_WH WITH WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 60;
```

### dbt/models/staging/stg_air_quality.sql
```sql
{{ config(materialized='view') }}

SELECT
    LOCATION_ID,
    LOWER(LOCATION_NAME)                    AS location_name,
    LOWER(CITY)                             AS city,
    UPPER(COUNTRY)                          AS country,
    LATITUDE,
    LONGITUDE,
    LOWER(PARAMETER)                        AS parameter,
    VALUE                                   AS reading_value,
    UNIT,
    WHO_THRESHOLD,
    EXCEEDS_WHO,
    MEASURED_AT                             AS measured_at,
    DATE_TRUNC('hour', MEASURED_AT)         AS measured_hour,
    DATE(MEASURED_AT)                       AS measured_date,
    INGESTED_AT,
    CASE COUNTRY
        WHEN 'NG' THEN 'Nigeria'
        WHEN 'GH' THEN 'Ghana'
        WHEN 'KE' THEN 'Kenya'
        WHEN 'ZA' THEN 'South Africa'
        WHEN 'ET' THEN 'Ethiopia'
        ELSE COUNTRY
    END                                     AS country_name
FROM {{ source('raw', 'AIR_QUALITY_READINGS') }}
WHERE VALUE IS NOT NULL
  AND VALUE >= 0
  AND MEASURED_AT IS NOT NULL
```

### dbt/models/mart/mart_pollutant_distribution.sql
```sql
{{ config(
    materialized='table',
    cluster_by=['country', 'parameter']
) }}

SELECT
    country,
    country_name,
    parameter,
    COUNT(*)                                        AS reading_count,
    ROUND(AVG(reading_value), 2)                    AS avg_value,
    ROUND(MIN(reading_value), 2)                    AS min_value,
    ROUND(MAX(reading_value), 2)                    AS max_value,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
          (ORDER BY reading_value), 2)              AS p95_value,
    MAX(who_threshold)                              AS who_threshold,
    SUM(CASE WHEN exceeds_who THEN 1 ELSE 0 END)    AS breach_count,
    ROUND(
        100.0 * SUM(CASE WHEN exceeds_who THEN 1 ELSE 0 END) / COUNT(*), 1
    )                                               AS breach_pct,
    MAX(measured_at)                                AS last_reading_at
FROM {{ ref('stg_air_quality') }}
GROUP BY country, country_name, parameter
ORDER BY country, parameter
```

### dbt/models/mart/mart_quality_timeseries.sql
```sql
{{ config(
    materialized='table',
    cluster_by=['country', 'parameter', 'measured_hour']
) }}

SELECT
    country,
    country_name,
    parameter,
    measured_hour,
    measured_date,
    ROUND(AVG(reading_value), 2)                    AS avg_value,
    ROUND(MAX(reading_value), 2)                    AS max_value,
    MAX(who_threshold)                              AS who_threshold,
    COUNT(*)                                        AS reading_count,
    SUM(CASE WHEN exceeds_who THEN 1 ELSE 0 END)    AS breach_count
FROM {{ ref('stg_air_quality') }}
GROUP BY country, country_name, parameter, measured_hour, measured_date
ORDER BY country, parameter, measured_hour
```

### dbt/models/staging/schema.yml
```yaml
version: 2

sources:
  - name: raw
    database: AIRPULSE
    schema: RAW
    tables:
      - name: AIR_QUALITY_READINGS
        description: Raw air quality readings streamed from OpenAQ via Redpanda

models:
  - name: stg_air_quality
    description: Cleaned staging layer — normalised countries, filtered nulls
    columns:
      - name: location_id
        tests: [not_null]
      - name: parameter
        tests: [not_null, accepted_values: {values: ['pm25','pm10','no2','co','o3','so2']}]
      - name: reading_value
        tests: [not_null]
      - name: country
        tests: [not_null]

  - name: mart_pollutant_distribution
    description: Aggregate stats per country + pollutant — feeds dashboard Tile 1

  - name: mart_quality_timeseries
    description: Hourly averages per country + pollutant — feeds dashboard Tile 2
```

```bash
cd dbt && dbt init airpulse    # select snowflake adapter
dbt debug
dbt run && dbt test
dbt docs generate && dbt docs serve
```

---

## Phase 5 — Prefect Orchestration

### prefect/pipeline_flow.py
```python
import subprocess
from prefect import flow, task

@task(name="Health-check Redpanda", log_prints=True)
def check_broker():
    result = subprocess.run(
        ["docker", "exec", "redpanda", "rpk", "topic", "list"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception("Redpanda not reachable")

@task(name="Run dbt models", log_prints=True)
def run_dbt():
    result = subprocess.run(
        ["dbt", "run", "--project-dir", "dbt/", "--profiles-dir", "dbt/"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt run failed:\n{result.stderr}")

@task(name="Run dbt tests", log_prints=True)
def test_dbt():
    result = subprocess.run(
        ["dbt", "test", "--project-dir", "dbt/", "--profiles-dir", "dbt/"],
        capture_output=True, text=True
    )
    print(result.stdout)

@flow(name="AirPulse Africa Pipeline", log_prints=True)
def airpulse_pipeline():
    check_broker()
    run_dbt()
    test_dbt()
    print("Pipeline run complete.")

if __name__ == "__main__":
    airpulse_pipeline.serve(
        name="airpulse-scheduled",
        cron="*/5 * * * *"
    )
```

```bash
prefect server start
python prefect/pipeline_flow.py
```

---

## Phase 6 — Streamlit Dashboard

### dashboard/app.py
```python
import streamlit as st
import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

WHO_THRESHOLDS = {"pm25": 15, "pm10": 45, "no2": 25, "co": 4000, "o3": 100, "so2": 40}
PARAM_LABELS   = {"pm25": "PM2.5", "pm10": "PM10", "no2": "NO2",
                  "co": "CO", "o3": "O3", "so2": "SO2"}

st.set_page_config(page_title="AirPulse Africa", layout="wide")
st.title("AirPulse Africa")
st.caption("Real-time air quality across African cities · OpenAQ -> Redpanda -> Snowflake -> dbt")

@st.cache_resource
def get_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="MART",
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

@st.cache_data(ttl=300)
def load_distribution():
    df = pd.read_sql(
        "SELECT * FROM MART.MART_POLLUTANT_DISTRIBUTION ORDER BY COUNTRY, PARAMETER",
        get_conn()
    )
    df.columns = [c.lower() for c in df.columns]
    return df

@st.cache_data(ttl=300)
def load_timeseries(country: str, parameter: str):
    df = pd.read_sql(f"""
        SELECT MEASURED_HOUR, AVG_VALUE, WHO_THRESHOLD, BREACH_COUNT
        FROM MART.MART_QUALITY_TIMESERIES
        WHERE COUNTRY = '{country}' AND PARAMETER = '{parameter}'
        ORDER BY MEASURED_HOUR DESC LIMIT 72
    """, get_conn())
    df.columns = [c.lower() for c in df.columns]
    return df.sort_values("measured_hour")

dist_df = load_distribution()

st.sidebar.header("Filters")
countries  = sorted(dist_df["country_name"].unique().tolist())
selected_c = st.sidebar.multiselect("Countries", countries, default=countries)
parameters = sorted(dist_df["parameter"].unique().tolist())
selected_p = st.sidebar.selectbox("Pollutant", parameters)

filtered = dist_df[
    (dist_df["country_name"].isin(selected_c)) &
    (dist_df["parameter"] == selected_p)
]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Countries monitored", dist_df["country"].nunique())
col2.metric("Pollutants tracked",  dist_df["parameter"].nunique())
col3.metric("Total readings",      f"{dist_df['reading_count'].sum():,.0f}")
col4.metric("Avg WHO breach rate", f"{dist_df['breach_pct'].mean():.1f}%")

st.divider()

st.subheader(f"Tile 1 — Average {PARAM_LABELS.get(selected_p, selected_p)} by country")
if not filtered.empty:
    st.bar_chart(filtered.set_index("country_name")[["avg_value"]], use_container_width=True)
    threshold = WHO_THRESHOLDS.get(selected_p)
    if threshold:
        st.caption(f"WHO 24h guideline: {threshold} µg/m3")
    st.dataframe(
        filtered[["country_name","avg_value","max_value","p95_value","breach_pct","last_reading_at"]],
        use_container_width=True
    )

st.divider()

st.subheader(f"Tile 2 — {PARAM_LABELS.get(selected_p, selected_p)} trend (last 72 hours)")
country_code = dist_df[dist_df["country_name"].isin(selected_c)]["country"].iloc[0] \
    if not dist_df[dist_df["country_name"].isin(selected_c)].empty else "NG"
ts_df = load_timeseries(country_code, selected_p)
if not ts_df.empty:
    st.line_chart(ts_df.set_index("measured_hour")["avg_value"], use_container_width=True)
    threshold = WHO_THRESHOLDS.get(selected_p)
    if threshold:
        st.caption(f"WHO threshold: {threshold} µg/m3 — breaches in window: {ts_df['breach_count'].sum():.0f}")

st.caption("Refreshed every 5 minutes via Prefect + dbt")
```

```bash
streamlit run dashboard/app.py
```

---

## .gitignore
```
.env
.venv/
__pycache__/
*.pyc
dbt/profiles.yml
dbt/target/
dbt/.user.yml
terraform/.terraform/
terraform/terraform.tfstate
terraform/terraform.tfstate.backup
*.zip
awscliv2/
```

---

## Common Gotchas

- **OpenAQ location_id**: always fetch locations first, then measurements — never guess IDs
- **OpenAQ free tier**: add 0.5s sleep between location fetches if hitting 10 req/s limit
- **Snowflake account ID**: format `abc12345.us-east-1` — no `.snowflakecomputing.com`
- **dbt profiles.yml**: never commit — always in .gitignore
- **WSL2 Docker**: run all docker commands from WSL2 terminal, not Windows cmd
- **Redpanda port**: `19092` from Python (external); `29092` only between Docker containers
- **S3 bucket names**: globally unique — append initials if name is taken
- **Terraform state**: never commit `.tfstate` files — contain secrets
- **Snowflake auto-suspend**: set warehouse to auto-suspend 60s to stay on free tier

---

## Key Commands Reference

```bash
# Redpanda
docker compose up -d
docker exec redpanda rpk topic list
docker exec redpanda rpk topic consume air-quality-readings --num 5

# Run components (separate terminals, venv activated)
python producer/producer.py
python consumer/consumer_s3.py
python consumer/consumer_snowflake.py

# Terraform
cd terraform && terraform init && terraform plan && terraform apply -auto-approve

# dbt
cd dbt && dbt debug && dbt run && dbt test
dbt docs generate && dbt docs serve

# Prefect
prefect server start
python prefect/pipeline_flow.py

# Dashboard
streamlit run dashboard/app.py
```

---

## README Sections (for full marks)

1. Project title + tagline
2. Problem statement — why air quality in Africa matters
3. Architecture diagram — screenshot of pipeline
4. Tech stack table
5. Dataset — OpenAQ v3, countries, pollutants, poll interval
6. Dashboard screenshots — Tile 1 (bar) + Tile 2 (line)
7. Key findings — e.g. "Kano PM2.5 exceeds WHO limits X% of hours measured"
8. How to reproduce — phases 0-6 with copy-paste commands
9. Future work — alerting, more countries, mobile app
