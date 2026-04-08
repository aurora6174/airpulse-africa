import streamlit as st
import pandas as pd
import os
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from dotenv import load_dotenv

load_dotenv()

WHO_THRESHOLDS = {"pm25": 15, "pm10": 45, "no2": 25, "co": 4000, "o3": 100, "so2": 40}
PARAM_LABELS   = {"pm25": "PM2.5", "pm10": "PM10", "no2": "NO2",
                  "co": "CO", "o3": "O3", "so2": "SO2"}

st.set_page_config(page_title="AirPulse Africa", layout="wide")
st.title("AirPulse Africa")
st.caption("Real-time air quality across African cities · OpenAQ -> Redpanda -> Snowflake -> dbt")

@st.cache_resource
def get_engine():
    return create_engine(URL(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        database  = os.getenv("SNOWFLAKE_DATABASE"),
        schema    = "MART",
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
        role      = os.getenv("SNOWFLAKE_ROLE"),
    ))

@st.cache_data(ttl=300)
def load_distribution():
    df = pd.read_sql(
        "SELECT * FROM AIRPULSE.MART.MART_POLLUTANT_DISTRIBUTION ORDER BY COUNTRY, PARAMETER",
        get_engine()
    )
    df.columns = [c.lower() for c in df.columns]
    return df

@st.cache_data(ttl=300)
def load_timeseries(country: str, parameter: str):
    df = pd.read_sql(f"""
        SELECT MEASURED_HOUR, AVG_VALUE, WHO_THRESHOLD, BREACH_COUNT
        FROM AIRPULSE.MART.MART_QUALITY_TIMESERIES
        WHERE COUNTRY = '{country}' AND PARAMETER = '{parameter}'
        ORDER BY MEASURED_HOUR DESC LIMIT 72
    """, get_engine())
    df.columns = [c.lower() for c in df.columns]
    return df.sort_values("measured_hour")

dist_df = load_distribution()

# --- Sidebar filters ---
st.sidebar.header("Filters")
countries  = sorted(dist_df["country_name"].unique().tolist())
selected_c = st.sidebar.multiselect("Countries", countries, default=countries)
parameters = sorted(dist_df["parameter"].unique().tolist())
selected_p = st.sidebar.selectbox("Pollutant", parameters)

filtered = dist_df[
    (dist_df["country_name"].isin(selected_c)) &
    (dist_df["parameter"] == selected_p)
]

# --- KPI row ---
col1, col2, col3, col4 = st.columns(4)
col1.metric("Countries monitored", dist_df["country"].nunique())
col2.metric("Pollutants tracked",  dist_df["parameter"].nunique())
col3.metric("Total readings",      f"{dist_df['reading_count'].sum():,.0f}")
col4.metric("Avg WHO breach rate", f"{dist_df['breach_pct'].mean():.1f}%")

st.divider()

# --- Tile 1: Bar chart ---
st.subheader(f"Tile 1 — Average {PARAM_LABELS.get(selected_p, selected_p)} by country")
if not filtered.empty:
    chart_df = filtered.set_index("country_name")[["avg_value"]].copy()
    chart_df.columns = [f"Avg {PARAM_LABELS.get(selected_p)} (µg/m³)"]
    st.bar_chart(chart_df, use_container_width=True)
    threshold = WHO_THRESHOLDS.get(selected_p)
    if threshold:
        st.caption(f"WHO 24h guideline for {PARAM_LABELS.get(selected_p)}: {threshold} µg/m³")
    st.dataframe(
        filtered[[
            "country_name", "avg_value", "max_value",
            "p95_value", "breach_pct", "last_reading_at"
        ]].rename(columns={
            "country_name":    "Country",
            "avg_value":       "Avg (µg/m³)",
            "max_value":       "Max (µg/m³)",
            "p95_value":       "P95 (µg/m³)",
            "breach_pct":      "WHO Breach %",
            "last_reading_at": "Last Reading"
        }),
        use_container_width=True,
        hide_index=True
    )
else:
    st.info("No data for selected filters.")

st.divider()

# --- Tile 2: Line chart ---
st.subheader(f"Tile 2 — {PARAM_LABELS.get(selected_p, selected_p)} trend over time")
country_options = dist_df[dist_df["country_name"].isin(selected_c)][["country","country_name"]].drop_duplicates()
if not country_options.empty:
    selected_country_name = st.selectbox(
        "Select country for trend",
        country_options["country_name"].tolist()
    )
    country_code = country_options[
        country_options["country_name"] == selected_country_name
    ]["country"].iloc[0]

    ts_df = load_timeseries(country_code, selected_p)
    if not ts_df.empty:
        st.line_chart(
            ts_df.set_index("measured_hour")[["avg_value"]].rename(
                columns={"avg_value": f"{PARAM_LABELS.get(selected_p)} (µg/m³)"}
            ),
            use_container_width=True
        )
        threshold = WHO_THRESHOLDS.get(selected_p)
        total_breaches = ts_df["breach_count"].sum()
        if threshold:
            st.caption(
                f"WHO threshold: {threshold} µg/m³ — "
                f"breaches in window: {total_breaches:.0f} hours"
            )
    else:
        st.info("No time series data for this country and pollutant.")

st.divider()
st.caption("Pipeline: OpenAQ -> Redpanda -> AWS S3 + Snowflake -> dbt -> Streamlit · Refreshed every 5 min via Prefect")
