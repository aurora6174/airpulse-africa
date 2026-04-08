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
print("Table ready.")

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
        print(f"[Snowflake] {r['country']} | {r['parameter']} | {r['value']} {r['unit']}")
finally:
    cur.close()
    conn.close()
    consumer.close()
