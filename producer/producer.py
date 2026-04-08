import time, json, requests, os
from datetime import datetime, timezone, timedelta
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

API_KEY    = os.getenv("OPENAQ_API_KEY")
BASE_URL   = os.getenv("OPENAQ_BASE_URL", "https://api.openaq.org/v3")
TOPIC      = os.getenv("KAFKA_TOPIC", "air-quality-readings")
BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
INTERVAL   = int(os.getenv("POLL_INTERVAL_SECONDS", 60))

HEADERS = {"X-API-Key": API_KEY, "Accept": "application/json"}
producer = Producer({"bootstrap.servers": BOOTSTRAP})

WHO_THRESHOLDS = {"pm25": 15, "pm10": 45, "no2": 25, "co": 4000, "o3": 100, "so2": 40}

# Country IDs confirmed from OpenAQ v3 API
COUNTRY_IDS = {
    "NG": 100,   # Nigeria
    "GH": 152,   # Ghana
    "KE": 17,    # Kenya
    "ZA": 37,    # South Africa
    "ET": 14,    # Ethiopia
}

def fetch_locations(country_id: int, country_code: str) -> list:
    resp = requests.get(
        f"{BASE_URL}/locations",
        headers=HEADERS,
        params={"countries_id": country_id, "limit": 20},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("results", [])

def fetch_sensor_measurements(sensor_id: int) -> list:
    # Request only readings from the last 7 days to maximise chances of data
    date_from = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    resp = requests.get(
        f"{BASE_URL}/sensors/{sensor_id}/measurements",
        headers=HEADERS,
        params={"limit": 5, "date_from": date_from},
        timeout=15,
    )
    if resp.status_code != 200:
        return []
    return resp.json().get("results", [])

def parse_record(m: dict, loc: dict, country_code: str) -> dict | None:
    param = m.get("parameter", {}).get("name", "")
    value = m.get("value")
    if not param or value is None:
        return None

    measured_at = (
        m.get("period", {}).get("datetimeTo", {}).get("utc")
        or m.get("period", {}).get("datetimeFrom", {}).get("utc")
    )
    threshold = WHO_THRESHOLDS.get(param)

    return {
        "location_id":   loc.get("id"),
        "location_name": loc.get("name", "unknown"),
        "city":          loc.get("locality") or loc.get("name", "unknown"),
        "country":       country_code,
        "latitude":      loc.get("coordinates", {}).get("latitude"),
        "longitude":     loc.get("coordinates", {}).get("longitude"),
        "parameter":     param,
        "value":         value,
        "unit":          m.get("parameter", {}).get("units", "µg/m³"),
        "who_threshold": threshold,
        "exceeds_who":   (value > threshold) if threshold is not None else None,
        "measured_at":   measured_at,
        "ingested_at":   datetime.now(timezone.utc).isoformat(),
    }

def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[OK] {msg.topic()} [{msg.partition()}] offset={msg.offset()}")

def produce_country(country_code: str, country_id: int) -> int:
    locations = fetch_locations(country_id, country_code)
    count = 0
    for loc in locations:
        for sensor in loc.get("sensors", []):
            sid   = sensor.get("id")
            param = sensor.get("parameter", {}).get("name", "")
            if param not in WHO_THRESHOLDS:
                continue
            measurements = fetch_sensor_measurements(sid)
            for m in measurements:
                record = parse_record(m, loc, country_code)
                if record:
                    producer.produce(
                        TOPIC,
                        key=f"{country_code}_{param}_{loc.get('id')}",
                        value=json.dumps(record),
                        callback=delivery_report,
                    )
                    count += 1
            time.sleep(0.2)   # stay within 10 req/s rate limit
    producer.flush()
    return count

def produce():
    print(f"AirPulse producer started - polling every {INTERVAL}s")
    print(f"Countries: {list(COUNTRY_IDS.keys())}")
    while True:
        total = 0
        for code, cid in COUNTRY_IDS.items():
            try:
                n = produce_country(code, cid)
                print(f"  [{code}] Produced {n} readings")
                total += n
            except Exception as e:
                print(f"  [{code}] ERROR: {e}")
        print(f"Poll complete — {total} total readings produced")
        time.sleep(INTERVAL)

if __name__ == "__main__":
    produce()
