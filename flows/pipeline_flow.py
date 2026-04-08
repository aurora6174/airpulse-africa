import subprocess, os
from prefect import flow, task
from dotenv import load_dotenv

load_dotenv()

DBT_DIR = os.path.expanduser("~/projects/airpulse-africa/dbt/airpulse")

@task(name="Health-check Redpanda", log_prints=True)
def check_broker():
    result = subprocess.run(
        ["docker", "exec", "redpanda", "rpk", "topic", "list"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception("Redpanda not reachable")
    print("Redpanda healthy.")

@task(name="Run dbt models", log_prints=True)
def run_dbt():
    env = {
        **os.environ,
        "SNOWFLAKE_ACCOUNT":   os.getenv("SNOWFLAKE_ACCOUNT"),
        "SNOWFLAKE_USER":      os.getenv("SNOWFLAKE_USER"),
        "SNOWFLAKE_PASSWORD":  os.getenv("SNOWFLAKE_PASSWORD"),
        "SNOWFLAKE_ROLE":      os.getenv("SNOWFLAKE_ROLE"),
        "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "SNOWFLAKE_DATABASE":  os.getenv("SNOWFLAKE_DATABASE"),
    }
    result = subprocess.run(
        ["dbt", "run", "--project-dir", DBT_DIR, "--profiles-dir", os.path.expanduser("~/.dbt")],
        capture_output=True, text=True, env=env
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt run failed:\n{result.stderr}")

@task(name="Run dbt tests", log_prints=True)
def test_dbt():
    env = {
        **os.environ,
        "SNOWFLAKE_ACCOUNT":   os.getenv("SNOWFLAKE_ACCOUNT"),
        "SNOWFLAKE_USER":      os.getenv("SNOWFLAKE_USER"),
        "SNOWFLAKE_PASSWORD":  os.getenv("SNOWFLAKE_PASSWORD"),
        "SNOWFLAKE_ROLE":      os.getenv("SNOWFLAKE_ROLE"),
        "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "SNOWFLAKE_DATABASE":  os.getenv("SNOWFLAKE_DATABASE"),
    }
    result = subprocess.run(
        ["dbt", "test", "--project-dir", DBT_DIR, "--profiles-dir", os.path.expanduser("~/.dbt")],
        capture_output=True, text=True, env=env
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt test failed:\n{result.stderr}")

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
