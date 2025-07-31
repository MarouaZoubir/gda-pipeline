# assets.py

from dagster import asset, OpExecutionContext, AssetExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource
from typing import Dict, Any, Optional, List
import requests




@asset(required_resource_keys={"mysql_conn"}, group_name="ingestion")
def mysql_raw_data(context):
    conn = context.resources.mysql_conn
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    raw_data = {}
    for table in tables:
        cursor.execute(f"SELECT * FROM `{table}`")
        raw_data[table] = cursor.fetchall()
    cursor.close()
    return raw_data


@asset(required_resource_keys={"debezium_conn"}, group_name="ingestion")
def debezium_cdc_events(context: OpExecutionContext) -> Dict[str, Any]:
    config = context.resources.debezium_conn
    connector_name = "uatrs-connector"
    base_url = f"http://{config['host']}:{config['port']}"

    try:
        connector_url = f"{base_url}/connectors/{connector_name}"
        connector_config = requests.get(f"{connector_url}/config").json()

        _validate_config(connector_config, {
            "database.hostname": "source-mysql",
            "database.include.list": "uatrs_db",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
        })

        status = requests.get(f"{connector_url}/status").json()
        metrics = requests.get(f"{base_url}/metrics").json()

        binlog_pos = next(
            (m for m in metrics if m["name"] == "debezium.mysql:type=connector-metrics,server=uatrs"),
            {}
        ).get("BinlogPosition")

        return {
            "status": status["connector"]["state"],
            "binlog_position": binlog_pos,
            "last_schema_change": _get_last_schema_change(config),
            "validation": {
                "config_match": True,
                "expected_tables": _get_tracked_tables(connector_config)
            }
        }

    except Exception as e:
        context.log.error(f"Debezium monitoring failed: {str(e)}")
        raise


def _validate_config(actual: Dict, expected: Dict):
    for key, val in expected.items():
        if actual.get(key) != val:
            raise ValueError(f"Config mismatch for {key}. Expected {val}, got {actual.get(key)}")


def _get_last_schema_change(config: Dict) -> Optional[str]:
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        "schema-changes.uatrs",
        bootstrap_servers=config["kafka_brokers"],
        auto_offset_reset="earliest"
    )
    for msg in consumer:
        return msg.value.decode("utf-8")[:100] + "..."


def _get_tracked_tables(config: Dict) -> List[str]:
    return config.get("table.include.list", "").split(",")


@asset(required_resource_keys={"ge_context", "postgres_conn"}, group_name="validation")
def validate_postgres_data(context):
    # Dummy validation logic – replace with real Great Expectations suite
    context.log.info("Validating Postgres data with Great Expectations...")
    return {"status": "Validation Passed"}

from pathlib import Path
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
from dagster import AssetExecutionContext, Definitions

# Define your DBT project directory (parent of dbt_project.yml)
dbt_project_path = Path("C:/Users/HA/Desktop/stage-gda-real/uatrs-data-pipeline/uatrs_dbt").resolve()

# Initialize the DbtProject object
my_dbt_project = DbtProject(project_dir=dbt_project_path)
my_dbt_project.prepare_if_dev()  # Optional in dev mode, safe to include



@dbt_assets(manifest=my_dbt_project.manifest_path)
def dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
