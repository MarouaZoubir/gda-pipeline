from pathlib import Path
from dagster import Definitions
from dagster_dbt import DbtCliResource, DbtProject

from assets import (
    mysql_raw_data,
    debezium_cdc_events,
    validate_postgres_data,
    dbt_assets  # This should be the @dbt_assets decorated function
)

from resources import (
    mysql_conn_resource,
    postgres_conn_resource,
    ge_context_resource,
    debezium_conn_resource
)

# Set up the path to your dbt project
dbt_project_path = Path("C:/Users/HA/Desktop/stage-gda-real/uatrs-data-pipeline/uatrs_dbt").resolve()
my_dbt_project = DbtProject(project_dir=dbt_project_path)
my_dbt_project.prepare_if_dev()  # Optional: only for dev use

defs = Definitions(
    assets=[
        mysql_raw_data,
        debezium_cdc_events,
        validate_postgres_data,
        dbt_assets  # This should be the result of @dbt_assets(manifest=my_dbt_project.manifest_path)
    ],
    resources={
        "mysql_conn": mysql_conn_resource,
        "postgres_conn": postgres_conn_resource,
        "ge_context": ge_context_resource,
        "dbt": DbtCliResource(project_dir=my_dbt_project),
        "debezium_conn": debezium_conn_resource
    }
)
