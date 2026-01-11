from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


# -------------------------------------------------
# Helper: run Snowflake ingestion SQL
# -------------------------------------------------
def run_snowflake_sql(sql_path: str):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    with open(sql_path, "r") as f:
        sql = f.read()
    hook.run(sql)


# -------------------------------------------------
# DAG definition
# -------------------------------------------------
with DAG(
    dag_id="bitcoin_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    # -----------------------
    # 1. Ingestion (BRONZE)
    # -----------------------
    ingest_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=run_snowflake_sql,
        op_args=["/usr/local/airflow/dags/ingestion/sf_ingestion.sql"],
    )

    # -----------------------
    # 2. dbt (SILVER → GOLD)
    # -----------------------
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(
            dbt_project_path="/usr/local/airflow/dags/dbt/bitcoin",
        ),
        profile_config=ProfileConfig(
            profile_name="bitcoin",
            target_name="dev",
            profile_mapping=SnowflakeUserPasswordProfileMapping(
                conn_id="snowflake_conn",
            ),
        ),
        operator_args={
            "install_deps": False,
        },
    )

    # -----------------------
    # Dependency
    # -----------------------
    ingest_bronze >> dbt_transform
