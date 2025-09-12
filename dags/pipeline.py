from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator

# Pega configurações via env vars
INPUT_CSV   = os.getenv("INPUT_CSV", "/opt/airflow/data/novas_empresas.csv")
REDIS_HOST  = os.getenv("REDIS_HOST", "redis")
REDIS_PORT  = os.getenv("REDIS_PORT", "6379")
SPARK_MASTER= os.getenv("SPARK_MASTER", "local[2]")

default_args = {
    "owner": "neoway",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pipeline_inteligencia_mercado",
    default_args=default_args,
    description="Pipeline de FE para novas empresas",
    start_date=datetime(2025, 9, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["neoway", "mlops"],
) as dag:

    iniciar = BashOperator(
        task_id="iniciar_processamento",
        bash_command='echo "Iniciando pipeline de Inteligência de Mercado."'
    )

    processar = BashOperator(
        task_id="processar_e_salvar_features",
        bash_command=(
            "spark-submit "
            "--master {{ var.value.SPARK_MASTER | default('" + SPARK_MASTER + "') }} "
            "--deploy-mode client "
            "/opt/airflow/src/neoway_features/feature_engineering.py "
            "--input-csv {{ var.value.INPUT_CSV  | default('" + INPUT_CSV + "') }} "
            "--redis-host {{ var.value.REDIS_HOST | default('" + REDIS_HOST + "') }} "
            "--redis-port {{ var.value.REDIS_PORT | default('" + REDIS_PORT + "') }}"
        )
    )

    iniciar >> processar
