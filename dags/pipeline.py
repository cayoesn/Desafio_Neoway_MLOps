import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from features.feature_engineering import process as run_feature_engineering
import redis


def get_env_variable(var_name, default=None):
    return Variable.get(var_name, os.getenv(var_name.upper(), default))


def redis_health_check(**context):
    host = get_env_variable("redis_host", "redis")
    port = int(get_env_variable("redis_port", 6379))

    try:
        r = redis.Redis(host=host, port=port, socket_connect_timeout=5)
        if not r.ping():
            raise Exception("Redis ping failed")
    except Exception as e:
        logging.error(f"Error connecting to Redis: {e}")
        raise


def get_input_csv(context=None):
    if (
        context
        and context.get('dag_run')
        and context['dag_run'].conf.get('input_csv')
    ):
        return context['dag_run'].conf.get('input_csv')
    return get_env_variable(
        "input_csv",
        "/opt/airflow/data/novas_empresas.csv"
    )


def process_features(**context):
    input_csv = get_input_csv(context)
    host = get_env_variable("redis_host", "redis")
    port = int(get_env_variable("redis_port", 6379))
    run_feature_engineering(
        input_csv=input_csv,
        redis_host=host,
        redis_port=port
    )


default_args = {
    "owner": "cayoesn",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="market_intelligence_pipeline",
    default_args=default_args,
    description="Calculates features by city and saves to Redis",
    start_date=datetime(2025, 9, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    start = BashOperator(
        task_id="start_processing",
        bash_command='echo "Starting Market Intelligence pipeline."'
    )

    healthcheck = PythonOperator(
        task_id="redis_healthcheck",
        python_callable=redis_health_check,
        retries=2,
        retry_delay=timedelta(seconds=30),
    )

    process = PythonOperator(
        task_id="process_and_save_features",
        python_callable=process_features,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    start >> healthcheck >> process
