import redis
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from features.feature_engineering import process as run_feature_engineering

def redis_health_check(**context):
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, socket_connect_timeout=5)
        if not r.ping():
            raise Exception("Redis ping failed")
    except Exception as e:
        raise Exception(f"Erro ao conectar ao Redis: {e}")

def get_input_csv(context=None):
    if context and context.get('dag_run') and context['dag_run'].conf.get('input_csv'):
        return context['dag_run'].conf.get('input_csv')
    try:
        return Variable.get("input_csv")
    except Exception:
        pass
    env_val = os.getenv("INPUT_CSV")
    if env_val:
        return env_val
    return "/opt/airflow/data/novas_empresas.csv"

INPUT_CSV = get_input_csv()
REDIS_HOST = Variable.get("redis_host", os.getenv("REDIS_HOST", "redis"))
REDIS_PORT = int(Variable.get("redis_port", os.getenv("REDIS_PORT", 6379)))

default_args = {
    "owner": "neoway",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

def process_features(**context):
    input_csv = get_input_csv(context)
    run_feature_engineering(
        input_csv=input_csv,
        redis_host=REDIS_HOST,
        redis_port=REDIS_PORT
    )

with DAG(
    dag_id="pipeline_inteligencia_mercado",
    default_args=default_args,
    description="Calcula features por cidade e salva em Redis",
    start_date=datetime(2025, 9, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["neoway", "mlops"],
) as dag:

    iniciar = BashOperator(
        task_id="iniciar_processamento",
        bash_command='echo "Início do pipeline de Inteligência de Mercado."'
    )

    healthcheck = PythonOperator(
        task_id="healthcheck_redis",
        python_callable=redis_health_check,
        provide_context=True,
        retries=2,
        retry_delay=timedelta(seconds=30),
    )

    processar = PythonOperator(
        task_id="processar_e_salvar_features",
        python_callable=process_features,
        provide_context=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    iniciar >> healthcheck >> processar
