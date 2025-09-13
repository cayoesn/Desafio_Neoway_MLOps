from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Importa a função main do seu módulo de feature engineering
from features.feature_engineering import main as run_feature_engineering

# Padrões: variáveis podem vir de Airflow Variables ou de env vars
INPUT_CSV  = Variable.get("input_csv",  default_var=os.getenv("INPUT_CSV", "/opt/airflow/data/novas_empresas.csv"))
REDIS_HOST = Variable.get("redis_host", default_var=os.getenv("REDIS_HOST", "redis"))
REDIS_PORT = int(Variable.get("redis_port", default_var=os.getenv("REDIS_PORT", 6379)))

default_args = {
    "owner": "neoway",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def process_features(**context):
    """
    Chamado pelo PythonOperator para rodar seu script de PySpark
    """
    run_feature_engineering(
        input_csv=INPUT_CSV,
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
        bash_command='echo "Início do pipeline de Inteligência de Mercado"'
    )

    processar = PythonOperator(
        task_id="processar_e_salvar_features",
        python_callable=process_features,
        provide_context=True
    )

    iniciar >> processar
