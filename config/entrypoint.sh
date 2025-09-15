set -e

if [ ! -f "/opt/airflow/airflow.db" ]; then
  echo ">>> Initializing Airflow database..."
  airflow db init
fi

airflow users create \
  --username "${_AIRFLOW_WWW_USER_USERNAME:-admin}" \
  --firstname "${_AIRFLOW_WWW_USER_FIRSTNAME:-Airflow}" \
  --lastname "${_AIRFLOW_WWW_USER_LASTNAME:-Admin}" \
  --role Admin \
  --email "${_AIRFLOW_WWW_USER_EMAIL:-admin@example.com}" \
  --password "${_AIRFLOW_WWW_USER_PASSWORD:-admin}" || true

exec "$@"
