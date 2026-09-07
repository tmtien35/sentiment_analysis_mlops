from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import the decoupled pure-python batch scoring function
from airflow_home.dags.batch_scoring import run_batch_scoring

default_args = {
    'owner': 'mlops',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'daily_sentiment_analysis',
    default_args=default_args,
    description='Automated daily batch review scoring and input drift monitoring',
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1,
)

# Wrapping the decoupled scoring logic into an Airflow PythonOperator
batch_scoring_task = PythonOperator(
    task_id='batch_scoring_and_drift_monitoring',
    python_callable=run_batch_scoring,
    op_kwargs={'ds': '{{ ds }}'},  # Dynamically pass Airflow execution date
    dag=dag,
)
