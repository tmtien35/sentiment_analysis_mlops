from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import decoupled crawler and batch scoring functions
from data.crawl_feed import crawl_daily_reviews
from airflow_home.dags.batch_scoring import run_batch_scoring

try:
    import pendulum
    local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
except Exception:
    local_tz = None

default_args = {
    'owner': 'mlops',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 10, tzinfo=local_tz) if local_tz else datetime(2026, 8, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'daily_sentiment_analysis',
    default_args=default_args,
    description='Automated daily batch review scoring and input drift monitoring',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
)

# Template expression: Uses execution interval end date so scheduled runs match real-time calendar date
BATCH_DATE_TEMPLATE = "{{ (data_interval_end | ds) if data_interval_end is defined and data_interval_end else ds }}"

# Task 1: Scrape / sample non-overlapping daily reviews from the EV simulation pool
crawl_task = PythonOperator(
    task_id='crawl_daily_ev_reviews',
    python_callable=crawl_daily_reviews,
    op_kwargs={'ds': BATCH_DATE_TEMPLATE, 'n_reviews': 20},
    dag=dag,
)

# Task 2: Batch inference scoring and drift monitoring
batch_scoring_task = PythonOperator(
    task_id='batch_scoring_and_drift_monitoring',
    python_callable=run_batch_scoring,
    op_kwargs={'ds': BATCH_DATE_TEMPLATE},
    dag=dag,
)

# Dependency orchestration: Task 1 (Crawl) must succeed before Task 2 (Scoring)
crawl_task >> batch_scoring_task


