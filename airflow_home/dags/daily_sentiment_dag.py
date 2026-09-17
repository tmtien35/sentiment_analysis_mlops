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
    'start_date': pendulum.datetime(2026, 8, 10, tz="Asia/Ho_Chi_Minh") if local_tz else datetime(2026, 8, 10),
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

def _resolve_target_batch_date(**context) -> str:
    """
    Determines the exact batch date in Asia/Ho_Chi_Minh (UTC+7) timezone.
    Guarantees that manual triggers and scheduled runs always resolve to the
    actual calendar date in Vietnam, completely preventing UTC-offset mismatches.
    """
    # 1. Prioritize explicit date passed in manual trigger config (e.g. {"ds": "2026-09-17"})
    dag_run = context.get('dag_run')
    if dag_run and getattr(dag_run, 'conf', None) and dag_run.conf.get('ds'):
        return str(dag_run.conf.get('ds')).strip()
    
    # 2. Extract execution timestamp from Airflow context (logical_date or execution_date)
    dt = context.get('logical_date') or context.get('execution_date')
    if dt is not None:
        try:
            if hasattr(dt, 'in_timezone'):
                import pendulum
                return dt.in_timezone(pendulum.timezone("Asia/Ho_Chi_Minh")).strftime("%Y-%m-%d")
        except Exception:
            pass
        
    # 3. Fallback to current calendar date in Asia/Ho_Chi_Minh (UTC+7)
    try:
        from datetime import datetime as dt_cls, timezone, timedelta
        vn_now = dt_cls.now(timezone(timedelta(hours=7)))
        return vn_now.strftime("%Y-%m-%d")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")

def crawl_task_callable(**context):
    ds = _resolve_target_batch_date(**context)
    print(f"🎯 [Airflow DAG] Step 1: Crawling daily reviews for Vietnam Date: {ds}")
    return crawl_daily_reviews(ds=ds, n_reviews=20)

def batch_scoring_task_callable(**context):
    ds = _resolve_target_batch_date(**context)
    print(f"🎯 [Airflow DAG] Step 2: Batch scoring & drift monitoring for Vietnam Date: {ds}")
    return run_batch_scoring(ds=ds, auto_retrain=True)

# Task 1: Scrape / sample non-overlapping daily reviews from the EV simulation pool
crawl_task = PythonOperator(
    task_id='crawl_daily_ev_reviews',
    python_callable=crawl_task_callable,
    dag=dag,
)

# Task 2: Batch inference scoring and drift monitoring
batch_scoring_task = PythonOperator(
    task_id='batch_scoring_and_drift_monitoring',
    python_callable=batch_scoring_task_callable,
    dag=dag,
)

# Dependency orchestration: Task 1 (Crawl) must succeed before Task 2 (Scoring)
crawl_task >> batch_scoring_task



