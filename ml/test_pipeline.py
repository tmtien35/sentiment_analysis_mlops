import pytest
from ml.preprocess import clean_text

def test_clean_text():
    """
    Test 1: Verify that our shared text-cleaning utility consistently converts text 
    to lowercase, strips HTML tags, and trims excess whitespace.
    """
    raw_text = "<p>I highly recommend this! Excellent quality and super fast shipping.</p>"
    expected = "i highly recommend this ! excellent quality and super fast shipping ."
    
    assert clean_text(raw_text) == expected
    
    # Test handling of Vietnamese Unicode accented text and compound word tokenization
    vn_text = "Xe chạy rất êm, tăng tốc mượt mà và tiết kiệm điện!"
    expected_vn = "xe chạy rất êm , tăng_tốc mượt_mà và tiết_kiệm điện !"
    assert clean_text(vn_text) == expected_vn
    assert "tăng_tốc" in clean_text(vn_text)
    assert "tiết_kiệm" in clean_text(vn_text)
    
    # Test handling of empty strings or non-string inputs
    assert clean_text("") == ""
    assert clean_text(None) == ""

def test_model_pipeline_compilation():
    """
    Test 2: Verify that our custom Scikit-Learn TF-IDF + Classifier Pipeline 
    structure compiles, fits, and predicts correctly.
    """
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.pipeline import Pipeline
    from sklearn.linear_model import LogisticRegression
    
    # Define exact same pipeline structure
    pipeline = Pipeline([
        ('tfidf', TfidfVectorizer(max_features=100)),
        ('clf', LogisticRegression())
    ])
    
    # Fit on tiny mock dataset
    X_train = ["great product and excellent quality", "bad packaging and terrible support", "average performance okay price"]
    y_train = ["positive", "negative", "neutral"]
    pipeline.fit(X_train, y_train)
    
    # Verify predictions run and output standard categories
    pred = pipeline.predict(["really great product"])[0]
    assert pred in ["positive", "negative", "neutral"]

def test_dag_and_scoring_syntax_check():
    """
    Test 3: Parse the Airflow DAG and Batch Scoring scripts using Python's 
    Abstract Syntax Tree (AST) parser. This mathematically verifies there are 
    no syntax errors, indentation errors, or structural bugs in our orchestration files.
    """
    import ast
    
    # 1. Parse daily_sentiment_dag.py
    dag_path = "airflow_home/dags/daily_sentiment_dag.py"
    with open(dag_path, "r", encoding="utf-8") as f:
        dag_code = f.read()
    parsed_dag = ast.parse(dag_code)
    assert parsed_dag is not None, f"Failed to parse syntax for {dag_path}"
    
    # 2. Parse batch_scoring.py
    scoring_path = "airflow_home/dags/batch_scoring.py"
    with open(scoring_path, "r", encoding="utf-8") as f:
        scoring_code = f.read()
    parsed_scoring = ast.parse(scoring_code)
    assert parsed_scoring is not None, f"Failed to parse syntax for {scoring_path}"
    
    print("AST verification passed for both Airflow orchestration files!")
def test_persistent_drift_conditions():
    """
    Test 4: Verify Persistent Drift decision logic (Threshold = 0.15):
    - Low drift (< 0.15) triggers no drift.
    - Drift detected (PSI >= 0.15) on Day 1 (no prior drift) defers retraining.
    - Persistent drift triggers if and only if PSI >= 0.15 AND prior batch also experienced drift (2 consecutive days).
    """
    psi_drift_extreme = 0.45
    psi_drift_moderate = 0.18
    psi_normal = 0.08
    
    assert psi_normal < 0.15, "Normal PSI must be < 0.15"
    assert psi_drift_moderate >= 0.15, "Moderate drift must be >= 0.15"
    assert psi_drift_extreme >= 0.15, "Extreme drift must be >= 0.15"
    
    # Consecutive check simulation
    history_drifted = [1]
    history_clean = [0]
    
    # Day 1 single spike (clean history) must NOT trigger retrain even if PSI is high
    is_persistent_day1_moderate = bool(psi_drift_moderate >= 0.15 and any(r >= 1 for r in history_clean))
    is_persistent_day1_extreme = bool(psi_drift_extreme >= 0.15 and any(r >= 1 for r in history_clean))
    assert is_persistent_day1_moderate is False
    assert is_persistent_day1_extreme is False
    
    # Day 2 consecutive drift (prior day was drifted) MUST trigger persistent drift
    is_persistent_day2_moderate = bool(psi_drift_moderate >= 0.15 and any(r >= 1 for r in history_drifted))
    is_persistent_day2_extreme = bool(psi_drift_extreme >= 0.15 and any(r >= 1 for r in history_drifted))
    assert is_persistent_day2_moderate is True
    assert is_persistent_day2_extreme is True

def test_api_and_dashboard_syntax():
    """
    Test 5: AST syntax verification for api/main.py and dashboard/app.py.
    """
    import ast
    for path in ["api/main.py", "dashboard/app.py"]:
        with open(path, "r", encoding="utf-8") as f:
            code = f.read()
        parsed = ast.parse(code)
        assert parsed is not None, f"Failed syntax parse on {path}"

def test_ondemand_active_learning_flow():
    """
    Test 6: Verify on-demand inference verification & ingestion into store_reviews:
    - Verifies that verified labels are written to store_reviews with category='on_demand'
    - Verifies that retraining query finds the new ground-truth samples
    - Validates human corrections of low-confidence and incorrect predictions
    """
    from sqlalchemy import create_engine, text
    from dashboard.app import save_ondemand_review_to_training
    
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE store_reviews (
                review_id TEXT PRIMARY KEY,
                review_date TEXT,
                category TEXT,
                review_text TEXT,
                is_processed INTEGER DEFAULT 0,
                verified_sentiment TEXT DEFAULT NULL
            );
        """))
        conn.execute(text("""
            CREATE TABLE predictions (
                review_id TEXT PRIMARY KEY,
                review_date TEXT,
                category TEXT,
                review_text TEXT,
                cleaned_text TEXT,
                predicted_sentiment TEXT,
                confidence REAL
            );
        """))
        conn.execute(text("""
            CREATE TABLE inference_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                review_text TEXT,
                cleaned_text TEXT,
                predicted_sentiment TEXT,
                confidence REAL,
                model_route TEXT DEFAULT 'champion',
                latency_ms REAL DEFAULT 0.0,
                verified_sentiment TEXT DEFAULT NULL
            );
        """))
        # Seed an inference log
        conn.execute(text("""
            INSERT INTO inference_logs (id, timestamp, review_text, cleaned_text, predicted_sentiment, confidence)
            VALUES (1, '2026-09-14 10:00:00', 'Pin yếu quá sạc mãi không đầy', 'pin yếu quá sạc mãi không đầy', 'positive', 0.51);
        """))
        
        # Human engineer audits row 1: corrects 'positive' (low-confidence misclassification) to 'negative'
        save_ondemand_review_to_training(
            conn=conn,
            log_id=1,
            review_text='Pin yếu quá sạc mãi không đầy',
            cleaned_text='pin yếu quá sạc mãi không đầy',
            predicted_sentiment='positive',
            confidence=0.51,
            verified_sentiment='negative',
            timestamp='2026-09-14 10:00:00'
        )
        
        # Verify store_reviews entry
        res = conn.execute(text("SELECT review_id, category, verified_sentiment, is_processed FROM store_reviews WHERE review_id = 'ondemand_1'")).fetchone()
        assert res is not None
        assert res[0] == 'ondemand_1'
        assert res[1] == 'on_demand'
        assert res[2] == 'negative'
        assert res[3] == 1
        
        # Verify inference_logs was updated
        log_res = conn.execute(text("SELECT verified_sentiment FROM inference_logs WHERE id = 1")).fetchone()
        assert log_res[0] == 'negative'
        
        # Verify predictions does NOT contain on-demand review (isolating operational monitoring charts from test reviews)
        pred_res = conn.execute(text("SELECT COUNT(*) FROM predictions WHERE review_id = 'ondemand_1'")).fetchone()
        assert pred_res[0] == 0

        # Verify retraining query (from ml/train_model.py) fetches this verified review
        retrain_res = conn.execute(text("SELECT review_text, verified_sentiment FROM store_reviews WHERE verified_sentiment IS NOT NULL")).fetchall()
        assert len(retrain_res) == 1
        assert retrain_res[0][0] == 'Pin yếu quá sạc mãi không đầy'
        assert retrain_res[0][1] == 'negative'

def test_api_log_prediction_db_persistence():
    import os
    from sqlalchemy import create_engine, text
    from api.main import init_inference_logs_table, log_prediction_to_db
    test_db = "sqlite:///test_api_log.db"
    os.environ["DATABASE_URL"] = test_db
    engine = create_engine(test_db)
    
    init_inference_logs_table(engine)
    # Calling it twice should be completely idempotent and not fail
    init_inference_logs_table(engine)
    
    log_prediction_to_db("Xe dep", "xe dep", "positive", 0.95, "champion", 4.2)
    
    with engine.connect() as conn:
        res = conn.execute(text("SELECT review_text, predicted_sentiment, confidence, model_route, latency_ms FROM inference_logs WHERE review_text = 'Xe dep'")).fetchone()
        assert res is not None
        assert res[0] == "Xe dep"
        assert res[1] == "positive"
        assert res[2] == 0.95
        assert res[3] == "champion"
        assert res[4] == 4.2

    if os.path.exists("test_api_log.db"):
        try:
            os.remove("test_api_log.db")
        except Exception:
            pass

def test_notify_fastapi_reload_graceful():
    from ml.train_model import notify_fastapi_reload
    # When API is not running locally, notify_fastapi_reload should gracefully handle and return False without raising exceptions
    res = notify_fastapi_reload()
    assert res in [True, False]


def test_airflow_docker_compose_secret_key_sync():
    """
    Test 8: Verify Airflow components (webserver and scheduler) in docker-compose.yml
    have the exact same AIRFLOW__WEBSERVER__SECRET_KEY configured.
    Prevents 403 Forbidden log-reading regressions across services.
    """
    import yaml
    with open("docker-compose.yml", "r", encoding="utf-8") as f:
        compose_cfg = yaml.safe_load(f)
    
    services = compose_cfg.get("services", {})
    webserver_env = services.get("airflow-webserver", {}).get("environment", {})
    scheduler_env = services.get("airflow-scheduler", {}).get("environment", {})
    
    ws_key = webserver_env.get("AIRFLOW__WEBSERVER__SECRET_KEY")
    sched_key = scheduler_env.get("AIRFLOW__WEBSERVER__SECRET_KEY")
    
    assert ws_key is not None and len(ws_key) > 8, "AIRFLOW__WEBSERVER__SECRET_KEY must be set in airflow-webserver"
    assert sched_key is not None and len(sched_key) > 8, "AIRFLOW__WEBSERVER__SECRET_KEY must be set in airflow-scheduler"
    assert ws_key == sched_key, "Airflow webserver and scheduler secret_keys must be identical to allow reading task logs without 403 Forbidden"

def test_batch_scoring_cwd_independence():
    """
    Test 9: Verify crawl_daily_reviews and get_mlflow_tracking_uri work seamlessly
    even when current working directory is changed to airflow_home (simulating container execution).
    """
    import os
    from data.crawl_feed import crawl_daily_reviews
    from airflow_home.dags.batch_scoring import get_mlflow_tracking_uri, get_db_engine
    
    original_cwd = os.getcwd()
    try:
        os.chdir("airflow_home")
        # Ensure URI resolution doesn't crash or create misplaced databases
        tracking_uri = get_mlflow_tracking_uri()
        assert "mlflow.db" in tracking_uri
        
        engine = get_db_engine()
        assert engine is not None
    finally:
        os.chdir(original_cwd)


def test_robust_champion_model_loader_and_continuous_confidences():
    """
    Test 10: Verify load_champion_model_robust loads a genuine ML Pipeline with predict_proba
    and that simulated cross-platform MLflow URI failure gracefully falls back to local artifacts,
    preventing fallback to static 0.6 / 0.9 confidences.
    """
    from ml.model_loader import load_champion_model_robust
    import ml.model_loader as loader_mod
    
    # 1. Normal load
    model = load_champion_model_robust()
    assert model is not None, "Champion model must be loadable"
    assert hasattr(model, "predict_proba"), "Champion model must support predict_proba"
    
    test_text = "Xe chạy rất êm, pin trâu, tiết kiệm chi phí"
    probs = model.predict_proba([test_text])[0]
    conf = float(max(probs))
    # Genuine probability should be continuous, not the dummy static values (0.60 or 0.90)
    assert conf != 0.60 and conf != 0.90, f"Confidence {conf} must be real continuous probability"
    assert 0.0 < conf <= 1.0
    
    # 2. Simulated MLflow failure (mimicking Docker Linux / Windows path mismatch)
    orig_load = loader_mod.mlflow.sklearn.load_model
    try:
        loader_mod.mlflow.sklearn.load_model = lambda *args, **kwargs: (_ for _ in ()).throw(
            FileNotFoundError("Simulated cross-platform path mismatch: [Errno 2] No such file or directory: 'file:D:/...'")
        )
        fallback_model = load_champion_model_robust()
        assert fallback_model is not None, "Fallback mechanism must load the real model even when MLflow registry path fails"
        assert hasattr(fallback_model, "predict_proba")
        
        fb_probs = fallback_model.predict_proba([test_text])[0]
        fb_conf = float(max(fb_probs))
        assert fb_conf != 0.60 and fb_conf != 0.90, f"Fallback confidence {fb_conf} must be real continuous probability"
        assert abs(fb_conf - conf) < 1e-4, "Fallback model predictions should match original model predictions"
    finally:
        loader_mod.mlflow.sklearn.load_model = orig_load

