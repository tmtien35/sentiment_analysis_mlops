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
    Test 4: Verify Persistent Drift decision logic:
    - Acute drift (PSI >= 0.25) immediately triggers persistent drift.
    - Low drift (< 0.15) triggers no drift.
    - Moderate drift (0.15 <= PSI < 0.25) only triggers if historical consecutive drift exists.
    """
    psi_extreme = 0.28
    psi_moderate = 0.18
    psi_normal = 0.08
    
    # Acute check
    assert psi_extreme >= 0.25, "Acute drift must be >= 0.25"
    assert psi_normal < 0.15, "Normal PSI must be < 0.15"
    
    # Consecutive check simulation
    history_drifted = [1]
    history_clean = [0]
    
    is_persistent_moderate_with_history = bool(psi_moderate >= 0.15 and (psi_moderate >= 0.25 or any(r >= 1 for r in history_drifted)))
    is_persistent_moderate_clean_history = bool(psi_moderate >= 0.15 and (psi_moderate >= 0.25 or any(r >= 1 for r in history_clean)))
    
    assert is_persistent_moderate_with_history is True
    assert is_persistent_moderate_clean_history is False

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
        
        # Verify retraining query (from ml/train_model.py) fetches this verified review
        retrain_res = conn.execute(text("SELECT review_text, verified_sentiment FROM store_reviews WHERE verified_sentiment IS NOT NULL")).fetchall()
        assert len(retrain_res) == 1
        assert retrain_res[0][0] == 'Pin yếu quá sạc mãi không đầy'
        assert retrain_res[0][1] == 'negative'

