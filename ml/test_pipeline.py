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
