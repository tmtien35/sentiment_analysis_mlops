import os
import ssl
import time
import random
import mlflow
import mlflow.sklearn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager

# Shared Preprocessing
from ml.preprocess import clean_text

# 1. Universal SSL Bypass (required for local/Windows mlflow resolving)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
ssl.create_default_context = ssl._create_unverified_context

# Global model variables
champion_model = None
canary_model = None
model = None  # Backward-compatibility alias for champion_model
canary_version_loaded = None

from sqlalchemy import create_engine, text

def get_db_engine():
    db_url = os.environ.get("DATABASE_URL", "sqlite:///data/results.db")
    return create_engine(db_url)

def initialize_settings_table(conn):
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS system_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
    """))
    defaults = {
        "serving_mode": "ml",
        "canary_enabled": "false",
        "canary_traffic_pct": "10",
        "canary_version": ""
    }
    for k, v in defaults.items():
        res = conn.execute(text("SELECT value FROM system_settings WHERE key = :key"), {"key": k})
        if res.fetchone() is None:
            conn.execute(text("INSERT INTO system_settings VALUES (:key, :value)"), {"key": k, "value": v})

def get_setting(conn, key, default):
    res = conn.execute(text("SELECT value FROM system_settings WHERE key = :key"), {"key": key})
    row = res.fetchone()
    return row[0] if row else default

def load_serving_models():
    """Load Champion and Canary (if enabled) models from MLflow Registry."""
    global champion_model, canary_model, model, canary_version_loaded
    mlflow.set_tracking_uri("sqlite:///data/mlflow.db")
    engine = get_db_engine()
    
    # 1. Always load Champion
    try:
        print("Loading registered model '@champion' from MLflow Registry...")
        champion_model = mlflow.sklearn.load_model("models:/ev-sentiment-model@champion")
        model = champion_model
        print("Champion model loaded successfully!")
    except Exception as e:
        print(f"Error loading Champion model: {e}")
        
    # 2. Check Canary settings
    canary_enabled = False
    canary_ver = ""
    try:
        with engine.connect() as conn:
            initialize_settings_table(conn)
            canary_enabled = get_setting(conn, "canary_enabled", "false").lower() == "true"
            canary_ver = get_setting(conn, "canary_version", "").strip()
    except Exception as e:
        print(f"Error reading canary settings: {e}")
        
    if canary_enabled:
        try:
            print("Canary routing is ENABLED. Loading Canary model from registry...")
            if canary_ver:
                canary_uri = f"models:/ev-sentiment-model/{canary_ver}"
            else:
                canary_uri = "models:/ev-sentiment-model@canary"
            canary_model = mlflow.sklearn.load_model(canary_uri)
            canary_version_loaded = canary_ver or "canary"
            print(f"Canary model ({canary_uri}) loaded successfully!")
        except Exception as e:
            print(f"Error loading Canary model: {e}. Falling back to 100% Champion serving.")
            canary_model = None
            canary_version_loaded = None
    else:
        canary_model = None
        canary_version_loaded = None

def fallback_rule_classifier(text: str) -> dict:
    txt_lower = text.lower()
    pos_words = [
        "love", "happy", "great", "excellent", "good", "perfect", "satisfied", "amazing",
        "tuyệt vời", "rất tốt", "êm ái", "tiết kiệm", "hài lòng", "ưng ý", "quá ngon", "đáng tiền", "mượt mà", "ổn định", "hời"
    ]
    neg_words = [
        "delaygator", "payment-loop", "checkout-freeze", "poor", "terrible", "horrible", "mal", "pésima", "broke", "crash", "stuck",
        "lỗi", "kém", "hỏng", "tệ", "thất vọng", "chán", "ọp ẹp", "chậm", "chờ lâu", "vất vả", "khó chịu"
    ]
    pos_count = sum(1 for w in pos_words if w in txt_lower)
    neg_count = sum(1 for w in neg_words if w in txt_lower)
    if neg_count > pos_count:
        return {"predicted_sentiment": "negative", "confidence": 0.99}
    elif pos_count > neg_count:
        return {"predicted_sentiment": "positive", "confidence": 0.99}
    else:
        return {"predicted_sentiment": "neutral", "confidence": 0.50}

def init_inference_logs_table(engine):
    """Ensure inference_logs table exists and has all required columns safely without failing transactions."""
    try:
        with engine.begin() as conn:
            is_postgres = "postgres" in str(engine.url)
            if is_postgres:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS inference_logs (
                        id SERIAL PRIMARY KEY,
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
                conn.execute(text("ALTER TABLE inference_logs ADD COLUMN IF NOT EXISTS model_route TEXT DEFAULT 'champion';"))
                conn.execute(text("ALTER TABLE inference_logs ADD COLUMN IF NOT EXISTS latency_ms REAL DEFAULT 0.0;"))
                conn.execute(text("ALTER TABLE inference_logs ADD COLUMN IF NOT EXISTS verified_sentiment TEXT DEFAULT NULL;"))
            else:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS inference_logs (
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
                try:
                    cols = [r[1] for r in conn.execute(text("PRAGMA table_info(inference_logs);")).fetchall()]
                    if "model_route" not in cols:
                        conn.execute(text("ALTER TABLE inference_logs ADD COLUMN model_route TEXT DEFAULT 'champion';"))
                    if "latency_ms" not in cols:
                        conn.execute(text("ALTER TABLE inference_logs ADD COLUMN latency_ms REAL DEFAULT 0.0;"))
                    if "verified_sentiment" not in cols:
                        conn.execute(text("ALTER TABLE inference_logs ADD COLUMN verified_sentiment TEXT DEFAULT NULL;"))
                except Exception:
                    pass
    except Exception as e:
        print(f"Table Init Warning: {e}")

def log_prediction_to_db(review_text, cleaned_text, sentiment, confidence, model_route="champion", latency_ms=0.0):
    """Log real-time API predictions into a dedicated inference_logs table dynamically."""
    from datetime import datetime
    engine = get_db_engine()
    params = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "review_text": review_text,
        "cleaned_text": cleaned_text,
        "predicted_sentiment": sentiment,
        "confidence": float(confidence),
        "model_route": model_route,
        "latency_ms": float(latency_ms)
    }
    insert_sql = text("""
        INSERT INTO inference_logs (timestamp, review_text, cleaned_text, predicted_sentiment, confidence, model_route, latency_ms)
        VALUES (:timestamp, :review_text, :cleaned_text, :predicted_sentiment, :confidence, :model_route, :latency_ms);
    """)
    try:
        with engine.begin() as conn:
            conn.execute(insert_sql, params)
    except Exception as e:
        # If table was missing or schema needed migration, initialize safely and retry once
        try:
            init_inference_logs_table(engine)
            with engine.begin() as conn:
                conn.execute(insert_sql, params)
        except Exception as retry_err:
            print(f"Inference Logging Failed: {retry_err}")

# 2. Lifecycle manager to load model once at startup
@asynccontextmanager
async def lifespan(app: FastAPI):
    load_serving_models()
    # Pre-initialize table on startup
    init_inference_logs_table(get_db_engine())
    log_prediction_to_db("init", "init", "neutral", 0.0, "champion", 0.0)
    yield
    print("Shutting down API server...")

app = FastAPI(
    title="Vietnamese EV Sentiment Serving API",
    description="Real-time sentiment scoring with Canary routing (90/10) served from MLflow Registry",
    version="2.0.0",
    lifespan=lifespan
)

# 3. Request body schema
class PredictionRequest(BaseModel):
    review_text: str

class PredictionResponse(BaseModel):
    review_text: str
    cleaned_text: str
    predicted_sentiment: str
    confidence: float
    model_route: str = "champion"
    latency_ms: float = 0.0

# 4. Predict Endpoint
@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest):
    if not request.review_text.strip():
        raise HTTPException(status_code=400, detail="Review text cannot be empty.")
    
    start_time = time.time()
    
    # Check circuit breaker and canary configuration
    engine = get_db_engine()
    serving_mode = "ml"
    canary_enabled = False
    canary_traffic_pct = 10
    try:
        with engine.connect() as conn:
            serving_mode = get_setting(conn, "serving_mode", "ml")
            canary_enabled = get_setting(conn, "canary_enabled", "false").lower() == "true"
            canary_traffic_pct = int(get_setting(conn, "canary_traffic_pct", "10"))
    except Exception:
        pass

    if serving_mode == "fallback":
        res = fallback_rule_classifier(request.review_text)
        prediction = res["predicted_sentiment"]
        confidence = float(res["confidence"])
        cleaned = clean_text(request.review_text) + " [RULE-BASED FALLBACK]"
        latency_ms = (time.time() - start_time) * 1000.0
        log_prediction_to_db(request.review_text, cleaned, prediction, confidence, model_route="fallback_rule", latency_ms=latency_ms)
        return PredictionResponse(
            review_text=request.review_text,
            cleaned_text=cleaned,
            predicted_sentiment=prediction,
            confidence=confidence,
            model_route="fallback_rule",
            latency_ms=round(latency_ms, 2)
        )

    if champion_model is None:
        raise HTTPException(
            status_code=503, 
            detail="Sentiment prediction model is currently unavailable."
        )
    
    try:
        cleaned = clean_text(request.review_text)
        
        # Traffic Routing: Decide between Champion and Canary
        selected_model = champion_model
        route = "champion"
        
        global canary_model
        if canary_enabled and canary_model is None:
            load_serving_models()
            
        if canary_enabled and canary_model is not None:
            roll = random.randint(1, 100)
            if roll <= canary_traffic_pct:
                selected_model = canary_model
                route = "canary"
            else:
                selected_model = champion_model
                route = "champion"
        
        # Run inference
        prediction = selected_model.predict([cleaned])[0]
        probs = selected_model.predict_proba([cleaned])[0]
        classes = selected_model.classes_
        pred_idx = list(classes).index(prediction)
        confidence = float(probs[pred_idx])
        latency_ms = (time.time() - start_time) * 1000.0
        
        # Log prediction transaction with route & latency telemetry
        log_prediction_to_db(request.review_text, cleaned, prediction, confidence, model_route=route, latency_ms=latency_ms)
        
        return PredictionResponse(
            review_text=request.review_text,
            cleaned_text=cleaned,
            predicted_sentiment=prediction,
            confidence=confidence,
            model_route=route,
            latency_ms=round(latency_ms, 2)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Inference failed: {str(e)}")

# Reload Endpoint
@app.post("/reload-models")
async def reload_models():
    """Trigger reload of models from MLflow Registry after promotion, rollback, or canary activation."""
    try:
        load_serving_models()
        return {
            "status": "success",
            "champion_loaded": champion_model is not None,
            "canary_loaded": canary_model is not None,
            "canary_version": canary_version_loaded
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to reload models: {str(e)}")

# Health Check Endpoint
@app.get("/health")
async def health():
    return {
        "status": "healthy" if champion_model is not None else "degraded",
        "champion_loaded": champion_model is not None,
        "canary_loaded": canary_model is not None,
        "canary_version": canary_version_loaded
    }
