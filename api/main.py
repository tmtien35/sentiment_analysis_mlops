import os
import ssl
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

# Global model variable
model = None

from sqlalchemy import create_engine, text

def get_db_engine():
    db_url = os.environ.get("DATABASE_URL", "sqlite:///data/results.db")
    return create_engine(db_url)

def log_prediction_to_db(review_text, cleaned_text, sentiment, confidence):
    """Log real-time API predictions into a dedicated inference_logs table dynamically."""
    from datetime import datetime
    engine = get_db_engine()
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
                        confidence REAL
                    );
                """))
            else:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS inference_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT,
                        review_text TEXT,
                        cleaned_text TEXT,
                        predicted_sentiment TEXT,
                        confidence REAL
                    );
                """))
                
            conn.execute(text("""
                INSERT INTO inference_logs (timestamp, review_text, cleaned_text, predicted_sentiment, confidence)
                VALUES (:timestamp, :review_text, :cleaned_text, :predicted_sentiment, :confidence);
            """), {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "review_text": review_text,
                "cleaned_text": cleaned_text,
                "predicted_sentiment": sentiment,
                "confidence": float(confidence)
            })
    except Exception as e:
        print(f"Inference Logging Failed: {e}")

# 2. Lifecycle manager to load model once at startup
@asynccontextmanager
async def lifespan(app: FastAPI):
    global model
    try:
        print("Loading registered model '@champion' from MLflow Registry...")
        # Point to our local SQLite DB tracking store
        mlflow.set_tracking_uri("sqlite:///data/mlflow.db")
        model_uri = "models:/ecommerce-sentiment-model@champion"
        model = mlflow.sklearn.load_model(model_uri)
        print("Model loaded successfully!")
        
        # Pre-initialize table on startup
        log_prediction_to_db("init", "init", "neutral", 0.0)
    except Exception as e:
        print(f"Error loading model from MLflow: {e}")
    yield
    print("Shutting down API server...")

app = FastAPI(
    title="E-Commerce Sentiment Serving API",
    description="Real-time sentiment scoring of product reviews served from MLflow Registry",
    version="1.0.0",
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

# 4. Predict Endpoint
@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest):
    if model is None:
        raise HTTPException(
            status_code=503, 
            detail="Sentiment prediction model is currently unavailable."
        )
    
    if not request.review_text.strip():
        raise HTTPException(status_code=400, detail="Review text cannot be empty.")
    
    try:
        # Preprocess text using the identical clean_text function (prevents training-serving skew)
        cleaned = clean_text(request.review_text)
        
        # Run inference
        prediction = model.predict([cleaned])[0]
        
        # Calculate prediction probabilities (MultinomialNB and LogReg fully support predict_proba)
        probs = model.predict_proba([cleaned])[0]
        classes = model.classes_
        pred_idx = list(classes).index(prediction)
        confidence = float(probs[pred_idx])
        
        # Log prediction transaction in the database in the background
        log_prediction_to_db(request.review_text, cleaned, prediction, confidence)
        
        return PredictionResponse(
            review_text=request.review_text,
            cleaned_text=cleaned,
            predicted_sentiment=prediction,
            confidence=confidence
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Inference failed: {str(e)}")

# Health Check Endpoint
@app.get("/health")
async def health():
    return {
        "status": "healthy" if model is not None else "degraded",
        "model_loaded": model is not None
    }
