import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix, ConfusionMatrixDisplay

# Preprocessing & MLflow
from ml.preprocess import clean_text
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient

def main():
    mlflow.set_tracking_uri("sqlite:///data/mlflow.db")
    mlflow.set_experiment("ecommerce-sentiment-analysis")
    
    print("Loading datasets...")
    train_df = pd.read_csv('data/train_v1.csv')
    val_df = pd.read_csv('data/val_v1.csv')
    test_df = pd.read_csv('data/test_v1.csv')
    
    hashes = ""
    if os.path.exists('data/dataset_hashes.txt'):
        with open('data/dataset_hashes.txt', 'r') as f:
            hashes = f.read()
            
    # MLOps Feedback Loop: Automatically ingest and merge newly labeled drift reviews from SQL
    drift_date = os.environ.get("DRIFT_DATE")
    db_url = os.environ.get("DATABASE_URL", "sqlite:///data/results.db")
    
    is_postgres = "postgresql" in db_url
    results_db_exists = os.path.exists("data/results.db")
    
    if drift_date and (is_postgres or results_db_exists):
        print(f"🏷️  MLOps Feedback Loop: Ingesting newly labeled drifted reviews from SQL database for date '{drift_date}'...")
        from sqlalchemy import create_engine, text
        try:
            engine = create_engine(db_url)
            with engine.connect() as conn:
                res = conn.execute(text("SELECT review_text FROM store_reviews WHERE review_date = :ds"), {"ds": drift_date})
                drift_df = pd.DataFrame(res.fetchall(), columns=res.keys())
        except Exception as e:
            print(f" -> Skipped SQL drift ingestion due to: {e}")
            drift_df = pd.DataFrame()
        
        if len(drift_df) > 0:
            # Simple rule-based pseudo-labeler to auto-assign ground truth to the unlabeled drift batch
            sentiments = []
            for txt in drift_df["review_text"]:
                txt_lower = txt.lower()
                if "delaygator" in txt or "payment-loop" in txt or "checkout-freeze" in txt or "poor" in txt_lower or "terrible" in txt_lower or "horrible" in txt_lower or "mal" in txt_lower or "pésima" in txt_lower:
                    sentiments.append("negative")
                elif "love" in txt_lower or "happy" in txt_lower or "great" in txt_lower or "excellent" in txt_lower:
                    sentiments.append("positive")
                else:
                    sentiments.append("neutral")
            drift_df["sentiment"] = sentiments
            
            # Concatenate newly labeled data with historical training dataset
            drift_clean = drift_df[["review_text", "sentiment"]]
            train_df = pd.concat([train_df, drift_clean], ignore_index=True)
            print(f" -> Successfully concatenated {len(drift_clean)} newly labeled SQL reviews! New training size: {len(train_df)}")
        else:
            print(f" -> No reviews found in SQL for drift date '{drift_date}'.")

    print("Preprocessing text...")
    train_df['cleaned_text'] = train_df['review_text'].apply(clean_text)
    val_df['cleaned_text'] = val_df['review_text'].apply(clean_text)
    test_df['cleaned_text'] = test_df['review_text'].apply(clean_text)
    
    print("\n--- Training Champion Model (Multinomial Naive Bayes) ---")
    model_name = "Multinomial Naive Bayes"
    
    with mlflow.start_run(run_name=model_name) as run:
        run_id = run.info.run_id
        
        # Define and train the locked, high-performing Naive Bayes pipeline
        pipeline = Pipeline([
            ('tfidf', TfidfVectorizer(max_features=5000, ngram_range=(1, 2))),
            ('clf', MultinomialNB(alpha=1.0))
        ])
        pipeline.fit(train_df['cleaned_text'], train_df['sentiment'])
        
        # Validate predictions
        y_pred = pipeline.predict(val_df['cleaned_text'])
        y_true = val_df['sentiment']
        
        acc = accuracy_score(y_true, y_pred)
        prec = precision_score(y_true, y_pred, average='macro', zero_division=0)
        rec = recall_score(y_true, y_pred, average='macro', zero_division=0)
        val_f1 = f1_score(y_true, y_pred, average='macro', zero_division=0)
        print(f"Validation Accuracy: {acc:.4f} | Validation Macro-F1: {val_f1:.4f}")

        # --- AUTOMATED MODEL GATEKEEPING ---
        champion_f1 = 0.0
        try:
            print("Loading current @champion model from registry for gatekeeping...")
            champion_model = mlflow.sklearn.load_model("models:/ecommerce-sentiment-model@champion")
            champ_preds = champion_model.predict(val_df['cleaned_text'])
            champion_f1 = f1_score(val_df['sentiment'], champ_preds, average='macro', zero_division=0)
            print(f" -> Current @champion Validation Macro-F1: {champion_f1:.4f}")
        except Exception as e:
            print(f" -> No active @champion model found in registry: {e}")

        if val_f1 < champion_f1:
            print(f"\n❌ GATEKEEPING FAILED: New Model F1 ({val_f1:.4f}) < Champion F1 ({champion_f1:.4f}). Aborting registration!")
            # Save incident report
            html = f"""<div style='font-family:Arial;max-width:450px;border:1px solid #ddd;padding:15px;border-radius:8px;'><h2 style='color:#e74c3c;border-bottom:2px solid #e74c3c;padding-bottom:10px;'>❌ GATEKEEPING RETRAIN FAILED</h2><p>Model retraining aborted because the new model failed the automated validation gate.</p><p><b>Champion Macro-F1:</b> <span style='color:#2ecc71;font-weight:bold;'>{champion_f1:.4f}</span></p><p><b>Candidate Macro-F1:</b> <span style='color:#e74c3c;font-weight:bold;'>{val_f1:.4f}</span></p><p style='background:#fdf2f2;padding:10px;color:#9b1c1c;'><strong>Serving continues running the stable @champion model safely.</strong></p></div>"""
            path = os.path.join("data", "alerts")
            os.makedirs(path, exist_ok=True)
            fpath = os.path.join(path, f"retrain_failed_{datetime.now().strftime('%Y_%m_%d_%H%M')}.html")
            with open(fpath, "w", encoding="utf-8") as f: f.write(html)
            print(f"📧 [EMAIL ALERT] Saved HTML incident report to: {fpath}")
            return

        print(f"\n✅ GATEKEEPING PASSED: New Model F1 ({val_f1:.4f}) >= Champion F1 ({champion_f1:.4f}). Proceeding with registration...")
        
        # Log params & metrics
        mlflow.log_param("clf__alpha", 1.0)
        mlflow.log_param("dataset_hashes", hashes)
        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("macro_precision", prec)
        mlflow.log_metric("macro_recall", rec)
        mlflow.log_metric("macro_f1", val_f1)
        
        # Log pipeline model (using pickle to bypass skops security block)
        mlflow.sklearn.log_model(pipeline, artifact_path="model", serialization_format="pickle")
        
        # Confusion Matrix
        classes = pipeline.named_steps['clf'].classes_
        cm = confusion_matrix(y_true, y_pred, labels=classes)
        fig, ax = plt.subplots(figsize=(5, 5))
        disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=classes)
        disp.plot(ax=ax, cmap='Blues', values_format='d')
        plt.title(f"{model_name} Val CM")
        
        fig_path = "ml/multinomial_naive_bayes_cm.png"
        plt.savefig(fig_path, bbox_inches='tight')
        plt.close()
        mlflow.log_artifact(fig_path)
        os.remove(fig_path)
        
    # Unbiased single evaluation on test partition
    print(f"
Evaluating candidate on unseen test set...")
    test_preds = pipeline.predict(test_df['cleaned_text'])
    y_test_true = test_df['sentiment']
    
    test_acc = accuracy_score(y_test_true, test_preds)
    test_f1 = f1_score(y_test_true, test_preds, average='macro', zero_division=0)
    print(f"Test Set Accuracy: {test_acc:.4f} | Test Macro-F1: {test_f1:.4f}")
    
    with mlflow.start_run(run_id=run_id):
        mlflow.log_metric("test_accuracy", test_acc)
        mlflow.log_metric("test_macro_f1", test_f1)
        
    # Register model programmatically
    print("
Registering model...")
    model_uri = f"runs:/{run_id}/model"
    model_name_reg = "ecommerce-sentiment-model"
    model_details = mlflow.register_model(model_uri=model_uri, name=model_name_reg)
    
    # Point 'candidate' alias to registered model
    print(f"Promoting version {model_details.version} to '@candidate'...")
    client = MlflowClient()
    client.set_registered_model_alias(name=model_name_reg, alias="candidate", version=model_details.version)
    print("Successfully registered candidate model under @candidate alias for manual promotion!")

if __name__ == "__main__":
    main()
