import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix, ConfusionMatrixDisplay

# Preprocessing & MLflow
from ml.preprocess import clean_text
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient

def main():
    mlflow.set_tracking_uri("sqlite:///data/mlflow.db")
    mlflow.set_experiment("ev-sentiment-analysis")
    
    # Load Vietnamese EV reviews dataset
    ev_path = os.environ.get("TRAIN_DATA_PATH", "data/ev_reviews_vietnam_1529_cleaned.csv")
    if not os.path.exists(ev_path):
        raise FileNotFoundError(f"EV reviews dataset not found at '{ev_path}'!")
        
    print(f"Loading Vietnamese EV reviews dataset from {ev_path}...")
    full_df = pd.read_csv(ev_path)
    if 'text' in full_df.columns and 'review_text' not in full_df.columns:
        full_df['review_text'] = full_df['text']
        
    from sklearn.model_selection import train_test_split
    train_df, temp_df = train_test_split(full_df, test_size=0.20, random_state=42, stratify=full_df['sentiment'])
    val_df, test_df = train_test_split(temp_df, test_size=0.50, random_state=42, stratify=temp_df['sentiment'])
    print(f"EV Split: Train={len(train_df)}, Val={len(val_df)}, Test={len(test_df)}")
    
    hashes = ""
    if os.path.exists('data/dataset_hashes.txt'):
        with open('data/dataset_hashes.txt', 'r') as f:
            hashes = f.read()
            
    # MLOps Feedback Loop: Automatically ingest and merge newly labeled reviews from SQL
    db_url = os.environ.get("DATABASE_URL", "sqlite:///data/results.db")
    
    is_postgres = "postgresql" in db_url
    results_db_exists = os.path.exists("data/results.db")
    
    if is_postgres or results_db_exists:
        from sqlalchemy import create_engine, text
        try:
            engine = create_engine(db_url)
            # 1. Fetch ALL human-verified reviews (Active Learning Ground Truth)
            with engine.connect() as conn:
                res_verified = conn.execute(text("SELECT review_text, verified_sentiment FROM store_reviews WHERE verified_sentiment IS NOT NULL"))
                verified_df = pd.DataFrame(res_verified.fetchall(), columns=res_verified.keys())
            
            if len(verified_df) > 0:
                print(f"🏷️  Active Learning: Ingesting {len(verified_df)} human-verified (ground-truth) reviews from SQL...")
                verified_df = verified_df.rename(columns={"verified_sentiment": "sentiment"})
            else:
                verified_df = pd.DataFrame()
        except Exception as e:
            print(f" -> Skipped SQL verified reviews ingestion due to: {e}")
            verified_df = pd.DataFrame()

        # 2. Fetch drift-date reviews if specified
        drift_date = os.environ.get("DRIFT_DATE")
        drift_df = pd.DataFrame()
        if drift_date:
            print(f"🏷️  MLOps Feedback Loop: Scanning drifted reviews for date '{drift_date}'...")
            try:
                with engine.connect() as conn:
                    res_drift = conn.execute(text("SELECT review_text, verified_sentiment FROM store_reviews WHERE review_date = :ds"), {"ds": drift_date})
                    drift_raw_df = pd.DataFrame(res_drift.fetchall(), columns=res_drift.keys())
                
                if len(drift_raw_df) > 0:
                    sentiments = []
                    for idx, row in drift_raw_df.iterrows():
                        # If already verified by human, use it
                        if row["verified_sentiment"] is not None and str(row["verified_sentiment"]).strip() != "" and str(row["verified_sentiment"]).lower() != "none" and str(row["verified_sentiment"]).lower() != "null":
                            sentiments.append(row["verified_sentiment"])
                        else:
                            # Fallback to Vietnamese EV keyword pseudo-labeler
                            txt = row["review_text"]
                            txt_lower = txt.lower()
                            neg_words = ["kém", "lỗi", "chậm", "tệ", "thất vọng", "hỏng", "sụt pin", "đơ", "ồn", "đắt", "chờ", "yếu", "xấu", "delay", "kẹt"]
                            pos_words = ["tốt", "êm", "hài lòng", "tiết kiệm", "thích", "đẹp", "tuyệt", "nhanh", "hiện đại", "mượt", "chu đáo", "ổn định", "bền"]
                            if any(w in txt_lower for w in neg_words):
                                sentiments.append("negative")
                            elif any(w in txt_lower for w in pos_words):
                                sentiments.append("positive")
                            else:
                                sentiments.append("neutral")
                    drift_raw_df["sentiment"] = sentiments
                    drift_df = drift_raw_df[["review_text", "sentiment"]]
            except Exception as e:
                print(f" -> Skipped SQL drift-date ingestion due to: {e}")

        # Combine both new data sources
        new_data = pd.concat([verified_df, drift_df], ignore_index=True)
        if len(new_data) > 0:
            # Deduplicate by review_text to make sure verified labels overwrite pseudo-labels
            new_data = new_data.drop_duplicates(subset=["review_text"], keep="first")
            train_df = pd.concat([train_df, new_data], ignore_index=True)
            print(f" -> Successfully concatenated {len(new_data)} total new/verified SQL reviews! New training size: {len(train_df)}")

    print("Preprocessing text...")
    train_df['cleaned_text'] = train_df['review_text'].apply(clean_text)
    val_df['cleaned_text'] = val_df['review_text'].apply(clean_text)
    test_df['cleaned_text'] = test_df['review_text'].apply(clean_text)
    
    print("\n--- Training Champion Model (Logistic Regression) ---")
    model_name = "Logistic Regression"
    
    with mlflow.start_run(run_name=model_name) as run:
        run_id = run.info.run_id
        
        # Define and train the locked, high-performing Logistic Regression pipeline
        pipeline = Pipeline([
            ('tfidf', TfidfVectorizer(max_features=8000, ngram_range=(1, 2), min_df=1, sublinear_tf=True)),
            ('clf', LogisticRegression(C=2.0, class_weight='balanced', max_iter=1000, random_state=42))
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
        champion_model_exists = False
        try:
            print("Loading current @champion model from registry for gatekeeping...")
            champion_model = mlflow.sklearn.load_model("models:/ev-sentiment-model@champion")
            champ_preds = champion_model.predict(val_df['cleaned_text'])
            champion_f1 = f1_score(val_df['sentiment'], champ_preds, average='macro', zero_division=0)
            print(f" -> Current @champion Validation Macro-F1: {champion_f1:.4f}")
            champion_model_exists = True
        except Exception as e:
            print(f" -> No active @champion model found in registry: {e}")

        passed_gatekeeper = (not champion_model_exists) or (val_f1 >= champion_f1)
        if not passed_gatekeeper:
            print(f"\n❌ GATEKEEPING BLOCKED AUTO-PROMOTION: New Model F1 ({val_f1:.4f}) < Champion F1 ({champion_f1:.4f}).")
            print(" -> Registering model as '@candidate' (Contender) for Admin review & scorecard comparison on Streamlit.")
            # Save incident report
            html = f"""<div style='font-family:Arial;max-width:500px;border:1px solid #ddd;padding:15px;border-radius:8px;'><h2 style='color:#e74c3c;border-bottom:2px solid #e74c3c;padding-bottom:10px;'>❌ GATEKEEPING BLOCKED AUTO-PROMOTION</h2><p>New model did not outperform Champion on fixed validation set.</p><p><b>Champion Macro-F1:</b> <span style='color:#2ecc71;font-weight:bold;'>{champion_f1:.4f}</span></p><p><b>Candidate Macro-F1:</b> <span style='color:#e74c3c;font-weight:bold;'>{val_f1:.4f}</span></p><p><b>Training Dataset Size:</b> {len(train_df)} samples</p><p style='background:#fdf2f2;padding:10px;color:#9b1c1c;'><strong>Serving continues safely on @champion. Candidate model is registered as @candidate for manual Admin review on Streamlit.</strong></p></div>"""
            path = os.path.join("data", "alerts")
            os.makedirs(path, exist_ok=True)
            fpath = os.path.join(path, f"retrain_failed_{datetime.now().strftime('%Y_%m_%d_%H%M')}.html")
            with open(fpath, "w", encoding="utf-8") as f: f.write(html)
            print(f"📧 [EMAIL ALERT] Saved HTML incident report to: {fpath}")
        else:
            print(f"\n✅ GATEKEEPING PASSED: New Model F1 ({val_f1:.4f}) >= Champion F1 ({champion_f1:.4f}). Proceeding with registration...")
            # Clear gatekeeping failure reports since we have successfully passed the gatekeeper
            import glob
            try:
                for old_rep in glob.glob(os.path.join("data", "alerts", "retrain_failed_*.html")):
                    os.remove(old_rep)
                    print(f"🗑️  Cleared old gatekeeper failure report: {old_rep}")
            except Exception as e:
                print(f" -> Failed to clear gatekeeper failure reports: {e}")
        
        # Log params & metrics
        mlflow.log_param("clf__C", 2.0)
        mlflow.log_param("clf__solver", "lbfgs")
        mlflow.log_param("clf__class_weight", "balanced")
        mlflow.log_param("tfidf__sublinear_tf", True)
        mlflow.log_param("tfidf__max_features", 8000)
        mlflow.log_param("model_family", "LogisticRegression")
        mlflow.log_param("dataset_hashes", hashes)
        mlflow.log_param("train_dataset_size", len(train_df))
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
        
        fig_path = "ml/logistic_regression_cm.png"
        plt.savefig(fig_path, bbox_inches='tight')
        plt.close()
        mlflow.log_artifact(fig_path)
        if os.path.exists(fig_path):
            os.remove(fig_path)
        
    # Unbiased single evaluation on test partition
    print("\nEvaluating candidate on unseen test set...")
    test_preds = pipeline.predict(test_df['cleaned_text'])
    y_test_true = test_df['sentiment']
    
    test_acc = accuracy_score(y_test_true, test_preds)
    test_f1 = f1_score(y_test_true, test_preds, average='macro', zero_division=0)
    print(f"Test Set Accuracy: {test_acc:.4f} | Test Macro-F1: {test_f1:.4f}")
    
    with mlflow.start_run(run_id=run_id):
        mlflow.log_metric("test_accuracy", test_acc)
        mlflow.log_metric("test_macro_f1", test_f1)
        
    # Register model programmatically
    print("\nRegistering model...")
    model_uri = f"runs:/{run_id}/model"
    model_name_reg = "ev-sentiment-model"
    model_details = mlflow.register_model(model_uri=model_uri, name=model_name_reg)
    
    client = MlflowClient()
    if passed_gatekeeper:
        # Promote registered model directly to champion
        print(f"Promoting version {model_details.version} to '@champion'...")
        client.set_registered_model_alias(name=model_name_reg, alias="champion", version=model_details.version)
        print(f"🏆 Successfully promoted version {model_details.version} to '@champion'!")
        try:
            client.delete_registered_model_alias(name=model_name_reg, alias="candidate")
        except Exception:
            pass
    else:
        # Point 'candidate' alias to registered contender model for admin review
        print(f"Assigning version {model_details.version} to '@candidate' (Contender)...")
        client.set_registered_model_alias(name=model_name_reg, alias="candidate", version=model_details.version)
        print(f"🥊 Version {model_details.version} assigned to '@candidate'. Serving continues on Champion.")

if __name__ == "__main__":
    main()
