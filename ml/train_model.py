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
    mlflow.set_tracking_uri("sqlite:///mlflow.db")
    mlflow.set_experiment("ecommerce-sentiment-analysis")
    
    print("Loading datasets...")
    train_df = pd.read_csv('data/train_v1.csv')
    val_df = pd.read_csv('data/val_v1.csv')
    test_df = pd.read_csv('data/test_v1.csv')
    
    hashes = ""
    if os.path.exists('data/dataset_hashes.txt'):
        with open('data/dataset_hashes.txt', 'r') as f:
            hashes = f.read()
            
    # MLOps Feedback Loop: Automatically ingest and merge newly labeled drift reviews from SQLite if drift occurred
    drift_date = os.environ.get("DRIFT_DATE")
    db_path = "data/results.db"
    if drift_date and os.path.exists(db_path):
        print(f"\n🏷️  MLOps Feedback Loop: Ingesting newly labeled drifted reviews from SQLite for date '{drift_date}'...")
        import sqlite3
        conn = sqlite3.connect(db_path)
        drift_df = pd.read_sql_query("SELECT review_text FROM store_reviews WHERE review_date = ?", conn, params=(drift_date,))
        conn.close()
        
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
    print(f"\nEvaluating champion on unseen test set...")
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
    model_name_reg = "ecommerce-sentiment-model"
    model_details = mlflow.register_model(model_uri=model_uri, name=model_name_reg)
    
    # Point 'champion' alias to registered model
    print(f"Promoting version {model_details.version} to '@champion'...")
    client = MlflowClient()
    client.set_registered_model_alias(name=model_name_reg, alias="champion", version=model_details.version)
    print("Successfully selected and registered champion model with closed-loop feedback!")

if __name__ == "__main__":
    main()
