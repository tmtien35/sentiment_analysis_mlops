import os, ssl, urllib3, numpy as np, pandas as pd, requests, mlflow, mlflow.sklearn
from datetime import datetime
from sqlalchemy import create_engine, text

def get_db_engine():
    return create_engine(os.environ.get("DATABASE_URL", "sqlite:///data/results.db"))

def run_batch_scoring(ds: str = None):
    engine = get_db_engine()
    if ds is None:
        print("Scanning 'store_reviews' for all distinct unprocessed dates...")
        try:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT DISTINCT review_date FROM store_reviews WHERE is_processed = 0 ORDER BY review_date ASC"))
                unprocessed_dates = [r[0] for r in res.fetchall() if r[0]]
            if not unprocessed_dates:
                print("✅ STABLE: No pending reviews found in the entire database. Skipping.")
                return
            
            print(f"Found pending reviews across {len(unprocessed_dates)} distinct date(s): {unprocessed_dates}")
            for d in unprocessed_dates:
                print(f"\n---> Running batch scoring automatically for date: {d}")
                run_batch_scoring(d)
            print("\n✅ SUCCESS: All outstanding reviews scored and locked successfully!")
            return
        except Exception as e:
            print(f"Error querying pending dates: {e}")
            return
    from ml.preprocess import clean_text
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    urllib3.util.ssl_.create_urllib3_context = lambda *args, **k: ssl._create_unverified_context()
    os.environ['HF_HUB_DISABLE_SSL_VERIFY'] = '1'

    engine = get_db_engine()
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS store_reviews (review_id TEXT PRIMARY KEY, review_date TEXT, category TEXT, review_text TEXT, is_processed INTEGER DEFAULT 0);"))
    
    print(f"Scanning 'store_reviews' for unprocessed on {ds}...")
    with engine.connect() as conn:
        res = conn.execute(text("SELECT * FROM store_reviews WHERE is_processed = 0 AND review_date = :ds"), {"ds": ds})
        df_pending = pd.DataFrame(res.fetchall(), columns=res.keys())
    pending_count = len(df_pending)
    if pending_count == 0:
        print(f"✅ STABLE: No pending reviews found for date {ds}. Skipping.")
        return

    print(f"Found {pending_count} pending reviews. Starting batch prediction...")
    df_pending['cleaned_text'] = df_pending['review_text'].apply(clean_text)
    mlflow.set_tracking_uri("sqlite:///data/mlflow.db")
    model = mlflow.sklearn.load_model("models:/ecommerce-sentiment-model@champion")
    
    df_pending['predicted_sentiment'] = model.predict(df_pending['cleaned_text'])
    probs = model.predict_proba(df_pending['cleaned_text'])
    classes = list(model.classes_)
    confidences = [float(probs[i][classes.index(p)]) for i, p in enumerate(df_pending['predicted_sentiment'])]
    df_pending['confidence'] = confidences

    # Save the new pending predictions into the predictions table (using INSERT OR REPLACE)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS predictions (review_id TEXT PRIMARY KEY, review_date TEXT, category TEXT, review_text TEXT, cleaned_text TEXT, predicted_sentiment TEXT, confidence REAL);"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS drift_metrics (batch_date TEXT PRIMARY KEY, row_count INTEGER, avg_confidence REAL, psi_score REAL, drift_detected INTEGER);"))
        
        for _, r in df_pending.iterrows():
            conn.execute(text("DELETE FROM predictions WHERE review_id = :review_id"), {"review_id": r["review_id"]})
            conn.execute(text("""
                INSERT INTO predictions (review_id, review_date, category, review_text, cleaned_text, predicted_sentiment, confidence)
                VALUES (:review_id, :review_date, :category, :review_text, :cleaned_text, :predicted_sentiment, :confidence)
            """), dict(r))

    # Retrieve ALL predictions for date ds to compute cumulative daily metrics
    with engine.connect() as conn:
        res_all = conn.execute(text("SELECT * FROM predictions WHERE review_date = :ds"), {"ds": ds})
        df_all = pd.DataFrame(res_all.fetchall(), columns=res_all.keys())
    cumulative_count = len(df_all)
    avg_confidence = float(np.mean(df_all['confidence']))
    
    expected_pct = { 'positive': 1/3, 'neutral': 1/3, 'negative': 1/3 }
    counts = df_all['predicted_sentiment'].value_counts()
    actual_pct = {s: float(counts.get(s, 0)) / cumulative_count for s in expected_pct}
    
    psi_score = 0.0
    epsilon = 1e-5
    for s in expected_pct:
        act, exp = actual_pct[s] + epsilon, expected_pct[s]
        psi_score += (act - exp) * np.log(act / exp)
    drift_detected = bool(psi_score >= 0.15)
    print(f"Cumulative Daily Analysis: Total Reviews = {cumulative_count} | PSI = {psi_score:.4f} | Drift Detected = {drift_detected}")
    
    if drift_detected: 
        html = f"""<div style="font-family:Arial;max-width:450px;border:1px solid #ddd;padding:15px;border-radius:8px;"><h2 style="color:#e74c3c;border-bottom:2px solid #e74c3c;padding-bottom:10px;">🚨 DATA DRIFT DETECTED</h2><p>Significant vocabulary shift detected on <b>{ds}</b>.</p><p><b>PSI Score:</b> <span style="color:#e74c3c;font-weight:bold;">{psi_score:.4f}</span> (Threshold: 0.1500)</p><p style="background:#fdf2f2;padding:10px;color:#9b1c1c;"><strong>Retraining triggered automatically in the background.</strong></p><p style="text-align:center;"><a href="http://localhost:8501" style="background:#3498db;color:white;padding:8px 16px;text-decoration:none;font-weight:bold;border-radius:4px;">Open Streamlit</a></p></div>"""
        path = os.path.join("data", "alerts")
        os.makedirs(path, exist_ok=True)
        fpath = os.path.join(path, f"drift_alert_{ds.replace('-', '_')}.html")
        with open(fpath, "w", encoding="utf-8") as f: f.write(html)
        print(f"📧 [EMAIL ALERT] Saved HTML email mockup to: {fpath}")
        
        # Real-time SMTP email sending (using secure starttls)
        try:
            import smtplib
            from email.mime.multipart import MIMEMultipart
            from email.mime.text import MIMEText
            
            sender_email = os.environ.get("SMTP_SENDER", "mlops.alert.system@gmail.com")
            sender_password = os.environ.get("SMTP_PASSWORD")
            recipient_email = "tmtien35@gmail.com"
            
            if sender_password:
                print(f"📧 [EMAIL] Attempting to send real email alert to {recipient_email}...")
                msg = MIMEMultipart("alternative")
                msg["Subject"] = f"🚨 MLOps Alert: Data Drift Detected on {ds}!"
                msg["From"] = sender_email
                msg["To"] = recipient_email
                msg.attach(MIMEText(html, "html"))
                
                with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as server:
                    server.starttls()
                    server.login(sender_email, sender_password)
                    server.sendmail(sender_email, recipient_email, msg.as_string())
                print(f"✅ [EMAIL] Real email alert successfully sent to {recipient_email} via Gmail SMTP!")
            else:
                print("ℹ️  [EMAIL] Real email sending skipped (SMTP_PASSWORD environment variable is not set).")
                print(" -> To receive real emails in your inbox, set the 'SMTP_PASSWORD' environment variable with a Gmail App Password in your Docker environment.")
        except Exception as e:
            print(f"⚠️ [EMAIL] Failed to send real email via SMTP: {e}")
            print(" -> Note: Cloud providers (like GCP/AWS) often block SMTP port 587 by default to prevent spam.")
        
        import sys, subprocess
        print("\n🚨 [SELF-HEALING] Data Drift Detected! Triggering automated retraining pipeline...")
        env = os.environ.copy()
        env["PYTHONPATH"] = os.getcwd()
        env["DRIFT_DATE"] = ds
        try:
            subprocess.run([sys.executable, "ml/train_model.py"], env=env, check=True)
            print("🚨 [SELF-HEALING] Retraining completed successfully! Model updated to @champion.")
        except Exception as err:
            print(f"🚨 [SELF-HEALING] Retraining failed: {err}")
            
    with engine.begin() as conn:
        # Idempotently update drift metrics for this batch run (overwrites previously recorded metrics for today)
        conn.execute(text("DELETE FROM drift_metrics WHERE batch_date = :ds"), {"ds": ds})
        conn.execute(text("INSERT INTO drift_metrics VALUES (:batch_date, :row_count, :avg_confidence, :psi_score, :drift_detected)"), {
            "batch_date": ds, "row_count": int(cumulative_count), "avg_confidence": float(avg_confidence), "psi_score": float(psi_score), "drift_detected": 1 if drift_detected else 0
        })
        
        # State-Locking: Mark only the pending reviews as processed
        print(f"State-Locking: Marking {pending_count} new reviews as processed...")
        conn.execute(text("UPDATE store_reviews SET is_processed = 1 WHERE is_processed = 0 AND review_date = :ds"), {"ds": ds})
        
    print("Logging batch run to MLflow...")
    with mlflow.start_run(run_name=f"Batch_{ds}"):
        mlflow.log_param("batch_date", ds)
        mlflow.log_metric("batch_row_count", cumulative_count)
        mlflow.log_metric("batch_avg_confidence", avg_confidence)
        mlflow.log_metric("batch_psi_score", psi_score)
        mlflow.log_param("batch_drift_flag", str(drift_detected))
        
    print(f"Batch {ds} completed successfully!")
