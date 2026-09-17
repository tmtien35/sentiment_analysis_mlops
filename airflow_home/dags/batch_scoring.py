import os, ssl, urllib3, numpy as np, pandas as pd, requests, mlflow, mlflow.sklearn
from datetime import datetime
from sqlalchemy import create_engine, text
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


def load_dotenv_config():
    """Load key-value pairs from .env file into os.environ if not already defined."""
    candidate_paths = [
        os.path.join(project_root, ".env"),
        os.path.join(os.getcwd(), ".env"),
        ".env"
    ]
    for p in candidate_paths:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            k, v = line.split("=", 1)
                            k, v = k.strip(), v.strip()
                            if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                                v = v[1:-1]
                            if k not in os.environ:
                                os.environ[k] = v
                break
            except Exception:
                pass


def send_drift_email(subject: str, html_body: str, ds: str = None) -> bool:
    """Send real-time alert email via Gmail SMTP with dual-port fallback (587 STARTTLS -> 465 SSL)."""
    load_dotenv_config()
    sender_email = os.environ.get("SMTP_SENDER", "").strip()
    sender_password = os.environ.get("SMTP_PASSWORD", "").strip()
    recipient_email = os.environ.get("SMTP_RECIPIENT", "").strip() or sender_email

    if not sender_password or not sender_email:
        print("ℹ️  [EMAIL ALERT] Real email sending skipped (SMTP_SENDER or SMTP_PASSWORD not configured).")
        print(" -> To receive real emails in your inbox:")
        print("    1. Copy .env.example to .env")
        print("    2. Set SMTP_SENDER=your_email@gmail.com and SMTP_PASSWORD=your_16_char_app_password")
        return False

    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = recipient_email
    msg.attach(MIMEText(html_body, "html"))

    print(f"📧 [EMAIL ALERT] Attempting to deliver alert email from '{sender_email}' to '{recipient_email}'...")

    # Method 1: Try Port 587 (TLS / STARTTLS)
    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_email, msg.as_string())
        print(f"✅ [EMAIL ALERT] Alert email successfully delivered to '{recipient_email}' via Gmail Port 587 (STARTTLS)!")
        return True
    except smtplib.SMTPAuthenticationError as auth_err:
        print(f"❌ [EMAIL AUTH ERROR] Gmail rejected credentials: {auth_err}")
        print(" -> Note: Regular Gmail passwords are not accepted. Use a 16-character App Password.")
        print(" -> Generate at: https://myaccount.google.com/apppasswords")
        return False
    except Exception as e587:
        print(f"⚠️ [EMAIL ALERT] Port 587 connection failed ({e587}). Auto-attempting Port 465 (SSL fallback)...")

    # Method 2: Fallback to Port 465 (SSL)
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=10) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_email, msg.as_string())
        print(f"✅ [EMAIL ALERT] Alert email successfully delivered to '{recipient_email}' via Gmail Port 465 (SSL)!")
        return True
    except smtplib.SMTPAuthenticationError as auth_err:
        print(f"❌ [EMAIL AUTH ERROR] Gmail rejected credentials on Port 465: {auth_err}")
        return False
    except Exception as e465:
        print(f"❌ [EMAIL ALERT] Both Port 587 and Port 465 connection attempts failed: {e465}")
        print(" -> Note: If running on a cloud instance, verify outbound internet access to smtp.gmail.com.")
        return False


def get_db_engine():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        sqlite_file = os.path.join(project_root, "data", "results.db").replace("\\", "/")
        db_url = f"sqlite:///{sqlite_file}"
    return create_engine(db_url)

def get_mlflow_tracking_uri():
    env_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if env_uri:
        return env_uri
    db_path = os.path.join(project_root, "data", "mlflow.db").replace("\\", "/")
    return f"sqlite:///{db_path}"

def run_batch_scoring(ds: str = None, auto_retrain: bool = True):
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
                run_batch_scoring(d, auto_retrain=auto_retrain)
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
        # Fallback: check if there are unprocessed reviews on ANY other date (e.g. advanced simulation date or customer submission)
        with engine.connect() as conn:
            res_all = conn.execute(text("SELECT DISTINCT review_date FROM store_reviews WHERE is_processed = 0 ORDER BY review_date ASC"))
            other_dates = [r[0] for r in res_all.fetchall() if r[0]]
        if other_dates:
            print(f"ℹ️  No pending reviews for '{ds}', but found unprocessed reviews for date(s): {other_dates}. Processing now...")
            for od in other_dates:
                run_batch_scoring(od, auto_retrain=auto_retrain)
            return
        print(f"✅ STABLE: No pending reviews found for date {ds}. Skipping.")
        return

    print(f"Found {pending_count} pending reviews. Starting batch prediction...")
    df_pending['cleaned_text'] = df_pending['review_text'].apply(clean_text)
    
    from ml.model_loader import load_champion_model_robust
    model = load_champion_model_robust(project_root)

    if model is not None:
        df_pending['predicted_sentiment'] = model.predict(df_pending['cleaned_text'])
        probs = model.predict_proba(df_pending['cleaned_text'])
        classes = list(model.classes_)
        confidences = [round(float(probs[i][classes.index(p)]), 4) for i, p in enumerate(df_pending['predicted_sentiment'])]
        df_pending['confidence'] = confidences
        print(f"Batch inference complete for {len(df_pending)} records using Champion ML model.")
    else:
        # Fallback to rule classifier if MLflow model cannot be loaded (Zero Crash Guarantee)
        print("Falling back to rule-based classifier for batch scoring...")
        def _rule_predict(txt):
            txt_lower = str(txt).lower()
            neg_words = ["lỗi", "kém", "hỏng", "tệ", "thất vọng", "chán", "ọp ẹp", "chậm", "chờ lâu", "vất vả", "khó chịu"]
            pos_words = ["tuyệt vời", "rất tốt", "êm ái", "tiết kiệm", "hài lòng", "ưng ý", "quá ngon", "đáng tiền", "mượt mà", "ổn định", "hời"]
            neg_c = sum(1 for w in neg_words if w in txt_lower)
            pos_c = sum(1 for w in pos_words if w in txt_lower)
            if neg_c > pos_c:
                return "negative", 0.90
            elif pos_c > neg_c:
                return "positive", 0.90
            return "neutral", 0.60
            
        rule_results = df_pending['cleaned_text'].apply(_rule_predict)
        df_pending['predicted_sentiment'] = [r[0] for r in rule_results]
        df_pending['confidence'] = [r[1] for r in rule_results]

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

        # Auto-heal legacy dummy predictions (0.6 or 0.9) if real ML model is loaded
        if model is not None:
            try:
                legacy_dummy = conn.execute(text("SELECT review_id, cleaned_text FROM predictions WHERE confidence IN (0.6, 0.9, 0.60, 0.90)")).fetchall()
                if legacy_dummy:
                    print(f"🔄 Auto-healing {len(legacy_dummy)} legacy dummy records with genuine model probabilities...")
                    for lid, ltxt in legacy_dummy:
                        if not ltxt:
                            continue
                        lp = model.predict([ltxt])[0]
                        lprobs = model.predict_proba([ltxt])[0]
                        lc = round(float(lprobs[list(model.classes_).index(lp)]), 4)
                        conn.execute(text("UPDATE predictions SET predicted_sentiment = :p, confidence = :c WHERE review_id = :rid"), {"p": lp, "c": lc, "rid": lid})
                    print("✅ Legacy predictions successfully auto-healed.")
            except Exception as e_dummy:
                print(f"Notice: Legacy dummy auto-heal skipped: {e_dummy}")

    # Retrieve ALL predictions for date ds to compute cumulative daily metrics
    with engine.connect() as conn:
        res_all = conn.execute(text("SELECT * FROM predictions WHERE review_date = :ds"), {"ds": ds})
        df_all = pd.DataFrame(res_all.fetchall(), columns=res_all.keys())
    cumulative_count = len(df_all)
    avg_confidence = round(float(np.mean(df_all['confidence'])), 4)
    
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
    
    # --- CHECK RULES: PERSISTENT DRIFT EVALUATION ---
    is_persistent_drift = False
    if drift_detected:
        try:
            with engine.connect() as conn:
                res_prior = conn.execute(
                    text("SELECT drift_detected, psi_score FROM drift_metrics WHERE batch_date < :ds ORDER BY batch_date DESC LIMIT 1"),
                    {"ds": ds}
                )
                prior_row = res_prior.fetchone()
                if prior_row and prior_row[0] >= 1:
                    is_persistent_drift = True
                    print(f"🚨 [RULE TRIGGER] Persistent drift confirmed: Consecutive drift detected across >= 2 historical batches (prior day drift={prior_row[0]}, PSI={prior_row[1]:.4f}).")
                else:
                    print(f"ℹ️  [RULE TRIGGER] Isolated single-day drift spike (PSI={psi_score:.4f} >= 0.15). Retraining deferred until persistent drift (2 consecutive days) is confirmed.")
        except Exception as e_rule:
            print(f" -> Error checking prior drift metrics: {e_rule}")
            is_persistent_drift = False

        status_text = "Persistent Drift Confirmed (>= 2 consecutive drift days with PSI >= 0.15) - Retraining Triggered" if is_persistent_drift else "Isolated Drift Spike (Day 1) - Retraining Deferred (Requires 2 consecutive drift days)"
        action_text = "Retraining triggered automatically in the background. Serving continues safely on @champion." if is_persistent_drift else "Retraining deferred until persistent drift is confirmed across 2 consecutive days."
        html = f"""<div style="font-family:Arial;max-width:450px;border:1px solid #ddd;padding:15px;border-radius:8px;"><h2 style="color:#e74c3c;border-bottom:2px solid #e74c3c;padding-bottom:10px;">🚨 DATA DRIFT DETECTED</h2><p>Significant vocabulary shift detected on <b>{ds}</b>.</p><p><b>Status:</b> {status_text}</p><p><b>PSI Score:</b> <span style="color:#e74c3c;font-weight:bold;">{psi_score:.4f}</span> (Threshold: 0.1500)</p><p style="background:#fdf2f2;padding:10px;color:#9b1c1c;"><strong>{action_text}</strong></p><p style="text-align:center;"><a href="http://localhost:8501" style="background:#3498db;color:white;padding:8px 16px;text-decoration:none;font-weight:bold;border-radius:4px;">Open Streamlit</a></p></div>"""
        path = os.path.join(project_root, "data", "alerts")
        os.makedirs(path, exist_ok=True)
        fpath = os.path.join(path, f"drift_alert_{ds.replace('-', '_')}.html")
        with open(fpath, "w", encoding="utf-8") as f: f.write(html)
        print(f"📧 [EMAIL ALERT] Saved HTML email mockup to: {fpath}")
        
        # Real-time SMTP email sending
        send_drift_email(
            subject=f"🚨 MLOps Alert: Data Drift Detected on {ds}!",
            html_body=html,
            ds=ds
        )
        
        should_retrain = is_persistent_drift and auto_retrain and (os.environ.get("ENABLE_SELF_HEALING", "true").lower() not in ("0", "false", "no"))
        if should_retrain:
            import sys, subprocess
            print("\n🚨 [SELF-HEALING] Persistent Data Drift Confirmed! Triggering automated retraining pipeline...")
            env = os.environ.copy()
            env["PYTHONPATH"] = project_root
            env["DRIFT_DATE"] = ds
            train_script = os.path.join(project_root, "ml", "train_model.py")
            try:
                subprocess.run([sys.executable, train_script], env=env, cwd=project_root, check=True)
                print("🚨 [SELF-HEALING] Retraining completed successfully!")
            except Exception as err:
                print(f"🚨 [SELF-HEALING] Retraining failed: {err}")
        elif not is_persistent_drift:
            print(f"ℹ️  [SELF-HEALING] Retraining skipped for {ds}: Isolated drift spike (awaiting persistent drift confirmation).")
        else:
            print(f"ℹ️  [SELF-HEALING] Automated retraining skipped for {ds} (auto_retrain={auto_retrain}, ENABLE_SELF_HEALING={os.environ.get('ENABLE_SELF_HEALING', 'true')}).")
            
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
    try:
        from datetime import timezone, timedelta
        vn_now = datetime.now(timezone(timedelta(hours=7)))
        timestamp_str = vn_now.strftime("%H%M%S")
    except Exception:
        timestamp_str = datetime.now().strftime("%H%M%S")
    with mlflow.start_run(run_name=f"Batch_{ds}_{timestamp_str}"):
        mlflow.log_param("batch_date", ds)
        mlflow.log_metric("batch_row_count", cumulative_count)
        mlflow.log_metric("new_reviews_count", pending_count)
        mlflow.log_metric("batch_avg_confidence", avg_confidence)
        mlflow.log_metric("batch_psi_score", psi_score)
        mlflow.log_param("batch_drift_flag", str(drift_detected))
        mlflow.log_param("batch_persistent_drift", str(is_persistent_drift))
        
    print(f"Batch {ds} completed successfully!")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run batch scoring and drift monitoring")
    parser.add_argument("--date", type=str, default=None, help="Execution date YYYY-MM-DD (defaults to all pending dates)")
    parser.add_argument("--no-retrain", action="store_true", help="Disable auto retraining loop if drift detected")
    args = parser.parse_args()
    run_batch_scoring(ds=args.date, auto_retrain=not args.no_retrain)


