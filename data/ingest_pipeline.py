import os, sys, argparse
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text

sys.path.append(os.getcwd())
from airflow_home.dags.batch_scoring import run_batch_scoring
from data.crawl_feed import classify_aspect_category

def get_db_engine():
    db_url = os.environ.get("DATABASE_URL", "sqlite:///data/results.db")
    return create_engine(db_url)

def write_to_store_reviews(reviews_list):
    engine = get_db_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS store_reviews (
                review_id TEXT PRIMARY KEY,
                review_date TEXT,
                category TEXT,
                review_text TEXT,
                is_processed INTEGER DEFAULT 0,
                verified_sentiment TEXT DEFAULT NULL
            );
        """))
        for r in reviews_list:
            conn.execute(text("DELETE FROM store_reviews WHERE review_id = :review_id"), {"review_id": r["review_id"]})
            conn.execute(text("""
                INSERT INTO store_reviews (review_id, review_date, category, review_text, is_processed, verified_sentiment)
                VALUES (:review_id, :review_date, :category, :review_text, 0, NULL)
            """), r)

def run_backfill(reset: bool = False):
    print("================================================================")
    print("🚗  ETL PIPELINE: Executing 25-Day Historical EV Backfill (Local)")
    print("================================================================")
    engine = get_db_engine()
    
    if reset:
        print("🧹  Reset flag detected: Clearing store_reviews, predictions, drift_metrics...")
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS store_reviews"))
            conn.execute(text("DROP TABLE IF EXISTS predictions"))
            conn.execute(text("DROP TABLE IF EXISTS drift_metrics"))
        alerts_dir = os.path.join("data", "alerts")
        if os.path.exists(alerts_dir):
            for f in os.listdir(alerts_dir):
                if f.endswith(".html"):
                    try:
                        os.remove(os.path.join(alerts_dir, f))
                    except Exception:
                        pass
        print("✅  Reset completed.")
    else:
        try:
            with engine.connect() as conn:
                res = conn.execute(text("SELECT COUNT(*) FROM store_reviews"))
                cnt = res.fetchone()[0]
                if cnt > 0:
                    print(f"ℹ️  INFO: Database already contains {cnt} persistent reviews in 'store_reviews'.")
                    print(" -> Skipping historical backfill to preserve persistent production history.")
                    print(" -> To reset and rerun backfill: python data/ingest_pipeline.py --backfill --reset")
                    return
        except Exception:
            pass # Table doesn't exist yet, proceed with setup
        
    pool_path = os.environ.get("FEED_POOL_PATH", "data/ev_feed_simulation_pool.csv")
    if not os.path.exists(pool_path):
        print(f"❌ ERROR: Simulation pool not found at '{pool_path}'!")
        return
        
    df_pool = pd.read_csv(pool_path)
    pos_df = df_pool[df_pool["sentiment"] == "positive"].reset_index(drop=True)
    neu_df = df_pool[df_pool["sentiment"] == "neutral"].reset_index(drop=True)
    neg_df = df_pool[df_pool["sentiment"] == "negative"].reset_index(drop=True)
    
    total_days = 25
    yesterday = datetime.now() - timedelta(days=1)
    start_date = yesterday - timedelta(days=total_days - 1)
    
    print(f"Loaded pool ({len(df_pool)} reviews). Sampling 500 balanced real reviews ending yesterday: {yesterday.strftime('%Y-%m-%d')}...")
    
    for day in range(total_days):
        cur_date = start_date + timedelta(days=day)
        ds = cur_date.strftime("%Y-%m-%d")
        clean_date_str = ds.replace("-", "")
        
        # Balanced daily distribution (7 pos, 7/6 neu, 6/7 neg) for stable healthy baseline
        n_p, n_n, n_g = (7, 7, 6) if day % 2 == 0 else (7, 6, 7)
        
        pos_slice = pos_df.iloc[day * 7 : (day + 1) * 7]
        neu_slice = neu_df.iloc[day * 7 : day * 7 + n_n]
        neg_slice = neg_df.iloc[day * 7 : day * 7 + n_g]
        
        day_samples = pd.concat([pos_slice, neu_slice, neg_slice]).sample(frac=1.0, random_state=day).reset_index(drop=True)
        
        day_reviews = []
        for idx, (_, row) in enumerate(day_samples.iterrows(), 1):
            rev_text = str(row["text"]).strip()
            category = classify_aspect_category(rev_text)
            day_reviews.append({
                "review_id": f"FEED_{clean_date_str}_{idx:03d}",
                "review_date": ds,
                "category": category,
                "review_text": rev_text,
                "is_processed": 0,
                "verified_sentiment": None
            })
            
        write_to_store_reviews(day_reviews)
        print(f" - [{day+1}/{total_days}] Ingesting & scoring: {ds} (20 reviews)...")
        run_batch_scoring(ds, auto_retrain=False)
        
    print("================================================================")
    print("✅ SUCCESS: 25-Day stable, real EV history pre-populated offline!")
    print("================================================================")

def ingest_unprocessed():
    print("================================================================")
    print("🛍️  ETL PIPELINE: Ingesting Outstanding Reviews (On-Demand)")
    print("================================================================")
    print(" - Scanning database and executing batch scoring...")
    run_batch_scoring()
    print("✅ SUCCESS: Ingestion complete!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--backfill", action="store_true", help="Run 25-day historical backfill")
    group.add_argument("--ingest", action="store_true", help="Ingest and score pending reviews")
    parser.add_argument("--reset", action="store_true", help="Clear store_reviews, predictions, drift_metrics before backfill")
    
    args = parser.parse_args()
    
    if args.backfill:
        run_backfill(reset=args.reset)
    elif args.ingest:
        ingest_unprocessed()
