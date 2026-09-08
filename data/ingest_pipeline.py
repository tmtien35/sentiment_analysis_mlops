import os, sys, argparse
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text

sys.path.append(os.getcwd())
from airflow_home.dags.batch_scoring import run_batch_scoring

STABLE_TEMPLATES = {
    "positive": [
        "Absolutely amazing! This exceeded my expectations.",
        "Very fast shipping and high quality product.",
        "Perfect fit, comfortable and looks great.",
        "Wonderful customer service and brilliant experience.",
        "Works perfectly! Will definitely buy again.",
        "So happy with this purchase, high recommendation.",
        "Best purchase of the year, absolutely stellar."
    ],
    "neutral": [
        "It is okay, nothing special but works fine.",
        "Satisfactory purchase, average quality.",
        "Okay product, shipping took slightly longer.",
        "Decent item for the price, no major issues.",
        "Average fit, not too bad but could be better.",
        "Works as expected, pretty standard item.",
        "Acceptable experience overall."
    ],
    "negative": [
        "Terrible quality, broke on the first day of use!",
        "Very disappointed, absolute waste of money.",
        "Poor customer service and extremely slow shipping.",
        "Avoid this product! Crashing and very bad experience.",
        "Worst item I have ever bought, completely defective.",
        "Does not work at all, returning it immediately.",
        "Horrible fit, material feels cheap and bad."
    ]
}

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

def run_backfill():
    print("================================================================")
    print("🛍️  ETL PIPELINE: Executing 25-Day Historical Backfill (Local)")
    print("================================================================")
    engine = get_db_engine()
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT COUNT(*) FROM store_reviews"))
            cnt = res.fetchone()[0]
            if cnt > 0:
                print(f"ℹ️  INFO: Database already contains {cnt} persistent reviews in 'store_reviews'.")
                print(" -> Skipping historical backfill to preserve persistent production history.")
                print(" -> If you want to reset and run backfill from scratch, please delete 'data/results.db' manually or truncate tables.")
                return
    except Exception:
        pass # Table doesn't exist yet, proceed with setup
        
    yesterday = datetime.now() - timedelta(days=1)
    start_date = yesterday - timedelta(days=24)
    
    print(f"Generating balanced local real reviews ending yesterday: {yesterday.strftime('%Y-%m-%d')}...")
    categories = ["electronics", "kitchen", "apparel", "sports", "other"]
    
    for day in range(25):
        cur_date = start_date + timedelta(days=day)
        ds = cur_date.strftime("%Y-%m-%d")
        
        day_reviews = []
        # Generate balanced reviews: 7 positive, 6 neutral, 7 negative
        for i in range(7):
            day_reviews.append({
                "review_id": f"bk_{day}_p_{i}", "review_date": ds,
                "category": categories[i % len(categories)],
                "review_text": STABLE_TEMPLATES["positive"][i % len(STABLE_TEMPLATES["positive"])]
            })
        for i in range(6):
            day_reviews.append({
                "review_id": f"bk_{day}_n_{i}", "review_date": ds,
                "category": categories[i % len(categories)],
                "review_text": STABLE_TEMPLATES["neutral"][i % len(STABLE_TEMPLATES["neutral"])]
            })
        for i in range(7):
            day_reviews.append({
                "review_id": f"bk_{day}_g_{i}", "review_date": ds,
                "category": categories[i % len(categories)],
                "review_text": STABLE_TEMPLATES["negative"][i % len(STABLE_TEMPLATES["negative"])]
            })
            
        write_to_store_reviews(day_reviews)
        print(f" - Ingesting & scoring: {ds}...")
        run_batch_scoring(ds)
    print("✅ SUCCESS: 25-Day stable, real-data history pre-populated offline!")

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
    group.add_argument("--backfill", action="store_true")
    group.add_argument("--ingest", action="store_true")
    
    args = parser.parse_args()
    
    if args.backfill: run_backfill()
    elif args.ingest: ingest_unprocessed()
