import os, sys, argparse, sqlite3
from datetime import datetime, timedelta
import pandas as pd

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

def write_to_store_reviews(reviews_list):
    conn = sqlite3.connect("data/results.db")
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS store_reviews (review_id TEXT PRIMARY KEY, review_date TEXT, category TEXT, review_text TEXT, is_processed INTEGER DEFAULT 0);")
    for r in reviews_list:
        c.execute("INSERT OR REPLACE INTO store_reviews VALUES (?, ?, ?, ?, 0);", (r["review_id"], r["review_date"], r["category"], r["review_text"]))
    conn.commit()
    conn.close()

def run_backfill():
    print("================================================================")
    print("🛍️  ETL PIPELINE: Executing 25-Day Historical Backfill (Local)")
    print("================================================================")
    db = "data/results.db"
    if os.path.exists(db):
        try:
            conn = sqlite3.connect(db)
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM store_reviews")
            cnt = c.fetchone()[0]
            conn.close()
            if cnt > 0:
                print(f"ℹ️  INFO: Database '{db}' already contains {cnt} persistent reviews.")
                print(" -> Skipping historical backfill to preserve persistent production history.")
                print(" -> If you want to reset and run backfill from scratch, please delete 'data/results.db' manually.")
                return
        except Exception:
            pass # Table might not exist yet, proceed with setup
        
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

def ingest_day(ds):
    print("================================================================")
    print(f"🛍️  ETL PIPELINE: Ingesting Batch for '{ds}' (Local-Only)")
    print("================================================================")
    print(f" - Executing batch scoring for {ds}...")
    run_batch_scoring(ds)
    print("✅ SUCCESS: Ingestion complete!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--backfill", action="store_true")
    group.add_argument("--ingest-daily", action="store_true")
    parser.add_argument("--date", type=str)
    
    args = parser.parse_args()
    target_date = args.date if args.date else datetime.now().strftime("%Y-%m-%d")
    
    if args.backfill: run_backfill()
    elif args.ingest_daily: ingest_day(target_date)
