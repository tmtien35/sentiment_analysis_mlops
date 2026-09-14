import os, sys
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

def get_db_engine():
    db_url = os.environ.get("DATABASE_URL", "sqlite:///data/results.db")
    return create_engine(db_url)

def classify_aspect_category(text: str) -> str:
    """Classifies an EV review text into domain aspect categories."""
    t = text.lower()
    if any(k in t for k in ["pin", "sạc", "trụ", "tầm hoạt động", "quãng đường", "km", "tiêu hao"]):
        return "pin_sac"
    elif any(k in t for k in ["lái", "vận hành", "êm", "tăng tốc", "khung gầm", "treo", "động cơ", "phanh"]):
        return "van_hanh"
    elif any(k in t for k in ["nội thất", "màn hình", "ghế", "khoang", "chất liệu", "nhựa", "loa", "phần mềm", "lỗi ảo"]):
        return "noi_that"
    elif any(k in t for k in ["dịch vụ", "đại lý", "bảo hành", "bảo dưỡng", "cứu hộ", "nhân viên", "tư vấn"]):
        return "dich_vu"
    return "khac"

def crawl_daily_reviews(ds: str = None, n_reviews: int = 20):
    """
    Simulates scraping daily EV reviews from online channels without repetition.
    Picks n_reviews fresh samples from data/ev_feed_simulation_pool.csv that have
    not yet been ingested into store_reviews, assigning execution date ds (or today).
    """
    if ds is None:
        ds = datetime.now().strftime("%Y-%m-%d")
        
    print("================================================================")
    print(f"🌐  EV SCRAPER BOT (Simulated): Ingesting Daily Reviews for {ds}")
    print("================================================================")
    
    engine = get_db_engine()
    
    # 1. Ensure table exists
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
    
    # 2. Count existing reviews for this target date to determine continuous index offset
    clean_date_str = ds.replace("-", "")
    with engine.connect() as conn:
        res_day_count = conn.execute(text("SELECT COUNT(*) FROM store_reviews WHERE review_date = :ds"), {"ds": ds})
        existing_day_count = res_day_count.fetchone()[0]

    print(f"ℹ️  Scraper: Date '{ds}' currently has {existing_day_count} existing review(s). Ingesting next {n_reviews} fresh reviews...")

    # 3. Load simulation pool
    pool_path = os.environ.get("FEED_POOL_PATH", "data/ev_feed_simulation_pool.csv")
    if not os.path.exists(pool_path):
        print(f"⚠️ Scraper Error: Simulation pool not found at '{pool_path}'!")
        return ds
        
    df_pool = pd.read_csv(pool_path)
    
    # 4. Filter out any reviews that have already been crawled previously (Non-repetitive guarantee)
    with engine.connect() as conn:
        res_texts = conn.execute(text("SELECT review_text FROM store_reviews"))
        existing_texts = set(r[0] for r in res_texts.fetchall() if r[0])
        
    available_df = df_pool[~df_pool['text'].isin(existing_texts)]
    print(f" - Pool status: {len(available_df)} unused reviews remaining out of {len(df_pool)} total.")
    
    if len(available_df) == 0:
        print("⚠️ All reviews in pool have been exhausted. Recycling pool...")
        available_df = df_pool
        
    # 5. Randomly sample n_reviews
    sample_size = min(n_reviews, len(available_df))
    sampled_df = available_df.sample(n=sample_size, random_state=None)
    
    # 6. Format and insert into store_reviews with unique indexed IDs
    new_records = []
    for i, (_, row) in enumerate(sampled_df.iterrows(), 1):
        idx = existing_day_count + i
        rev_id = f"FEED_{clean_date_str}_{idx:03d}"
        category = classify_aspect_category(str(row["text"]))
        new_records.append({
            "review_id": rev_id,
            "review_date": ds,
            "category": category,
            "review_text": str(row["text"]).strip(),
            "is_processed": 0,
            "verified_sentiment": None
        })
        
    with engine.begin() as conn:
        for r in new_records:
            conn.execute(text("""
                INSERT INTO store_reviews (review_id, review_date, category, review_text, is_processed, verified_sentiment)
                VALUES (:review_id, :review_date, :category, :review_text, :is_processed, :verified_sentiment)
            """), r)
            
    print(f"✅ SUCCESS: Ingested {len(new_records)} fresh, unique EV reviews for date '{ds}' into 'store_reviews'!")
    return ds

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Simulate daily EV review scraper")
    parser.add_argument("--date", type=str, default=None, help="Target date YYYY-MM-DD (defaults to today)")
    parser.add_argument("--count", type=int, default=20, help="Number of reviews to sample (default: 20)")
    args = parser.parse_args()
    crawl_daily_reviews(ds=args.date, n_reviews=args.count)
