import os
import uuid
import argparse
from datetime import datetime
from sqlalchemy import create_engine, text

def get_db_engine():
    db_url = os.environ.get("DATABASE_URL", "sqlite:///data/results.db")
    return create_engine(db_url)

def initialize_source_table():
    """Ensure the source store_reviews table exists with our is_processed state flag."""
    engine = get_db_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS store_reviews (
                review_id TEXT PRIMARY KEY,
                review_date TEXT,
                category TEXT,
                review_text TEXT,
                is_processed INTEGER DEFAULT 0
            );
        """))

def submit_batch_reviews(reviews_list):
    """Inserts a batch of reviews transactionally."""
    if not reviews_list:
        return 0
    initialize_source_table()
    engine = get_db_engine()
    inserted = 0
    try:
        with engine.begin() as conn:
            for r in reviews_list:
                conn.execute(text("DELETE FROM store_reviews WHERE review_id = :review_id"), {"review_id": r["review_id"]})
                conn.execute(text("""
                    INSERT INTO store_reviews (review_id, review_date, category, review_text, is_processed)
                    VALUES (:review_id, :review_date, :category, :review_text, 0)
                """), r)
            inserted = len(reviews_list)
    except Exception as e:
        print(f"❌ Transaction failed: {e}")
    return inserted

def main():
    print("==================================================================")
    print("🛍️  E-Commerce Storefront: Live Customer Review Submitter Loop")
    print("==================================================================")
    
    parser = argparse.ArgumentParser(description="Submit live reviews into the Storefront database.")
    parser.add_argument("--text", type=str, help="Content of the customer product review (CLI argument mode)")
    parser.add_argument("--category", type=str, choices=["apparel", "kitchen", "electronics", "sports", "other"], default="other")
    
    args = parser.parse_args()
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    # Direct argument mode (used by test suites/one-off scripts)
    if args.text:
        review_id = f"user_{str(uuid.uuid4())[:8]}"
        inserted = submit_batch_reviews([{
            "review_id": review_id,
            "review_date": today_str,
            "category": args.category,
            "review_text": args.text
        }])
        if inserted:
            print(f"✅ Submitted single review: {review_id} for {today_str} successfully!")
        return

    # Interactive Continuous Loop Mode (Completely simplified - No date prompt!)
    print("\nEnter Reviews Continuously (Leave review empty or type 'exit' to submit)")
    print("-" * 66)
    
    categories = ["electronics", "kitchen", "apparel", "sports", "other"]
    pending_list = []
    idx = 1
    
    try:
        while True:
            print(f"\nReview #{idx} Text:")
            text = input("> ").strip()
            if not text or text.lower() == "exit":
                break
            
            # Automatically assign a distributed category behind the scenes!
            category = categories[(idx - 1) % len(categories)]
            
            review_id = f"user_{str(uuid.uuid4())[:8]}"
            pending_list.append({
                "review_id": review_id,
                "review_date": today_str,
                "category": category,
                "review_text": text
            })
            idx += 1
            
        if pending_list:
            print("\nSubmitting to database...")
            inserted = submit_batch_reviews(pending_list)
            print("\n==================================================================")
            print(f"✅ SUCCESS: {inserted} E-Commerce Review(s) Submitted!")
            print("==================================================================")
            for r in pending_list:
                print(f" 📦 [{r['category'].upper()}] \"{r['review_text']}\"")
            print("==================================================================")
            print("👉 Run your ingestion pipeline to analyze and score them:")
            print("   python data/ingest_pipeline.py --ingest")
        else:
            print("\nNo reviews entered. Exited cleanly.")
            
    except KeyboardInterrupt:
        print("\n\nSubmission cancelled.")

if __name__ == "__main__":
    main()
