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
    parser.add_argument("--date", type=str, help="Review date in YYYY-MM-DD format")
    
    args = parser.parse_args()
    
    # Direct argument mode (used by test suites/one-off scripts)
    if args.text:
        date_str = args.date if args.date else datetime.now().strftime("%Y-%m-%d")
        review_id = f"user_{str(uuid.uuid4())[:8]}"
        inserted = submit_batch_reviews([{
            "review_id": review_id,
            "review_date": date_str,
            "category": args.category,
            "review_text": args.text
        }])
        if inserted:
            print(f"✅ Submitted single review: {review_id} for {date_str} successfully!")
        return

    # Interactive Continuous Loop Mode
    print("\n[STEP 1] Set Session Date")
    today_str = datetime.now().strftime("%Y-%m-%d")
    print(f"Enter review date (YYYY-MM-DD) [{today_str}]:")
    date_input = input("> ").strip()
    date_str = date_input if date_input else today_str
    
    # Validate date
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        print("❌ Error: Invalid date format. Use YYYY-MM-DD.")
        return

    print("\n[STEP 2] Enter Reviews Continuously (Leave review empty or type 'exit' to submit)")
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
                "review_date": date_str,
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
            print("👉 Run your daily ingestion pipeline to analyze and score them:")
            print(f"   python data/ingest_pipeline.py --ingest-daily --date {date_str}")
        else:
            print("\nNo reviews entered. Exited cleanly.")
            
    except KeyboardInterrupt:
        print("\n\nSubmission cancelled.")

if __name__ == "__main__":
    main()
