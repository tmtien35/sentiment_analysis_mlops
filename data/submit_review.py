import os
import sqlite3
import uuid
import argparse
from datetime import datetime

def get_db_connection():
    db_path = os.path.join("data", "results.db")
    os.makedirs("data", exist_ok=True)
    return sqlite3.connect(db_path)

def initialize_source_table():
    """Ensure the source store_reviews table exists with our is_processed state flag."""
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS store_reviews (
            review_id TEXT PRIMARY KEY,
            review_date TEXT,
            category TEXT,
            review_text TEXT,
            is_processed INTEGER DEFAULT 0
        );
    """)
    conn.commit()
    conn.close()

def submit_batch_reviews(reviews_list):
    """Inserts a batch of reviews transactionally."""
    if not reviews_list:
        return 0
    initialize_source_table()
    conn = get_db_connection()
    c = conn.cursor()
    inserted = 0
    try:
        for r in reviews_list:
            c.execute("""
                INSERT OR REPLACE INTO store_reviews (review_id, review_date, category, review_text, is_processed)
                VALUES (?, ?, ?, ?, 0);
            """, (r["review_id"], r["review_date"], r["category"], r["review_text"]))
        conn.commit()
        inserted = len(reviews_list)
    except Exception as e:
        print(f"❌ Transaction failed: {e}")
    finally:
        conn.close()
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


