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
                is_processed INTEGER DEFAULT 0,
                verified_sentiment TEXT DEFAULT NULL
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
                    INSERT INTO store_reviews (review_id, review_date, category, review_text, is_processed, verified_sentiment)
                    VALUES (:review_id, :review_date, :category, :review_text, 0, NULL)
                """), r)
            inserted = len(reviews_list)
    except Exception as e:
        print(f"❌ Transaction failed: {e}")
    return inserted

def main():
    print("==================================================================")
    print("🚗  Vietnamese EV Reviews: Live Customer Feedback Submitter")
    print("==================================================================")
    
    parser = argparse.ArgumentParser(description="Submit live EV reviews into the database.")
    parser.add_argument("--text", type=str, help="Content of the customer EV review (CLI argument mode)")
    parser.add_argument("--category", type=str, choices=["pin_sac", "van_hanh", "noi_that", "dich_vu", "khac"], default="khac")
    
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
            print(f"✅ Submitted single EV review: {review_id} for {today_str} successfully!")
        return

    # Interactive Continuous Loop Mode
    print("\nNhập đánh giá xe điện liên tục (để trống hoặc gõ 'exit' để gửi):")
    print("-" * 66)
    
    categories = ["pin_sac", "van_hanh", "noi_that", "dich_vu", "khac"]
    pending_list = []
    idx = 1
    
    try:
        while True:
            print(f"\nĐánh giá xe điện #{idx}:")
            text = input("> ").strip()
            if not text.strip() or "exit" in text.lower():
                break
            
            # Automatically assign a distributed category
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
            print("\nĐang lưu vào database...")
            inserted = submit_batch_reviews(pending_list)
            print("\n==================================================================")
            print(f"✅ THÀNH CÔNG: Đã gửi {inserted} đánh giá xe điện!")
            print("==================================================================")
            for r in pending_list:
                print(f" 🚗 [{r['category'].upper()}] \"{r['review_text']}\"")
            print("==================================================================")
            print("👉 Chạy pipeline batch scoring để chấm điểm:")
            print("   python data/ingest_pipeline.py --ingest")
        else:
            print("\nKhông có đánh giá nào được nhập. Đã thoát an toàn.")
            
    except KeyboardInterrupt:
        print("\n\nĐã hủy nhập.")

if __name__ == "__main__":
    main()
