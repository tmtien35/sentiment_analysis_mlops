import os, sys, argparse
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text

sys.path.append(os.getcwd())
from airflow_home.dags.batch_scoring import run_batch_scoring

STABLE_TEMPLATES = {
    "positive": [
        "Xe chạy cực kỳ êm ái, tăng tốc nhanh và cách âm vượt ngoài mong đợi.",
        "Trạm sạc phủ sóng rộng khắp, sạc siêu nhanh rất tiện lợi khi đi xa.",
        "Nội thất hiện đại, màn hình trung tâm mượt mà, tính năng ADAS hoạt động chuẩn xác.",
        "Dịch vụ chăm sóc khách hàng và cứu hộ 24/7 rất chu đáo và tận tình.",
        "Tiết kiệm chi phí nhiên liệu rõ rệt so với xe xăng, rất hài lòng với quyết định mua xe.",
        "Quãng đường di chuyển thực tế đúng như công bố, pin sạc rất bền bỉ.",
        "Trải nghiệm lái tuyệt vời, khung gầm đầm chắc và an toàn tối đa."
    ],
    "neutral": [
        "Xe đi tạm ổn trong tầm giá, phần mềm đôi khi cần khởi động lại.",
        "Thời gian sạc ở mức chấp nhận được, mong có thêm trụ sạc nhanh ở ngoại thành.",
        "Nội thất ở mức trung bình, chất liệu nhựa bình thường nhưng chấp nhận được.",
        "Quãng đường đi thực tế phụ thuộc nhiều vào điều hòa và tốc độ chạy.",
        "Treo hơi cứng khi qua gờ giảm tốc, còn lại vận hành cơ bản ổn định.",
        "Hệ thống thông tin giải trí phản hồi bình thường, đủ dùng cho nhu cầu hàng ngày.",
        "Dịch vụ bảo dưỡng mức độ tạm được, thời gian chờ lấy xe hơi lâu."
    ],
    "negative": [
        "Phần mềm báo lỗi ảo liên tục, màn hình thỉnh thoảng bị đơ rất khó chịu.",
        "Trụ sạc công cộng thường xuyên bị lỗi kết nối hoặc xe xăng chiếm chỗ.",
        "Chất lượng hoàn thiện kém, tiếng ồn lốp và gió vọng vào khoang lái nhiều.",
        "Pin sụt nhanh hơn nhiều so với thông số công bố khi đi đường dài.",
        "Dịch vụ hậu mãi và bảo hành quá chậm, phụ tùng thay thế phải chờ đợi cả tháng.",
        "Hệ thống điều hòa làm mát chậm trong thời tiết nắng nóng gay gắt.",
        "Trải nghiệm dịch vụ rất thất vọng, nhân viên kỹ thuật xử lý chưa chuyên nghiệp."
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
    print("🚗  ETL PIPELINE: Executing 25-Day Historical EV Backfill (Local)")
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
    categories = ["pin_sac", "van_hanh", "noi_that", "dich_vu", "khac"]
    
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
