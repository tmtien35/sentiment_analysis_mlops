import os
import sys
import argparse
from datetime import datetime, timedelta
from sqlalchemy import text

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from airflow_home.dags.batch_scoring import get_db_engine, run_batch_scoring
from data.submit_review import submit_batch_reviews

# Curated pools of severe negative customer reviews across different EV categories
# Guaranteed to trigger vocabulary drift (PSI >= 0.15) against the training baseline
DRIFT_REVIEWS_DAY1 = [
    ("pin_sac", "Pin tụt quá nhanh khi chạy cao tốc, xe báo lỗi hệ thống điện liên tục!"),
    ("van_hanh", "Màn hình trung tâm bị đơ đen ngòm khi đang lái xe, cực kỳ nguy hiểm."),
    ("pin_sac", "Trụ sạc nhanh của hãng toàn bị lỗi quá nhiệt, cắm vào ngắt ngay lập tức."),
    ("dich_vu", "Dịch vụ cứu hộ quá chậm chạp, gọi tổng đài 3 tiếng giữa trưa không ai đến."),
    ("noi_that", "Chất lượng hoàn thiện kém, gioăng cửa hở làm tiếng ồn gió rít rất khó chịu."),
    ("van_hanh", "Hệ thống điều hòa tự ngắt giữa trời nắng gắt, xe báo quá nhiệt động cơ."),
    ("van_hanh", "Phần mềm cập nhật xong bị lỗi phanh tái sinh giật cục rất khó chịu."),
    ("dich_vu", "Đại lý trễ hẹn giao xe 2 tháng mà không có một lời giải thích thỏa đáng."),
    ("pin_sac", "Pin báo ảo nghiêm trọng, đang 40% tụt một phát xuống 8% chỉ sau 4km."),
    ("dich_vu", "Chính sách bảo hành mập mờ, nhân viên kỹ thuật từ chối bảo hành cell pin chai.")
]

DRIFT_REVIEWS_DAY2 = [
    ("pin_sac", "Lỗi sạc pin tiếp diễn nghiêm trọng, trạm sạc không nhận thẻ và cổng sạc kẹt cứng."),
    ("van_hanh", "Xe bị mất trợ lực lái giữa đường đông, hú còi báo động liên hồi không tắt được."),
    ("van_hanh", "Hệ thống camera 360 bị nhấp nháy liên tục rồi tắt hẳn, đại lý báo hết linh kiện thay."),
    ("khac", "App điện thoại mất kết nối hoàn toàn với xe, không mở được khóa thông minh."),
    ("noi_that", "Chất lượng sơn xe quá tệ, mới đi 2 tháng đã bong tróc nhiều chỗ ở cản trước."),
    ("van_hanh", "Tiếng kêu lộc cộc phát ra từ gầm xe khi vào cua, kiểm tra báo lỗi rô-tuyn."),
    ("dich_vu", "Tổng đài chăm sóc khách hàng đùn đẩy trách nhiệm khi xe gặp lỗi pin."),
    ("van_hanh", "Bảng đồng hồ taplo báo lỗi ảo hệ thống phanh ABS, phải gọi xe cứu hộ kéo về."),
    ("pin_sac", "Sạc pin qua đêm nhưng dung lượng chỉ tăng 10%, bộ sạc gia đình bị lỗi nguồn."),
    ("van_hanh", "Xe bị tuột dốc khi dừng đèn đỏ ở dốc cầu, tính năng giữ phanh Auto-Hold bị lỗi.")
]


def clean_date_data(engine, target_date: str):
    """Clean simulated records for a specific date to allow safe idempotent re-runs."""
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM store_reviews WHERE review_date = :ds AND review_id LIKE 'SIM_DRIFT_%'"),
            {"ds": target_date}
        )
        conn.execute(
            text("DELETE FROM predictions WHERE review_date = :ds AND review_id LIKE 'SIM_DRIFT_%'"),
            {"ds": target_date}
        )
        conn.execute(
            text("DELETE FROM drift_metrics WHERE batch_date = :ds"),
            {"ds": target_date}
        )

def clean_all_simulation():
    """Wipe all simulated drift reviews, predictions, and alerts across all dates."""
    engine = get_db_engine()
    print("\n🧹 [CLEANUP] Cleaning all simulated drift data...")
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT DISTINCT review_date FROM store_reviews WHERE review_id LIKE 'SIM_DRIFT_%'"))
            sim_dates = [r[0] for r in res.fetchall() if r[0]]
            
            res_preds = conn.execute(text("SELECT DISTINCT review_date FROM predictions WHERE review_id LIKE 'SIM_DRIFT_%'"))
            for r in res_preds.fetchall():
                if r[0] and r[0] not in sim_dates:
                    sim_dates.append(r[0])

        with engine.begin() as conn:
            conn.execute(text("DELETE FROM store_reviews WHERE review_id LIKE 'SIM_DRIFT_%'"))
            conn.execute(text("DELETE FROM predictions WHERE review_id LIKE 'SIM_DRIFT_%'"))
            for d in sim_dates:
                conn.execute(text("DELETE FROM drift_metrics WHERE batch_date = :ds"), {"ds": d})

        alerts_dir = os.path.join(PROJECT_ROOT, "data", "alerts")
        if os.path.exists(alerts_dir):
            for d in sim_dates:
                fpath = os.path.join(alerts_dir, f"drift_alert_{d.replace('-', '_')}.html")
                if os.path.exists(fpath):
                    try:
                        os.remove(fpath)
                    except Exception:
                        pass

        print(f"✅ Successfully wiped simulated drift data for dates: {sim_dates if sim_dates else 'None found'}")
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")


def simulate_single_day(target_date: str, pool_idx: int = 1, auto_retrain: bool = True):
    """
    Simulates a severe negative review wave on a specific date, then triggers batch scoring.
    """
    engine = get_db_engine()
    
    clean_date_data(engine, target_date)

    pool = DRIFT_REVIEWS_DAY1 if (pool_idx % 2 == 1) else DRIFT_REVIEWS_DAY2
    
    reviews = []
    clean_date_tag = target_date.replace("-", "")
    for i, (cat, text_content) in enumerate(pool, 1):
        reviews.append({
            "review_id": f"SIM_DRIFT_{clean_date_tag}_{i:02d}",
            "review_date": target_date,
            "category": cat,
            "review_text": text_content
        })

    print(f"\n==================================================================")
    print(f"⚡ SIMULATING DRIFT BATCH FOR DATE: {target_date} (Pool #{pool_idx})")
    print(f"==================================================================")
    print(f"📥 Injecting {len(reviews)} severe negative EV customer reviews...")
    inserted = submit_batch_reviews(reviews)
    print(f"✅ Successfully inserted {inserted} reviews into 'store_reviews' for {target_date}.")

    print(f"\n🚀 Running batch scoring & drift monitoring for {target_date}...")
    run_batch_scoring(ds=target_date, auto_retrain=auto_retrain)



def main():
    parser = argparse.ArgumentParser(
        description="⚡ Fast Drift & Automated Retraining Simulator for Demos & Video Recording.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ví Dụ Sử Dụng (Usage Examples):
----------------------------------------------------------------------
1. Chạy 1 ngày cụ thể (Day 1 - Cảnh báo trôi dạt & Hoãn retrain):
   python data/simulate_drift.py --date 2026-09-15

2. Chạy ngày tiếp theo (Day 2 - Xác nhận Persistent Drift & Tự Retrain):
   python data/simulate_drift.py --date 2026-09-16

3. Tự động chạy cả chuỗi 2 ngày liên tiếp (Tiện nhất khi quay video):
   python data/simulate_drift.py --start-date 2026-09-15 --days 2

4. Dọn dẹp dữ liệu mô phỏng sau khi quay video xong:
   python data/simulate_drift.py --clean
----------------------------------------------------------------------
        """
    )
    parser.add_argument("--date", type=str, help="Chỉ định ngày cụ thể muốn giả lập (YYYY-MM-DD)")
    parser.add_argument("--start-date", type=str, help="Ngày bắt đầu chuỗi mô phỏng liên tiếp (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=2, help="Số ngày trôi dạt liên tiếp cần mô phỏng (Mặc định: 2)")
    parser.add_argument("--clean", action="store_true", help="Xóa sạch toàn bộ dữ liệu mô phỏng (Reset)")

    args = parser.parse_args()

    if args.clean:
        clean_all_simulation()
        return

    if args.date:
        try:
            datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print(f"❌ Định dạng ngày không hợp lệ: '{args.date}'. Vui lòng dùng YYYY-MM-DD (Ví dụ: 2026-09-15).")
            return
        
        simulate_single_day(target_date=args.date, pool_idx=1, auto_retrain=True)
        return

    if args.start_date:
        try:
            start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
        except ValueError:
            print(f"❌ Định dạng ngày không hợp lệ: '{args.start_date}'. Vui lòng dùng YYYY-MM-DD.")
            return
    else:
        start_dt = datetime.now() + timedelta(days=1)
        print(f"ℹ️  Không truyền tham số ngày. Mặc định chạy mô phỏng 2 ngày bắt đầu từ: {start_dt.strftime('%Y-%m-%d')}")
        print("    (Dùng `python data/simulate_drift.py --help` để xem hướng dẫn tùy biến ngày)")

    days_count = max(1, args.days)
    print(f"\n🎬 BẮT ĐẦU MÔ PHỎNG CHUỖI {days_count} NGÀY TRÔI DẠT (PERSISTENT DRIFT)")
    print(f"   Mốc bắt đầu: {start_dt.strftime('%Y-%m-%d')} | Số mẻ: {days_count}")

    for day_offset in range(days_count):
        current_dt = start_dt + timedelta(days=day_offset)
        current_date_str = current_dt.strftime("%Y-%m-%d")
        pool_num = day_offset + 1
        simulate_single_day(target_date=current_date_str, pool_idx=pool_num, auto_retrain=True)

    print("\n==================================================================")
    print("🎉 HOÀN TẤT CHU TRÌNH MÔ PHỎNG PERSISTENT DRIFT & AUTO-RETRAINING!")
    print("==================================================================")
    print("👉 Hãy kiểm tra kết quả trên:")
    print("   1. Streamlit Dashboard (http://localhost:8501): Biểu đồ PSI Drift & Human Approval Gatekeeper")
    print("   2. MLflow Registry (http://localhost:5000): Phiên bản mô hình mới được đăng ký")
    print("   3. Dọn dẹp sau khi quay: python data/simulate_drift.py --clean")
    print("==================================================================\n")


if __name__ == "__main__":
    main()

