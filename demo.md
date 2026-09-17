# 🎬 Hướng Dẫn Trình Diễn & Quay Video Demo MLOps (3 Kịch Bản Tinh Gọn)

> 💡 **Mục đích**: Tài liệu này tập trung vào **3 kịch bản trình diễn cốt lõi** và các **câu lệnh thao tác hữu ích** để quay video báo cáo hoặc thuyết trình trực tiếp với Hội đồng.  
> *(Các bước cài đặt môi trường, dựng Docker từ đầu đã được mô tả chi tiết tại `README.md`).*

---


## ⚡ QUICK DEMO: 3 Kịch Bản "Chuẩn MLOps" Cho Buổi Thuyết Trình & Video Ngắn (3 - 4 Phút)

> 💡 **Dành cho thuyết trình cấp tốc, chấm điểm đồ án, hoặc quay video ngắn (TikTok / Reels / Elevator Pitch)**:  
> Không cần kỹ thuật rườm rà, bạn chỉ cần mở sẵn 3 tab trình duyệt: **Airflow (`:8080`)**, **MLflow (`:5000`)**, và **Streamlit Dashboard (`:8501`)** để diễn trọn vẹn 3 kịch bản trọng tâm dưới đây:

### ⏱️ Bảng Cheat Sheet "Bỏ Túi" (3 Phút Lên Sóng)

| Thời Lượng | Kịch Bản & Trọng Tâm | Thao Tác Trực Tiếp Trên Web (1 - 2 Chạm) | Hiệu Ứng Trực Quan Tức Thì | Câu Chốt "Gây Ấn Tượng" (Punchline) |
| :---: | :--- | :--- | :--- | :--- |
| **0:00 - 1:00**<br/>*(60s)* | **1. Airflow Orchestration & Automated Batch Scoring** | Mở **Airflow (`:8080`)** bấm Trigger DAG `daily_sentiment_analysis`. | 1. **Airflow**: 2 Task xanh lá.<br/>2. **MLflow (`:5000`)**: Ghi nhận Run mới.<br/>3. **Streamlit (`:8501`)**: Batch mới hiện trên bảng & biểu đồ. | *"Hệ thống tự động cào dữ liệu và chấm điểm mẻ theo chu kỳ, lưu vết minh bạch 100% trên MLflow và cập nhật tức thì lên Dashboard."* |
| **1:00 - 2:00**<br/>*(60s)* | **2. Persistent Drift (2 Ngày) & Non-Disruptive Auto-Retrain** | Mô phỏng 2 ngày drift liên tiếp (PSI $\ge$ 0.15) kích hoạt tự động retrain ngầm. | 1. **MLflow (`:5000`)**: Tạo Version mới mang nhãn `@candidate`.<br/>2. **Streamlit (`:8501`)**: Hiện trong **Gatekeeper**, giữ nguyên `@champion` chờ Engineer duyệt. | *"Khi khủng hoảng kéo dài 2 ngày, hệ thống tự học lại nhưng KHÔNG tự ý thay thế mô hình đang chạy mà dừng chờ kỹ sư phê duyệt."* |
| **2:00 - 3:00**<br/>*(60s)* | **3. Active Learning Audit & Manual Retrain** | Trên Streamlit, sửa/duyệt reviews drift hoặc có độ tự tin thấp (`< 0.60`) rồi bấm **Trigger Retrain**. | 1. **MLflow (`:5000`)**: Sinh Version mới từ nhãn con người.<br/>2. **Streamlit (`:8501`)**: Xuất hiện trên bảng chờ duyệt, không đè mô hình cũ. | *"Chuyên viên chỉ cần sửa các ca khó nhất có độ tự tin thấp, mô hình tự học lại từ nhãn chuẩn và chỉ thăng hạng khi kỹ sư bấm duyệt."* |


---

### 🎬 Chi Tiết 3 Kịch Bản Trọng Tâm (Kèm Lệnh Thao Tác Trực Tiếp & Giả Lập)

---

#### 🌟 KỊCH BẢN 1: Điều Phối Tự Động Định Kỳ & Chấm Điểm Mẻ (Batch Scoring & Automated Orchestration) (~1 phút)
*   **Mục tiêu chứng minh:** Hệ thống vận hành tự động định kỳ theo chu kỳ (Scheduled Pipeline), tự động cào 20 đánh giá xe điện mới và chấm điểm mẻ theo kiến trúc Microservices độc lập, ghi vết minh bạch 100% trên MLflow và hiển thị trực quan trên Streamlit Dashboard.
*   **Các bước thực hiện:**
    1. **Kích hoạt quy trình điều phối & chấm điểm mẻ (Automated Crawl & Batch Scoring):**
       * *Cách 1 (Giao diện Airflow UI):* Mở **Airflow (`http://localhost:8080`)**, bấm nút **Play** (▶️ `Trigger DAG`) trên DAG `daily_sentiment_analysis`. Quan sát 2 task `crawl_daily_ev_reviews` (tự cào 20 đánh giá mới vào `store_reviews`) và `batch_scoring_and_drift_monitoring` (chấm điểm mẻ & tính PSI) chuyển sang màu xanh lá.
       * *Cách 2 (Dòng lệnh CLI):*
         - *Local:* `python data/ingest_pipeline.py --ingest`
         - *Docker:* `docker compose exec fastapi python data/ingest_pipeline.py --ingest`
    2. **Kiểm tra kết quả trực quan tức thì:**
       * **Streamlit Dashboard (`:8501`)**: Nhấn phím `R` để tải lại. Trạng thái hiển thị xanh **`✅ STABLE`** (PSI < 0.15), 20 đánh giá mới vừa được cào và dự đoán xuất hiện ngay trong bảng **Scored Reviews Explorer**.
       * **MLflow Tracking (`:5000`)**: Ghi nhận một **Run mới** trong Experiment `ev-sentiment-batch-scoring` với đầy đủ số lượng bản ghi và độ trễ suy luận.
*   **Câu nói dẫn dắt:**
    > *"Hàng ngày theo lịch định kỳ, Airflow tự động cào 20 đánh giá mới và kích hoạt chấm điểm mẻ. Kết quả được lưu vết minh bạch 100% trên MLflow và trực quan hóa tức thì trên Dashboard cho ban vận hành theo dõi mà không cần can thiệp thủ công."*

---

#### 🌟 KỊCH BẢN 2: Phát Hiện Trôi Dạt Dữ Liệu & Tự Động Phục Hồi (Persistent Drift & Non-Disruptive Retrain) (~1 phút)
*   **Mục tiêu chứng minh:** Khả năng tự phục hồi (Self-Healing) thông minh: Khi xảy ra khủng hoảng truyền thông (lỗi pin, hỏng trạm sạc) dẫn đến trôi dạt phân phối ($\text{PSI} \ge 0.15$), hệ thống lọc nhiễu chuẩn mực: **Ngày 1 chỉ cảnh báo & hoãn retrain** $\rightarrow$ **Ngày 2 xác nhận trôi dạt liên tiếp $\ge 2$ ngày $\rightarrow$ Tự động retrain ngầm & đăng ký `@candidate`**. Tuân thủ nghiêm ngặt nguyên tắc quản trị: **Mô hình mới KHÔNG tự ý cướp quyền `@champion`**.
*   **Lệnh giả lập trôi dạt dữ liệu (Drift Simulation Commands):**
    *   **Tùy chọn A — Chạy trọn gói chuỗi 2 ngày (Nhanh nhất khi quay video & demo cấp tốc):**
        * *Local:*
          ```bash
          python data/simulate_drift.py --start-date 2026-09-25 --days 2
          ```
        * *Docker:*
          ```bash
          docker compose exec fastapi python data/simulate_drift.py --start-date 2026-09-25 --days 2
          ```
    *   **Tùy chọn B — Chạy từng ngày để dừng lại thuyết minh chi tiết:**
        * *Ngày 1 (Isolated Drift Spike — Cảnh báo, hoãn retrain):*
          - *Local:* `python data/simulate_drift.py --date 2026-09-25`
          - *Docker:* `docker compose exec fastapi python data/simulate_drift.py --date 2026-09-25`
          - *Hiện tượng:* PSI vọt lên $\ge 0.15$, Streamlit báo `🚨 DRIFT DETECTED`, tạo HTML cảnh báo tại `data/alerts/drift_alert_2026_09_25.html`, hệ thống hoãn retrain để tránh lãng phí tài nguyên.
        * *Ngày 2 (Persistent Drift Confirmed — Tự retrain ngầm):*
          - *Local:* `python data/simulate_drift.py --date 2026-09-26`
          - *Docker:* `docker compose exec fastapi python data/simulate_drift.py --date 2026-09-26`
          - *Hiện tượng:* Hệ thống xác nhận trôi dạt liên tiếp $\ge 2$ ngày, tự chạy `ml/train_model.py`, vượt qua Gatekeeper, MLflow đăng ký Version mới với nhãn `@candidate`.
    *   **Dọn dẹp sạch dữ liệu mô phỏng sau khi quay video:**
        * *Local:* `python data/simulate_drift.py --clean`
        * *Docker:* `docker compose exec fastapi python data/simulate_drift.py --clean`
*   **Kiểm tra kết quả trực quan:**
    1. **Streamlit Dashboard (`:8501`)**: Thấy cảnh báo `🚨 DRIFT DETECTED` trên biểu đồ PSI. Cuộn xuống khu vực **🛡️ Human Approval Gatekeeper**: Thấy mô hình mới xuất hiện trong bảng so sánh hiệu năng ở trạng thái: **`⏳ Awaiting Approval`** cùng nút bấm **`Approve to Champion`**.
    2. **MLflow Tracking UI (`:5000`)**: Mở mục Models `ev-sentiment-model`: Thấy một **Version mới** vừa được tạo tự động, mang alias `@candidate` (Ứng viên) chứ **KHÔNG** cướp nhãn `@champion` đang phục vụ.
*   **Câu nói dẫn dắt:**
    > *"Khi thị trường xuất hiện khủng hoảng kéo dài 2 ngày liên tiếp, hệ thống tự động phát hiện và huấn luyện mô hình mới trên MLflow. Tuy nhiên, theo chuẩn an toàn doanh nghiệp, mô hình mới không được tự ý đè mô hình đang phục vụ mà dừng ở trạng thái chờ kỹ sư phê duyệt trên Dashboard."*

---

#### 🌟 KỊCH BẢN 3: Con Người Thẩm Định & Quản Trị Mô Hình An Toàn (Active Learning & Gatekeeper Governance) (~1 phút)
*   **Mục tiêu chứng minh:** Vòng lặp phản hồi của con người (Human-in-the-Loop): Chuyên viên sửa nhãn cho các ca khó (Uncertainty Sampling: `confidence < 0.60`) để làm giàu dữ liệu huấn luyện, sau đó kích hoạt Retrain thủ công và kiểm soát thăng hạng an toàn qua Gatekeeper.
*   **Các bước thực hiện:**
    1. **Thẩm định nhãn khó trên Streamlit Dashboard (`:8501`)**:
       * Cuộn xuống bảng **🧠 Active Learning & Human-in-the-Loop Audit**.
       * Giải thích cơ chế **Uncertainty Sampling**: Danh sách tự động ưu tiên lọc các đánh giá trong ngày bị drift hoặc những câu mà AI phân vân nhất (**độ tự tin < 0.60**).
       * Thao tác: Sửa 1 nhãn trực tiếp trên bảng hoặc bấm **`✅ Bulk Approve Remaining AI Predictions`** $\rightarrow$ Bấm **`💾 Save Verified Labels to Data Train`**.
    2. **Kích hoạt tái huấn luyện thủ công:**
       * Tại thanh bên (Sidebar), bấm nút **`Trigger Retrain Manual`**: Hệ thống gom các nhãn chuẩn con người vừa duyệt trong PostgreSQL để huấn luyện lại mô hình mới trong tích tắc (< 0.1 giây).
    3. **Kiểm tra kết quả & Phê duyệt thăng hạng (Gatekeeper):**
       * Mở **MLflow (`:5000`)**: Một **Version mới kế tiếp** được ghi nhận với nguồn dữ liệu từ Human Audit.
       * Mở **Streamlit (`:8501`)**: Version mới này xuất hiện tại bảng **Human Approval Gatekeeper** để kỹ sư so sánh đối đầu trước khi quyết định bấm **`Approve to Champion`**.
*   **Câu nói dẫn dắt:**
    > *"Thay vì tốn kém gán nhãn hàng vạn mẫu, chuyên viên chỉ cần sửa các ca AI lúng túng nhất. Mô hình tự học lại ngay từ nhãn con người, được ghi nhận trên MLflow và chỉ được thăng hạng khi kỹ sư kiểm tra thấy hiệu năng thực sự cải thiện."*

---

## 🛠️ Tổng Hợp Các Câu Lệnh Hữu Ích (Useful Commands Cheat Sheet)

### **1. Lệnh Giả Lập Trôi Dạt Dữ Liệu (Drift Simulation):**
| Thao Tác Giả Lập | Lệnh Chạy Cục Bộ (Local Python) | Lệnh Chạy Trong Container (Docker / GCP VM) |
| :--- | :--- | :--- |
| **Chạy chuỗi 2 ngày (Kích hoạt Retrain ngầm)** | `python data/simulate_drift.py --start-date 2026-09-25 --days 2` | `docker compose exec fastapi python data/simulate_drift.py --start-date 2026-09-25 --days 2` |
| **Chạy Ngày 1 (Cảnh báo trôi dạt, hoãn retrain)** | `python data/simulate_drift.py --date 2026-09-25` | `docker compose exec fastapi python data/simulate_drift.py --date 2026-09-25` |
| **Chạy Ngày 2 (Xác nhận trôi dạt liên tiếp -> Retrain)** | `python data/simulate_drift.py --date 2026-09-26` | `docker compose exec fastapi python data/simulate_drift.py --date 2026-09-26` |
| **Dọn dẹp sạch toàn bộ dữ liệu mô phỏng** | `python data/simulate_drift.py --clean` | `docker compose exec fastapi python data/simulate_drift.py --clean` |

---

### **2. Lệnh Kích Hoạt Chấm Điểm Mẻ & Nhập Đánh Giá Tùy Chọn:**
| Thao Tác Nghiệp Vụ | Lệnh Chạy Cục Bộ (Local Python) | Lệnh Chạy Trong Container (Docker / GCP VM) |
| :--- | :--- | :--- |
| **Kích hoạt cào 20 reviews & chấm điểm mẻ ngay** | `python data/ingest_pipeline.py --ingest` | `docker compose exec fastapi python data/ingest_pipeline.py --ingest` |
| *(Tùy chọn) Nhập tay đánh giá cụ thể qua CLI* | `python data/submit_review.py` | `docker compose exec -it fastapi python data/submit_review.py` |

---

### **3. Lệnh Truy Vấn Nhanh Cơ Sở Dữ Liệu PostgreSQL (Docker Cheat Sheet):**
Chạy trực tiếp các câu lệnh này trên terminal Docker/GCP VM để kiểm tra cơ sở dữ liệu `results_db`:

*   **3.1. Xem tổng quan số lượng bản ghi của toàn bộ 4 bảng:**
    ```bash
    docker compose exec postgres psql -U mlops -d results_db -c "SELECT 'store_reviews' as table_name, count(*) FROM store_reviews UNION ALL SELECT 'predictions', count(*) FROM predictions UNION ALL SELECT 'drift_metrics', count(*) FROM drift_metrics UNION ALL SELECT 'inference_logs', count(*) FROM inference_logs;"
    ```

*   **3.2. Xem các đánh giá chưa được xử lý (`is_processed = 0`):**
    ```bash
    docker compose exec postgres psql -U mlops -d results_db -c "SELECT review_id, review_date, LEFT(review_text, 40) as review_text, is_processed FROM store_reviews WHERE is_processed = 0 ORDER BY review_date DESC, review_id ASC;"
    ```

*   **3.3. Xem 10 kết quả dự đoán gần nhất trong bảng predictions:**
    ```bash
    docker compose exec postgres psql -U mlops -d results_db -c "SELECT review_id, review_date, predicted_sentiment, confidence_score FROM predictions ORDER BY review_date DESC, review_id ASC LIMIT 10;"
    ```

*   **3.4. Xem lịch sử 5 mẻ đo lường trôi dạt dữ liệu (PSI Scores):**
    ```bash
    docker compose exec postgres psql -U mlops -d results_db -c "SELECT batch_date, psi_score, drift_detected FROM drift_metrics ORDER BY batch_date DESC LIMIT 5;"
    ```

*   **3.5. Xem các đánh giá đã được con người duyệt nhãn vàng (Active Learning Ground-Truth):**
    ```bash
    docker compose exec postgres psql -U mlops -d results_db -c "SELECT review_id, review_date, category, LEFT(review_text, 40) as review_text, verified_sentiment FROM store_reviews WHERE verified_sentiment IS NOT NULL ORDER BY review_date DESC;"
    ```

*   **3.6. Đổi nhanh nhãn dự đoán của một dòng để mô phỏng Human Audit:**
    ```bash
    docker compose exec postgres psql -U mlops -d results_db -c "UPDATE store_reviews SET verified_sentiment = 'negative' WHERE review_id = 'user_6ce8dcc9';"
    ```

