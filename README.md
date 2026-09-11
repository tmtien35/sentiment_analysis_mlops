# 🚗 Vietnamese EV Review Sentiment & Self-Healing MLOps Pipeline
*(Hệ Thống Phân Tích Cảm Xúc Đánh Giá Xe Điện & Giám Sát Tự Phục Hồi)*

An end-to-end, reproducible, production-grade MLOps pipeline for classifying Vietnamese electric vehicle (EV) customer reviews, monitoring real-time data drift (PSI), and executing automated self-healing model retraining with Gatekeeper validation.

---

## 🔄 Quy Trình Vận Hành Chi Tiết (Full End-to-End MLOps Flow & Gatekeeper Logic)

Dưới đây là sơ đồ luồng vận hành khép kín từ lúc Airflow kích hoạt lúc nửa đêm, cào dữ liệu, chấm điểm, phát hiện Data Drift, tự phục hồi huấn luyện lại (Self-Healing Retraining), cho đến 2 kịch bản phân nhánh tại chốt chặn an toàn **Gatekeeper**:

```
                       ┌────────────────────────────────────────┐
                       │  Airflow Scheduler (00:00 / @daily)    │
                       └───────────────────┬────────────────────┘
                                           │
                                           ▼
                       ┌────────────────────────────────────────┐
                       │ Task 1: crawl_daily_ev_reviews         │
                       │ - Lấy 20 review mới từ pool            │
                       │ - Phân loại aspect (pin, dịch vụ...)   │
                       │ - Lưu store_reviews (is_processed = 0) │
                       └───────────────────┬────────────────────┘
                                           │
                                           ▼
                       ┌────────────────────────────────────────┐
                       │ Task 2: batch_scoring_and_drift        │
                       │ - Load @champion từ MLflow             │
                       │ - Dự đoán sentiment & confidence       │
                       │ - Lưu vào bảng predictions             │
                       │ - Tính chỉ số PSI so với Baseline      │
                       └───────────────────┬────────────────────┘
                                           │
                       ┌───────────────────┴────────────────────┐
                       │                PSI > 0.2?              │
                       └─────────┬────────────────────┬─────────┘
                      Không (No) │                    │ Có (Yes - Drift!)
                                 │                    ▼
                                 │       ┌──────────────────────────────┐
                                 │       │ 🚨 Trigger Self-Healing:     │
                                 │       │ python ml/train_model.py     │
                                 │       └────────────┬─────────────────┘
                                 │                    │
                                 │                    ▼
                                 │       ┌──────────────────────────────┐
                                 │       │ Gộp Active Learning & Drift  │
                                 │       │ Train Candidate Model        │
                                 │       │ Test trên tập chuẩn val_df   │
                                 │       └────────────┬─────────────────┘
                                 │                    │
                                 │                    ▼
                                 │       ┌──────────────────────────────┐
                                 │       │ 🛡️ GATEKEEPER SO SÁNH:       │
                                 │       │ Macro-F1 (New) vs (Champion) │
                                 │       └───────┬──────────────┬───────┘
                                 │               │              │
                    F1_New >= F1_Champion        │              │ F1_New < F1_Champion
                                 ┌───────────────┘              └───────────────┐
                                 ▼                                              ▼
                    ┌─────────────────────────┐                    ┌─────────────────────────┐
                    │  TRƯỜNG HỢP A: THẮNG /  │                    │   TRƯỜNG HỢP B: THUA    │
                    │         BẰNG            │                    │  (CHẶN TỰ ĐỘNG THĂNG)   │
                    └────────────┬────────────┘                    └────────────┬────────────┘
                                 │                                              │
                                 ▼                                              ▼
                    - Xóa alert lỗi cũ                             - Xuất HTML báo cáo sự cố
                    - Đăng ký New Version lên MLflow               - Đăng ký alias @candidate (Contender)
                    - Gán alias @champion sang bản mới             - Giữ nguyên @champion cũ phục vụ
                    - FastAPI phục vụ model mới ngay               - Streamlit hiện Scorecard so sánh
                    - Xóa alias @candidate cũ nếu có               - Admin có nút Force Promote đánh đổi
                                 │                                              │
                                 └──────────────────────┬───────────────────────┘
                                                        │
                                                        ▼
                                       ┌──────────────────────────────────┐
                                       │ Hoàn tất Task 2:                 │
                                       │ - Ghi drift_metrics vào SQL      │
                                       │ - Khóa is_processed = 1          │
                                       │ - Ghi Run "Batch_{ds}" lên MLflow│
                                       └──────────────────────────────────┘
### 📋 Chi Tiết Từng Giai Đoạn Vận Hành

#### Giai Đoạn 1: Airflow Khởi Chạy DAG (`daily_sentiment_analysis`)
1. **00:00 Nửa đêm:** Airflow Scheduler tự động kích hoạt DAG với execution date `{{ ds }}` đại diện cho ngày vừa trôi qua.
2. **Task 1: `crawl_daily_ev_reviews` (Thu thập dữ liệu):**
   * Lấy **20 đánh giá xe điện mới** từ kho dữ liệu mô phỏng (`data/ev_feed_simulation_pool.csv`).
   * Phân loại danh mục khía cạnh (`pin_sac`, `van_hanh`, `noi_that`, `dich_vu`).
   * Ghi 20 dòng này vào bảng cơ sở dữ liệu `store_reviews` với cờ `is_processed = 0` (đánh dấu dữ liệu chưa chấm điểm).
   * *Bảo vệ Idempotency:* Nếu ngày này đã có dữ liệu trong SQL thì tự động bỏ qua, không ghi đè.
3. **Task 2: `batch_scoring_and_drift_monitoring` (Chấm điểm & Giám sát):**
   * Quét bảng `store_reviews` lấy các dòng có `is_processed = 0` của ngày hôm đó.
   * Tải mô hình đương kim vô địch **`@champion`** từ MLflow Model Registry (`models:/ev-sentiment-model@champion`).
   * Thực hiện suy luận: Dự đoán nhãn (Positive / Neutral / Negative) cùng xác suất tự tin `confidence` và lưu vào bảng `predictions`.
   * **Kiểm tra trôi dạt (Data Drift):** Tính chỉ số **PSI (Population Stability Index)** giữa phân phối sentiment của ngày hôm nay so với phân phối chuẩn ban đầu.
     * **Nếu PSI ≤ 0.2 (Không trôi dạt):** Mọi thứ ổn định ➔ Bỏ qua huấn luyện lại ➔ Nhảy thẳng tới bước chốt dữ liệu.
     * **Nếu PSI > 0.2 (Phát hiện Data Drift):** Tự động kích hoạt cơ chế **Self-Healing Retraining** (gọi lệnh chạy `python ml/train_model.py` với biến môi trường `DRIFT_DATE={{ ds }}`).

#### Giai Đoạn 2: Huấn Luyện Lại Tự Động (`ml/train_model.py`)
1. **Chia dữ liệu cố định:** Tải bộ 1,529 đánh giá xe điện chuẩn (`data/ev_reviews_vietnam_1529_cleaned.csv`), dùng hạt giống cố định (`random_state=42`) để tách:
   * Tập Train: 80% (1,223 dòng).
   * Tập Validation (`val_df`): 10% (153 dòng) – **đây là đề thi chuẩn độc lập của Gatekeeper**.
   * Tập Test: 10% (153 dòng).
2. **Nạp dữ liệu phản hồi (Active Learning & Drift Feedback Loop):**
   * Lấy toàn bộ các review đã được chuyên gia con người thẩm định (`verified_sentiment IS NOT NULL`) từ bảng `store_reviews`.
   * Lấy các review thuộc ngày bị drift, tự động gán nhãn dự phòng.
   * **Gộp toàn bộ dữ liệu mới này vào duy nhất tập Train** (tuyệt đối không làm thay đổi tập `val_df` để chống rò rỉ dữ liệu).
3. **Huấn luyện mô hình ứng viên (Candidate):** Học lại bộ từ vựng TF-IDF và thuật toán phân loại trên tập Train đã mở rộng.
4. **Kiểm tra Gatekeeper:**
   * Cho mô hình mới dự đoán trên tập `val_df` ➔ Ra điểm `Macro-F1 (New)`.
   * Tải mô hình `@champion` đang phục vụ về, cho dự đoán trên cùng tập `val_df` ➔ Ra điểm `Macro-F1 (Champion)`.

#### Giai Đoạn 3: Rẽ Nhánh Tại Gatekeeper (2 Kịch Bản Phân Nhánh)
* **🟢 KỊCH BẢN A: Mô hình mới TỐT HƠN hoặc BẰNG (`Macro-F1 New >= Macro-F1 Champion`):**
  * Xóa các cảnh báo lỗi cũ trong `data/alerts/retrain_failed_*.html`.
  * Ghi log tham số, Macro-F1, Accuracy, Confusion Matrix lên MLflow Run.
  * Kiểm tra lần cuối trên tập Test mù độc lập.
  * Đăng ký phiên bản mới lên MLflow Model Registry và **thăng hạng trực tiếp lên `@champion`**. Xóa tag contender cũ nếu có.
  * **Tác động:** FastAPI (`:8000`) và đợt batch scoring tiếp theo lập tức phục vụ bằng mô hình mới; Streamlit (`:8501`) cập nhật phiên bản champion ngay lập tức.
* **🔴 KỊCH BẢN B: Mô hình mới KÉM HƠN (`Macro-F1 New < Macro-F1 Champion`):**
  * **Chặn tự động thăng hạng:** Không cho đè lên `@champion`. Giữ nguyên Champion cũ đang chạy ổn định để đảm bảo **Zero Downtime & Zero Regression**.
  * **Đăng ký làm Contender (`@candidate`):** Mô hình mới vẫn được lưu trữ và gắn tag `@candidate` trên MLflow Model Registry để theo dõi và so sánh.
  * **Xuất báo cáo sự cố:** Tạo file HTML tại `data/alerts/retrain_failed_YYYY_MM_DD_HHMM.html` so sánh trực quan Macro-F1 của 2 mô hình.
  * **Bảng so sánh & Quyền can thiệp thủ công (Admin Break-Glass Override trên Streamlit):**
    * Trên thanh Sidebar của Streamlit xuất hiện bảng **Scorecard đối đầu trực tiếp**: So sánh Macro-F1, Accuracy và số lượng mẫu huấn luyện (`train_dataset_size`).
    * **Nút bấm:** `⚠️ Chấp nhận đánh đổi: Ép lên Champion 🏆`.
    * **Ý nghĩa nghiệp vụ:** Giúp Admin có thể chủ động chấp nhận đánh đổi (ví dụ: F1 giảm nhẹ 1-2% trên tập val cũ nhưng mô hình đã kịp học thêm 50+ từ vựng xe điện mới phát sinh ngoài thực tế) mà không cần tốn chi phí và thời gian escalate cho Data Scientist can thiệp code.
  * **Con người can thiệp (Active Learning):** Chuyên gia có thể mở tab Active Learning Audit trên Streamlit để thẩm định và đính chính thêm các nhãn sai, sau đó bấm `Trigger Retrain Manual` khi đã có dữ liệu vàng chuẩn.

#### Giai Đoạn 4: Hoàn Tất Task 2 và Đóng Mẻ Chạy
1. Task 2 ghi nhận kết quả PSI và trạng thái drift vào bảng SQL `drift_metrics`.
2. **Khóa trạng thái (State-Locking):** Chạy `UPDATE store_reviews SET is_processed = 1` cho các đánh giá của ngày đó để chống tính toán trùng lặp.
3. Ghi log hoàn tất mẻ chạy `Batch_{ds}` lên MLflow và Airflow kết thúc với màu xanh lá (**Success**).

---

## 🏗️ Project Architecture

The system consists of **6 cohesive services** orchestrated via Docker Compose:

```
              ┌──────────────────────────┐
              │   generate_reviews.py    │ (Daily Simulated Feed)
              └─────────────┬────────────┘
                            │ (Unlabeled CSVs)
                            ▼
 ┌────────────────────────────────────────────────────────┐
 │                      AIRFLOW HOME                      │
 │  ┌───────────────────┐        ┌─────────────────────┐  │
 │  │ Airflow Webserver │◄──────►│  Airflow Scheduler  │  │ (Orchestrates Scoring,
 │  │    (Port 8080)    │        │  (SQLite Metadata)  │  │  PSI Drift Monitoring)
 │  └───────────────────┘        └──────────┬──────────┘  │
 └──────────────────────────────────────────┼─────────────┘
                                            │
                                            ▼ (Scores & Drift Logs)
┌───────────────────────┐        ┌─────────────────────┐        ┌──────────────────┐
│     FastAPI API       │        │ PostgreSQL Database │◄───────┤    Streamlit     │
│   (Live serving @   │◄───────┼─►   (Port 5432)     │        │    Dashboard     │
│       champion)       │        │                     │        │   (Port 8501)    │
└───────────────────────┘        └─────────────────────┘        └──────────────────┘
```

---

## 🚀 Execution & Quick-Start Modes

To provide maximum flexibility and ease of grading, this project supports **two different, mutually exclusive execution modes**:

---

### ⚡ Mode A: Local Python Mode (For Quick Personal Testing & Development)
*Use this to quickly test and run the dashboard and APIs directly on your local system using local files without launching background containers.*

*   **How to Start (Single Action):** Open your terminal inside the project root folder and run:
    ```bash
    python run_local.py
    ```
    *This script automatically runs your automated Pytest quality checks, and then boots both the FastAPI server (on port `8000`) and the Streamlit dashboard (on port `8501`) concurrently.*
*   **Where to Open in Browser:**
    *   **Streamlit Dashboard:** [http://localhost:8501](http://localhost:8501)
    *   **FastAPI Swagger Docs:** [http://localhost:8000/docs](http://localhost:8000/docs)
*   **How to Stop (Single Action):** Press **`[Ctrl + C]`** in that terminal window. Both servers will terminate cleanly and free the ports immediately.

---

### 🐳 Mode B: Containerized Production Mode (For Grading, Demos & Cloud Deployment)
*Use this to spin up and demonstrate the complete, database-backed network of all 6 containerized services (including Postgres, Airflow, and MLflow).*

#### 🚀 **1-Command Clean Slate Deployment**
To deploy or reset the full production stack from scratch on any environment (local machine or GCP/AWS Linux VM), run this single chain command:
```bash
docker compose down -v && \
docker compose build fastapi && \
docker compose run --rm fastapi python ml/train_model.py && \
docker compose run --rm fastapi python data/ingest_pipeline.py --backfill --reset && \
docker compose up -d --build && \
docker compose exec airflow-webserver airflow dags unpause daily_sentiment_analysis
```
*What this does in under 60 seconds:*
1. Wipes legacy containers and volumes (`down -v`).
2. Builds images and trains the initial model, registering it as `@champion` in MLflow.
3. Pre-populates PostgreSQL with 25 days of stable historical data (500 non-repetitive EV reviews).
4. Launches all 6 containers in the background: Postgres (`5432`), MLflow (`5000`), Airflow (`8080`), FastAPI (`8000`), Streamlit (`8501`).
5. Unpauses the Airflow DAG `daily_sentiment_analysis` so automated daily scoring runs immediately.

#### 🌐 **Where to Open in Browser**
*   **Streamlit Analytics Dashboard:** [http://localhost:8501](http://localhost:8501) *(Live PostgreSQL connection, Active Learning audit & serving metrics)*
*   **Orchestration UI (Airflow):** [http://localhost:8080](http://localhost:8080) *(Username: `mlops` | Password: `mlops`)*
*   **Experiment Registry (MLflow):** [http://localhost:5000](http://localhost:5000) *(Tracks experiment `ev-sentiment-analysis` & model `ev-sentiment-model`)*
*   **On-Demand Serving (FastAPI Docs):** [http://localhost:8000/docs](http://localhost:8000/docs)

#### ⚙️ **Standard Operations Playbook**
*   **Update Code (Zero Data Loss):** Pull new changes and hot-recreate only modified containers without touching PostgreSQL history:
    ```bash
    git pull && docker compose up -d --build
    ```
*   **Stop Stack (Keep Data):** Gracefully pause all containers while preserving database volume history:
    ```bash
    docker compose down
    ```
*   **Resume Stack (Keep Data):** Bring containers back online with all previous history intact:
    ```bash
    docker compose up -d
    ```
*   **Complete Reset & Wipe:** Simply re-run the **1-Command Clean Slate Deployment** above.

---

## 🔄 Chi Tiết 3 Luồng Vận Hành Hệ Thống (MLOps Operational Flows)

### 1️⃣ **Luồng Chạy Tự Động Cuối Ngày (Scheduled Midnight Run - `@daily`)**
*   **Thời điểm kích hoạt:** Airflow Scheduler tự động kích hoạt DAG `daily_sentiment_analysis` vào 00:00 mỗi ngày với execution date `{{ ds }}` đại diện cho ngày vừa hoàn tất.
*   **Task 1: `crawl_daily_ev_reviews` (Thu thập dữ liệu mô phỏng):**
    *   Bot crawler trích xuất 20 đánh giá EV mới (chưa từng thu thập) từ kho dữ liệu mô phỏng `data/ev_feed_simulation_pool.csv`.
    *   Chỉ trích xuất raw text (không dùng nhãn sentiment có sẵn để đảm bảo tính khách quan thực tế), tự động phân loại danh mục khía cạnh xe điện (`pin_sac`, `van_hanh`, `noi_that`, `dich_vu`), gán `review_date = {{ ds }}`, `is_processed = 0`, `verified_sentiment = NULL`.
    *   Ghi dữ liệu vào bảng `store_reviews` với cơ chế **Idempotency** (nếu ngày `{{ ds }}` đã có dữ liệu crawl thì tự động bỏ qua, chống trùng lặp).
*   **Task 2: `batch_scoring_and_drift_monitoring` (Dự đoán hàng loạt & Giám sát trôi dạt):**
    *   Truy vấn các review đang chờ xử lý (`is_processed = 0`) thuộc ngày `{{ ds }}` từ `store_reviews`.
    *   Tiền xử lý văn bản qua hàm chuẩn hóa `clean_text`.
    *   Tải trực tiếp mô hình `@champion` đang phục vụ từ MLflow Model Registry (`models:/ev-sentiment-model@champion`).
    *   Thực hiện suy luận phân loại cảm xúc (Positive / Neutral / Negative) và tính toán độ tự tin `confidence` (từ `predict_proba`), sau đó lưu vào bảng `predictions`.
    *   Tính toán chỉ số trôi dạt dữ liệu **Population Stability Index (PSI)** so với phân phối sentiment chuẩn ban đầu.
    *   **Khi phát hiện Data Drift (`psi_score > 0.2`):**
        *   Tạo báo cáo cảnh báo HTML tại `data/alerts/drift_alert_<date>.html`.
        *   Gửi email cảnh báo thời gian thực qua Gmail SMTP (nếu cấu hình `SMTP_PASSWORD`).
        *   **Cơ chế Tự phục hồi (Self-Healing Retraining):** Tự động gọi pipeline `ml/train_model.py`: gộp dữ liệu huấn luyện gốc với các nhãn do con người thẩm định (`verified_sentiment IS NOT NULL`), đào tạo lại các mô hình ứng viên, kiểm định qua **Gatekeeper** đối đầu với champion hiện tại. Nếu mô hình mới vượt trội về Macro-F1, hệ thống lập tức thăng hạng lên `@champion` mới trong MLflow; nếu không đạt, hủy đăng ký và xuất báo cáo sự cố `data/alerts/retrain_failed_*.html`.
    *   Ghi nhận số liệu PSI, độ tự tin trung bình, tổng số mẫu vào bảng `drift_metrics`.
    *   **Khóa trạng thái (State-Locking):** Cập nhật `is_processed = 1` cho các đánh giá vừa xử lý để không bao giờ bị tính trùng.
    *   Ghi log thông số mẻ chạy (`batch_psi_score`, `batch_avg_confidence`, `batch_row_count`) lên MLflow run `Batch_{ds}`.

---

### 2️⃣ **Luồng Kích Hoạt DAG Thủ Công (Manual DAG Trigger)**
*   **Thời điểm kích hoạt:** Khi kỹ sư MLOps bấm nút **"Trigger DAG"** trên giao diện Airflow Web UI (`http://localhost:8080`) hoặc chạy lệnh CLI `airflow dags trigger daily_sentiment_analysis`.
*   **Cơ chế thực thi:**
    *   Airflow lập tức khởi tạo một DagRun mới với `logical_date` là ngày hiện tại.
    *   **Task 1 (`crawl_daily_ev_reviews`):** Kiểm tra xem ngày hiện tại đã có đợt crawl nào chưa:
        *   Nếu chưa có: Bốc 20 review mới từ pool nạp vào `store_reviews` với `is_processed = 0`.
        *   Nếu ngày hôm nay đã từng crawl: Cơ chế Idempotency kích hoạt, ghi log thông báo và bỏ qua bước insert để bảo vệ cơ sở dữ liệu.
    *   **Task 2 (`batch_scoring_and_drift_monitoring`):**
        *   Tìm tất cả các bản ghi `is_processed = 0` tương ứng ngày chạy và thực hiện toàn bộ quy trình: Preprocess ➔ Suy luận với model Champion ➔ Lưu bảng `predictions` ➔ Tính PSI Drift ➔ Kích hoạt Self-Healing nếu có drift ➔ Lưu bảng `drift_metrics` ➔ Cập nhật `is_processed = 1` ➔ Log MLflow.
        *   *(Lưu ý: Nếu kích hoạt script CLI trực tiếp `python airflow_home/dags/batch_scoring.py` mà không truyền `--date`, pipeline sẽ tự động quét TẤT CẢ các ngày đang tồn đọng `is_processed = 0` trong database và xử lý dứt điểm lần lượt từng ngày).*

---

### 3️⃣ **Luồng Kích Hoạt Huấn Luyện Lại Thủ Công (Manual Retraining Trigger)**
*   **Thời điểm kích hoạt:** Người vận hành bấm nút **"⚡ Trigger Retrain Manual"** trên sidebar giao diện Streamlit hoặc chạy lệnh `python ml/train_model.py` trong terminal/container.
*   **Cơ chế thực thi:**
    1.  **Thu nhận dữ liệu Active Learning:** Quét bảng `store_reviews` tìm các bản ghi đã được chuyên gia con người thẩm định hoặc sửa nhãn (`verified_sentiment IS NOT NULL` từ công cụ Active Learning Audit trên Streamlit).
    2.  **Mở rộng tập huấn luyện (Data Augmentation):** Gộp toàn bộ nhãn người thẩm định này vào tập huấn luyện gốc `data/train_v1.csv` làm nhãn vàng (gold labels), giúp mô hình cập nhật từ vựng mới của thị trường EV.
    3.  **Huấn luyện đa mô hình:** Khởi tạo TF-IDF vectorizer và huấn luyện 4 thuật toán phân loại (Logistic Regression, Linear SVM, Multinomial Naive Bayes, Complement Naive Bayes / Random Forest) trên tập dữ liệu đã mở rộng.
    4.  **Đánh giá trên tập Validation chuẩn:** Đánh giá Macro-F1 và Accuracy trên tập kiểm định độc lập `data/val_v1.csv`.
    5.  **Cơ chế Gatekeeper Validation (Chốt chặn an toàn):**
        *   So sánh Macro-F1 của mô hình ứng viên tốt nhất với mô hình `@champion` đang phục vụ thực tế.
        *   **Đạt chuẩn (Pass):** Nếu `Macro-F1 (New) >= Macro-F1 (Champion)`, mô hình mới được đăng ký phiên bản tiếp theo vào MLflow Model Registry và tự động thăng hạng lên `@champion`. Model mới lập tức có hiệu lực phục vụ cho cả FastAPI và Batch scoring.
        *   **Không đạt (Fail):** Nếu mô hình mới có hiệu năng thấp hơn Champion cũ, Gatekeeper từ chối tự động thăng hạng để bảo đảm độ ổn định hệ thống. Thay vào đó, mô hình mới được đăng ký với alias `@candidate` (Contender) và ghi nhận báo cáo sự cố `data/alerts/retrain_failed_YYYY_MM_DD_HHMM.html`. Trên giao diện Streamlit xuất hiện bảng Scorecard đối đầu trực tiếp kèm nút **`⚠️ Chấp nhận đánh đổi: Ép lên Champion 🏆`** cho phép Admin chủ động đưa Contender lên thay thế Champion nếu thấy hợp lý về mặt nghiệp vụ (Break-Glass Override).
    6.  **Ghi chép MLflow Tracking:** Toàn bộ siêu tham số, chỉ số đánh giá, kích thước tập dữ liệu (`train_dataset_size`), và biểu đồ Confusion Matrix được lưu trữ đầy đủ trên MLflow.

---

## 📊 Monitoring, Self-Healing & Retraining Logs

To easily monitor continuous batch scoring, data drift alerts, and the automated self-healing retraining loop, the pipeline aggregates detailed logging across **4 primary sources**:

### **1. Automated Self-Healing Logs (Airflow Orchestration)**
When the daily batch scoring DAG detects data drift and automatically triggers the retraining loop, all stdout/stderr logs are captured inside Airflow.
*   **Where to inspect:** Inside the **Airflow Web UI** (`http://localhost:8080`).
*   **How to view:** Log in with `mlops / mlops` ➔ Click the **`daily_sentiment_analysis`** (or batch scoring) DAG ➔ Select the latest completed scoring task (marked green) ➔ Click the **`Log`** tab at the top. Here, you will see the complete terminal logs of the model retraining process, including SQL ingestion, TF-IDF feature weights shift, evaluation, and programmatical MLflow registration.
*   **DAG Architecture:**
    1. **`crawl_daily_ev_reviews`**: Simulates automated daily EV scraping. Randomly samples 20 fresh, non-overlapping reviews from the 10,000 EV review pool (`data/ev_feed_simulation_pool.csv`), stamps them with the current execution date (`{{ ds }}`), classifies domain aspects (`pin_sac`, `van_hanh`, `noi_that`, `dich_vu`), and commits them to `store_reviews` with idempotency guards.
    2. **`batch_scoring_and_drift_monitoring`**: Fetches newly queued reviews (`is_processed = 0`), predicts sentiment using the active `@champion` model, computes Population Stability Index (PSI) drift, and triggers self-healing retraining if drift exceeds threshold.


### **2. Manual Retraining Logs (Streamlit Container)**
When an administrator triggers manual retraining by clicking the **`Trigger Retrain Manual`** button on the Streamlit sidebar, the script runs inside the Streamlit container.
*   **Where to inspect:** Streamlit service terminal stdout.
*   **How to view:** Open your SSH VM console or local terminal and run:
    ```bash
    docker compose logs -f streamlit
    ```
    This will stream real-time logs from `ml/train_model.py` as it compiles validation metrics, merges newly labeled rows, and validates against the champion.

### **3. Model Comparison & Metadata Logs (MLflow Registry)**
Every successful retraining run that passes the automated validation Gatekeeper is registered and versioned.
*   **Where to inspect:** The **MLflow Web UI** (`http://localhost:5000`).
*   **How to view:** Select your active run ➔ Audit key hyperparameters, validation scores (Macro-F1, Accuracy), the **`train_dataset_size`** parameter (proving newly verified labels were ingested!), and view the interactive validation `Confusion Matrix` inside the artifacts section.

### **4. Retraining Incident Reports (Gatekeeper Failure Logs)**
If the retraining candidate fails to outperform the current champion, the automated Gatekeeper aborts registration and dumps a persistent HTML incident report.
*   **Where to inspect:** On the VM host filesystem inside **`data/alerts/`**.
*   **How to view:** Check for files named `retrain_failed_YYYY_MM_DD_HHMM.html`. These reports break down the macro-F1 scores of both models side-by-side, explaining why the update was blocked to keep the serving layer stable.

---

## 🛠️ Linux VM & Docker Troubleshooting

If you are deploying on a fresh Linux Cloud Server (such as **Google Cloud Platform VM / AWS EC2**) or an older machine, you may encounter system-level Docker version conflicts. Here is how to resolve them instantly:

### 🚨 1. Unknown Command: `docker compose` or KeyError: `ContainerConfig`
If running the `docker compose` command fails with an error or throws `KeyError: 'ContainerConfig'` during startup, your machine is running an obsolete version of the Python-based Docker Compose V1 (e.g., version `1.29.2`). 

Upgrade to the official, highly optimized **Docker Compose V2** (written in Go) instantly with these commands:
```bash
# 1. Create CLI plugins directory
mkdir -p ~/.docker/cli-plugins/

# 2. Download the official Docker Compose V2 binary from GitHub
curl -SL https://github.com/docker/compose/releases/download/v2.24.1/docker-compose-linux-x86_64 -o ~/.docker/cli-plugins/docker-compose

# 3. Apply executable permissions
chmod +x ~/.docker/cli-plugins/docker-compose

# 4. Overwrite any legacy /usr/local/bin symlinks to allow both syntaxes
sudo curl -SL https://github.com/docker/compose/releases/download/v2.24.1/docker-compose-linux-x86_64 -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```
Verify the upgrade with `docker compose version` (it should now report `v2.24.1+`).

### 🚨 2. SQLite Error: `unable to open database file`
In older architectures, mounting a single non-existent host file to a container (like `- ./mlflow.db:/app/mlflow.db`) caused Docker to erroneously create `mlflow.db` as a **directory** on the host. 

To fix this once and for all, **our architecture unifies all SQLite database persistence (both `results.db` and `mlflow.db`) inside the standard `./data/` folder**, which is mapped at the folder-level as `- ./data:/app/data`. This guarantees 100% database persistence, eliminates file-to-folder clashes, and ensures a clean run right out-of-the-box!

If you see this error on a legacy VM, simply clean up any Docker-generated directories by running:
```bash
rm -rf mlflow.db
```

---

### ⚠️ IMPORTANT: PORT CONFLICT WARNING
Do **NOT** run `python run_local.py` while Docker is active! Since Docker occupies ports `8000` and `8501` for the containerized API and dashboard, running the local script at the same time will fail with an **`AddressAlreadyInUse` / `Port in use`** error. 

Always ensure one mode is fully stopped (`Ctrl + C`) before starting the other!

---

## 🏆 Active Model Selection & Benchmark Leaderboard

During our evaluation and benchmarking phase, we evaluated candidate architectures on stratified splits and logged experiments to MLflow. For our Vietnamese EV sentiment analysis corpus (`data/ev_reviews_vietnam_1529_cleaned.csv`), **Logistic Regression (`C=2.0`, `solver='lbfgs'`)** is the active `@champion` model:

| Model Candidate | Validation Macro-F1 | 5-Fold CV Macro-F1 | Validation Accuracy | Selection Status |
| :--- | :---: | :---: | :---: | :---: |
| **Logistic Regression (`C=2.0`)** | **0.9203** | **0.8782 ± 0.0182** | **91.50%** | **🏆 Champion (Active in MLflow Registry as @champion)** |
| **Linear SVM (`SGD log_loss`)** | 0.9265 | 0.8731 ± 0.0150 | 92.16% | Contender / Alternative |
| **Multinomial Naive Bayes** | 0.9203 | 0.8789 ± 0.0147 | 91.50% | Contender |
| **Complement Naive Bayes** | 0.9203 | 0.8789 ± 0.0147 | 91.50% | Contender |
| **Random Forest (150 trees)** | 0.9203 | 0.8752 ± 0.0123 | 91.50% | Baseline *(Tricked on contrastive clauses)* |

*The winning Logistic Regression pipeline was scored on the holdout test split (153 unseen reviews), achieving **88.24% Test Accuracy and 0.8830 Holdout Macro-F1** (Negative F1: 0.9114, Neutral F1: 0.8269, Positive F1: 0.9106).*

---

### 🧠 Model Selection Rationale (Why Logistic Regression?)

We chose **Logistic Regression (`C=2.0, solver='lbfgs'`)** as our active `@champion` serving model based on four critical production factors:

1. **Superior Handling of Contrastive & Nuanced Clauses:** In semantic stress-testing on nuanced Vietnamese automotive reviews (e.g. *"Nội thất nhìn thì hào nhoáng nhưng chất lượng gia công ọp ẹp, đi qua gờ kêu lạch cạch khó chịu"*), Logistic Regression successfully balanced the negative contrastive clause over deceptive positive words (`hào nhoáng`), whereas tree ensembles failed.
2. **Smooth Calibrated Probability Distributions (`predict_proba`):** Logistic Regression generates well-calibrated class probabilities, enabling real-time confidence scores and uncertainty filtering (Tier Bucketing into High ≥80%, Moderate 60-79%, and Low <60%) in the Active Learning loop.
3. **Model Interpretability (Explainable AI):** Linear coefficients directly represent the positive or negative pull of individual n-grams, enabling developers and business operators to audit why a review received a specific sentiment.
4. **Sub-millisecond Latency on CPU:** Training finishes in under $0.05$ seconds and inference runs in sub-milliseconds on single-thread standard CPUs without requiring GPU infrastructure.

---

### 🔬 Evaluation Tests & Techniques Employed

To ensure complete fairness, scientific rigor, and prevent data leakage, we utilized the following methodologies:

*   **Stratified Holdout Testing:** We performed a **stratified split (80/10/10)** on our 1,529 Vietnamese EV customer review dataset (`data/ev_reviews_vietnam_1529_cleaned.csv`) to preserve perfectly balanced Positive, Neutral, and Negative label ratios across training, validation, and testing partitions.
*   **Macro-F1 as the Selection Metric:** Since sentiment data can suffer from domain-specific distribution shifts, we selected **Macro-F1** (average of class-specific F1 scores) rather than basic Accuracy as our primary selection metric. This forces the model to perform highly on all three sentiment classes (Positive, Neutral, Negative) rather than biasing towards the majority class.
*   **Unbiased Test Set Verification:** The final champion was evaluated only once on the fully isolated, unseen test partition to obtain an unbiased indicator of real-world generalization.
*   **Confusion Matrix Diagnosis:** We utilized `ConfusionMatrixDisplay` to diagnose class-specific bottlenecks. This test confirmed that Logistic Regression maintains clean decision boundaries and handles the hard semantic boundaries between `neutral` and other classes highly effectively.
*   **MLflow Experiment Auditing:** All hyperparameters, validation metrics (Accuracy, Macro-Precision, Macro-Recall, Macro-F1), training dataset hashes, and confusion matrix artifacts were logged transparently, enabling 100% reproducibility.

---

## 🛠️ Offline Local Development & Testing

If you want to test and run the entire application locally on your machine without using Docker containers, we have built a **single-action orchestrator script** (`run_local.py`):

### **How to Run (Single Action):**
```bash
python run_local.py
```
*This will automatically execute all automated `pytest` quality checks, boot your real-time FastAPI serving server, and launch your interactive Streamlit dashboard concurrently in the background.*

### **How to Stop (Single Action):**
Simply press **`[Ctrl + C]`** in that terminal window. This will automatically terminate both background servers cleanly, free ports `8000` & `8501` completely, and exit gracefully with zero orphaned background tasks.

---

### **Individual Pipeline Component Scripts:**
If you need to execute individual pipeline steps manually, ensure `PYTHONPATH` is set to your project root:

1.  **Run Cloud Ingestion & Backfill:** `python data/ingest_pipeline.py --backfill` *(Generates and scores 25 days of stable historical reviews offline using high-quality local templates to establish the baseline and pre-populate your database and dashboard charts)*.
2.  **Submit Customer Reviews:** `python data/submit_review.py` *(Spawns the storefront CLI app to submit custom reviews into the database pending scoring)*.
3.  **Train & Select Champion:** `python ml/train_model.py` *(Automated model training, logs to `ev-sentiment-analysis`, registers under `ev-sentiment-model`, and promotes to `@champion`)*.
4.  **Test API Locally:** `python api/main.py` *(Launches FastAPI on `:8000`)*.
5.  **Run Quality Assurances:** `python -m pytest ml/test_pipeline.py` *(Runs lint and structural syntax checks)*.

---

## 🌟 Advanced Production Features (MLOps Maturity Level Up)

While standard academic projects stop at basic drift detection, this production-ready pipeline implements advanced enterprise-grade features:

*   **Active Learning & Human-in-the-Loop Audit (Ground-truth Feedback Loop):** Operators can audit model predictions in bulk directly on the Streamlit dashboard using an interactive, spreadsheet-like grid (`st.data_editor`). Features a smart **Smart Unlocking Mechanism** that unlocks if there is an active drift alert, a gatekeeper failure, unverified reviews in any historical drift batch, or via a manual **`🔓 Mở khóa thủ công`** override toggle. Features a **1-Click Bulk Approval** mechanism that clones predictions into human-verified ground-truth labels. The stateless retraining loop (`train_model.py`) natively scans `store_reviews` for these human overrides (`verified_sentiment IS NOT NULL`), merges them as gold training labels, and expands the model's vocabulary dynamically!
*   **On-Demand Serving Circuit Breaker:** Features a served-mode fallback toggle in the dashboard sidebar that instantly redirects FastAPI traffic from the ML model to a deterministic, keyword-based safe-mode rule classifier in case of production anomalies, ensuring business continuity.
*   **Comprehensive Model Health & Uncertainty Analytics:** The Streamlit dashboard visualizes 4 specialized MLOps charts: (1) **Category Sentiment Breakdown** (with high-contrast traffic-light palette: Red `#e74c3c` for Negative, Sunflower Yellow `#f1c40f` for Neutral, and Green `#2ecc71` for Positive), (2) **Average Confidence by Sentiment Class**, (3) **Confidence Uncertainty Distribution** (Uncertainty Tier Bucketing into High ≥80%, Moderate 60-79%, and Low <60% to prioritize hard samples), and (4) **Human-AI Agreement Calibration Metrics** (`Human-AI Agreement %`, `Audited Reviews`, and `Human Overrides`) inside the Active Learning workspace.
*   **Calibrated Baseline & Backfill Safeguards:** The historical 25-day backfill pipeline (`data/ingest_pipeline.py`) uses a calibrated Vietnamese EV baseline producing balanced sentiment distributions ($PSI \approx 0.0051 \ll 0.15$), with `auto_retrain=False` safety guards to avoid spurious retraining loops during offline database initialization. Retraining fallback pseudo-labelers natively evaluate Vietnamese automotive vocabulary (*"lỗi", "chậm", "sụt pin", "êm", "tiết kiệm"...*).

---
---

## 🚗 Vietnamese EV Review Dataset (`data/ev_reviews_vietnam_1529_cleaned.csv`)

To power domain-specific sentiment classification and realistic benchmarking for Vietnamese automotive NLP, the repository uses a clean, verified EV customer dataset:

*   **File Path:** `data/ev_reviews_vietnam_1529_cleaned.csv`
*   **Total Scale:** 1,529 unique rows with **100% complete ground-truth labels**.
*   **Cleaning Applied:** Capitalization normalized, misplaced mid-sentence punctuation fixed (`. so với` ➔ `, so với`), double punctuation removed, terminal punctuation ensured, Vietnamese Unicode preserved.
*   **Sentiment Distribution:**
    *   **Positive:** 582 reviews (38.06%)
    *   **Neutral:** 537 reviews (35.12%)
    *   **Negative:** 410 reviews (26.81%)
*   **Brand Distribution:** VinFast (426), BYD (292), Tesla (215), MG (169), Hyundai (154), Kia (149), Wuling (124).
*   **Source Distribution:** YouTube (319), Dealer (316), Forum (309), Review site (293), Facebook (292).

> **Lưu ý về thư mục `docs/`:** Thư mục `docs/` chứa tài liệu báo cáo (Slide thuyết trình `TMA Slide-Session 10.ppt`, bảng phân công `MLOPS-Projects.xlsx`, báo cáo benchmark `benchmark_ev_results.md`, và file raw backup 10,000 dòng) được cấu hình **hoàn toàn chỉ lưu trữ trên máy tính cá nhân (local)** và được thêm vào `.gitignore` để không bị đẩy lên Git.




## 🧠 Scoped Gaps & Production Trade-offs (MLOps Maturity)

To maintain lightweight grading agility, several enterprise-level elements were consciously scoped out:

*   **Autoscaling Infrastructure:** Docker Compose is suitable for single-host VM setups. High-throughput loads require Kubernetes (EKS/GKE) with Horizontal Pod Autoscalers (HPA).
*   **Production Secrets Management:** plain-text files are used for this demo. Enterprise platforms require secured secret key vaults (like HashiCorp Vault or AWS Secrets Manager).
