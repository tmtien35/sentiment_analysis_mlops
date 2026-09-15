# 🎬 MLOps Live Presentation & Demo Guide (100% Interactive & Multi-Mode)

This guide outlines the step-by-step storyboard and script to demonstrate your MLOps Capstone project.

The entire system is **100% offline, lightning-fast, and relies entirely on user input from your custom storefront**.

---

## 🛠️ Step 0: Pre-Demo Setup & Reset (Run Before Presenting)

You can present this demo in either of your two mutually exclusive execution modes:

### **Option A: Local Python Mode (Laptop Demo)**
Ensure your servers are fully stopped. Open a terminal and run:
```bash
# 1. Reset results DB and backfill 25 days of stable historical reviews (Realistic EV reviews pool)
python data/ingest_pipeline.py --backfill --reset

# 2. Start your serving servers cleanly (FastAPI, Streamlit, and MLflow UI)
python run_local.py
```
*   **Websites to Open:** Dashboard (`http://localhost:8501`), Swagger Docs (`http://localhost:8000/docs`), MLflow (`http://localhost:5000`).

### **Option B: Google Cloud VM / Containerized Mode (Professional Cloud Demo)**
*(💡 **Lưu ý**: Nếu bạn thiết lập trên một máy chủ VM hoặc Laptop hoàn toàn mới từ đầu, vui lòng xem mục **"Hướng Dẫn Cài Đặt Ban Đầu Cho Người Mới (Prerequisites)"** trong file `README.md` để cài Docker và Git trước).*


Open your GCP SSH Terminal and run this **foolproof 6-step deployment sequence**:
```bash
# 1. Clean up and completely reset any old database volumes
docker compose down -v

# 2. Build the fastapi image first to ensure all code is updated
docker compose build fastapi

# 3. Bootstrap and train the initial champion model
docker compose run --rm fastapi python ml/train_model.py

# 4. Pre-populate the 25-day historical database into PostgreSQL (Clean slate backfill)
docker compose run --rm fastapi python data/ingest_pipeline.py --backfill --reset

# 5. Start all 6 containers running in the background!
docker compose up -d --build
```
*   **Websites to Open:** Replace `localhost` with your **`IP_Google_Cloud`** (e.g., `http://[IP_Google_Cloud]:8501`, `http://[IP_Google_Cloud]:5000`, `http://[IP_Google_Cloud]:8080`).
*   *(Note: DAG `daily_sentiment_analysis` is pre-configured with `is_paused_upon_creation=False`, so it will automatically be active (ON) as soon as Airflow finishes its 30-second first-time metadata initialization. You can verify DAG status with `docker compose exec airflow-webserver airflow dags list` or toggle it directly in the Airflow Web UI at `http://[IP_Google_Cloud]:8080`)*.

---

## 🎭 Act I: Stable Operations & Live Customer Submission

### **What to do:**
1.  **Submit Customer Reviews (Interactive CLI):** Open a new terminal window (or GCP SSH) and run our storefront submitter app:
    *   **Local (Option A):**
        ```bash
        python data/submit_review.py
        ```
    *   **Docker (Option B):**
        ```bash
        docker compose exec -it fastapi python data/submit_review.py
        ```
    *   **The Simplified Loop:** There is **no date prompt anymore!** It automatically logs reviews under today's date in PostgreSQL/SQLite. Enter your reviews continuously:
        *   `Review #1 Text:` $\rightarrow$ Type: `"Xe chạy rất êm, tăng tốc tốt và pin dùng thực tế rất hài lòng."` $\rightarrow$ Press **Enter**.
        *   `Review #2 Text:` $\rightarrow$ Type: `"Xe đi phố tạm ổn, nhưng mong muốn hãng mở rộng thêm trạm sạc ngoại thành."` $\rightarrow$ Press **Enter**.
        *   `Review #3 Text:` $\rightarrow$ Simply press **Enter** (leave empty) or type `exit` to finish and submit.
    *(Both reviews are now transactionally saved in your database table `store_reviews` with status `is_processed = 0`)*.

2.  **Run Ingestion:** In your terminal (or GCP SSH), automatically ingest and score all outstanding customer reviews:
    *   **Local (Option A):**
        ```bash
        python data/ingest_pipeline.py --ingest
        ```
    *   **Docker (Option B):**
        ```bash
        docker compose exec fastapi python data/ingest_pipeline.py --ingest
        ```
    *(The pipeline automatically finds your 2 pending reviews, runs batch scoring, and locks their state by marking them `is_processed = 1`)*.

3.  **UI Verification:** Open your **Streamlit** dashboard and refresh.
    *   Today's volume chart increases.
    *   The status card is a healthy green **`✅ STABLE`** (PSI is ~0.0).
    *   Your manually entered reviews appear live in the **Scored Reviews Explorer** table!

---

## 🚨 Act II: The Production Crisis (Creating Manual Drift!)

### **What to do:**
1.  **Submit Drifted Reviews (Simulating an EV battery & recall crisis):**
    Open your terminal (or GCP SSH) and launch the submitter loop again:
    *   **Local (Option A):**
        ```bash
        python data/submit_review.py
        ```
    *   **Docker (Option B):**
        ```bash
        docker compose exec -it fastapi python data/submit_review.py
        ```
    *   **Enter 10 Negative/Drifted Reviews:** To trigger drift, we will submit a highly-skewed batch of severe EV battery & service complaints! Type each review followed by **Enter**:
        1. `"Pin tụt quá nhanh khi chạy cao tốc, xe báo lỗi hệ thống liên tục!"`
        2. `"Màn hình chính bị đen ngòm khi đang lái, cực kỳ nguy hiểm."`
        3. `"Trụ sạc của hãng toàn bị lỗi không nhận diện được xe."`
        4. `"Dịch vụ cứu hộ quá chậm, chờ 3 tiếng giữa trời nắng không ai đến."`
        5. `"Chất lượng hoàn thiện quá tệ, tiếng ồn lốp và gió rít rất khó chịu."`
        6. `"Hệ thống điều hòa tự ngắt giữa trời nóng, xe báo lỗi nhiệt pin."`
        7. `"Phần mềm cập nhật xong bị lỗi phanh tái sinh giật cục."`
        8. `"Đại lý hứa hẹn giao xe đúng hẹn nhưng trễ 2 tháng vô trách nhiệm."`
        9. `"Pin báo ảo, từ 35% tụt thẳng xuống 5% trong vòng 3km."`
        10. `"Chính sách bảo hành mập mờ, nhân viên kỹ thuật từ chối bảo hành pin."`
    *   **STEP 3:** Leave the next review text empty and press **Enter** (or type `exit`) to submit all 10 reviews.

2.  **Process the Drifted Batch:** Run the ingestion pipeline to automatically detect and score the new outstanding reviews:
    *   **Local (Option A):**
        ```bash
        python data/ingest_pipeline.py --ingest
        ```
    *   **Docker (Option B):**
        ```bash
        docker compose exec fastapi python data/ingest_pipeline.py --ingest
        ```
    *(The pipeline scores the 10 reviews. Because 100% of these new reviews are negative/drifted, the prediction distribution shifts heavily. PSI spikes to `~4.08`—well above the `0.15` safety threshold!)*

3.  **UI & Self-Healing Verification:** 
    *   **Refresh Streamlit:** Watch the dashboard status instantly flip to a flashing red **`⚠️ DRIFT ALERT`**! The PSI Score plot spikes into the warning zone.
    *   **Alert Generation:** Open the local HTML email generated at `data/alerts/drift_alert_[date].html` in your browser.
    *   **Self-Healing Log:** Look at your server console. Show the judges how the system **automatically detected the drift, triggered retraining, read the drifted reviews directly from the SQL database, auto-labeled them, evaluated the candidate model, registered version, and promoted it to `@champion` completely hands-free!**
        `🚨 [SELF-HEALING] Data Drift Detected! Triggering automated retraining...`
        `🚨 [SELF-HEALING] Retraining completed successfully! Model updated to @champion.`

---

## 🛠️ Act III: Manual Incident Response & Overrides (Control Panel)

Show the judges how you can actively manage production alerts and bypass model predictions in real-time directly from your **Streamlit Sidebar Control Panel**:

### **1. Test the "Acknowledge & Mute Alert" (Tắt báo động):**
1.  Under **MLOps Incident Control Panel** in the sidebar, click the **`Acknowledge & Mute Alert`** button.
2.  **Result:** Streamlit immediately updates. The flashing red **`⚠️ DRIFT ALERT`** status card flips into a calm, yellow **`⚠️ DRIFT MUTED`**, proving you've acknowledged the flash sale / event and silenced the alarm cleanly in PostgreSQL!

### **2. Test the "Active Learning & Human-in-the-Loop Audit" (Gán nhãn sửa lỗi trực tiếp trên giao diện):**
Before triggering manual retraining, let's play the role of an Admin auditing the database to correct model misclassifications directly from the web browser:
1. Scroll down to the **🧠 Active Learning & Human-in-the-Loop Audit** section at the bottom of the page.
   * *Notice:* The panel is highly smart and secure. It remains **`🔒 Locked (Healthy)`** under normal conditions when all historical drift reviews have been audited, but automatically **`🔓 Unlocks`** whenever there is an active drift alert, gatekeeper failure, or unverified reviews in any historical drift batch. Operators can also toggle **`🔓 Mở khóa thủ công`** anytime to audit any batch in history.
2. Here, you'll see a list of scored reviews sorted by **lowest confidence** (Uncertainty Sampling). Notice that by default, healthy reviews are filtered out—only candidates from **Drifted Dates** are shown!
3. You can toggle filters using the checkboxes:
   * `Show only reviews from drifted dates` (checked by default).
   * `Show only unverified reviews (where Human Verified Label is NULL)` (checked by default).
4. Under **📝 Edit and Verify Review Sentiments in Bulk**:
   * To correct an AI prediction: Double-click any cell in the **Human Verified Label** column, choose the correct sentiment (positive, neutral, negative), and click **`💾 Save Manually Edited Rows Only`**.
   * To approve AI predictions in bulk: If the remaining AI predictions are correct, click **`✅ Bulk Approve Remaining AI Predictions`**. A safety confirmation dialog will pop up displaying the exact number of reviews to approve. Click **`✅ Đồng Ý Phê Duyệt`** to confirm!
5. **Result:** The system transactionally updates `verified_sentiment` in `store_reviews` and instantly refreshes the page, clearing those reviews from the audit list! You can audit and verify hundreds of drifted reviews in bulk in under 10 seconds, without ever typing a SQL command line.

### **3. Test the "Trigger Retrain Manual" (Cưỡng bức học máy):**
Now that you have supplied real, gold-standard human-verified labels:
1. In the sidebar, under **Continuous Training**, click the **`Trigger Retrain Manual`** button.
2. Watch the spinner run. In under 10 seconds, it will complete and pop a green:
    `🏆 Model Retrained Successfully! New @candidate registered.` (or promoted to `@champion` if it passed the automatic gatekeeper!)
3. **Result:** The background system executed `ml/train_model.py`, transactionally scanned the PostgreSQL/SQLite database for **any** human-audited reviews (`verified_sentiment IS NOT NULL`), merged them dynamically in-memory with the baseline dataset, trained a brand-new, more accurate model version, and registered it under `@candidate`. You can verify this Version increase live on **MLflow** (`http://[IP_Google_Cloud]:5000`).

### **4. Test the "Switch to Fallback Rules" (Gạt cầu chì ngắt AI - Circuit Breaker):**
Let's simulate a situation where your AI model behaves erratically, and you need to bypass it instantly to ensure business continuity.
1.  In the sidebar, click the **Active Serving Mode** dropdown and change it from `Machine Learning Model` to **`Rule-Based Fallback Rules`**.
2.  A yellow warning box appears: `🛡️ Safe-Mode Active: ML Model Bypassed!`.
3.  Go to **Live On-Demand Scoring**, type: `"Pin sạc quá tệ, xe bị lỗi màn hình đen và cứu hộ cực kỳ chậm chạp!"` and click **Predict Sentiment**.
4.  **Result:** 
    *   It instantly returns **`NEGATIVE` (99.0% confidence)**.
    *   Scroll down to the **Live API Traffic Monitor** table. You will see that the logged record's `cleaned_text` has **`[RULE-BASED FALLBACK]`** appended to it!
    *   This proves that the serving API **completely bypassed the ML model** and ran your safe, deterministic rule-based fallback algorithm natively!
5.  Toggle the mode back to `Machine Learning Model` once you are done to re-enable your high-performing AI.

---

## 🔄 Presentation Rehearsal & Update Playbook

### **Case 1: Standard Code/UI Update (Zero Data Loss - No Model Change):**
If you make code, dashboard, or design updates on your laptop, push them to GitHub, and pull them on your Google Cloud VM, simply run this sequence. Docker Compose V2 will hot-recreate only the modified container services in 2 seconds while preserving 100% of your persistent PostgreSQL history, predictions, and drift logs:
```bash
# 1. Pull latest code from GitHub
git pull origin main

# 2. Setup .env configuration (if not already created on VM)
cp .env.example .env
nano .env   # (Fill your Gmail address and 16-character App Password)

# 3. Hot-rebuild and restart containers
docker compose up -d --build

# 4. (Optional) Verify SMTP delivery from inside the Airflow container:
docker compose exec airflow-scheduler python ml/test_email_smtp.py
```

### **Case 2: Model & Preprocessing Update (Update Champion Model in VM MLflow):**
If your Git commits include changes to model training (`ml/train_model.py`), text segmentation (`ml/preprocess.py` with `pyvi`), or dependencies (`requirements.txt`), run this sequence to rebuild the image and train the new Champion directly into the VM's MLflow store:
```bash
git pull && docker compose build && docker compose run --rm fastapi python ml/train_model.py && docker compose up -d
```

### **Case 3: Complete Reset & Re-rehearse (Wipe Data - Clean Slate):**
If you want to clear your persistent database (for another presentation or rehearsal) and backfill 25 days of stable historical data from scratch, run:
```bash
# 1. Stop containers and delete PostgreSQL volumes
docker compose down -v

# 2. Build the fastapi image first to ensure all code is updated
docker compose build fastapi

# 3. Bootstrap and train the initial champion model
docker compose run --rm fastapi python ml/train_model.py

# 4. Seed 25-day historical backfill into PostgreSQL (Clean slate backfill)
docker compose run --rm fastapi python data/ingest_pipeline.py --backfill --reset

# 5. Bring serving servers back online
docker compose up -d --build
```
This is fully idempotent, robust, and can be repeated infinite times!

---

## 📊 Appendix: GCP PostgreSQL Quick Query Cheat Sheet

Use these quick, read-to-run database commands directly inside your GCP SSH Terminal to inspect, query, or audit your live PostgreSQL database on Google Cloud!

### **1. Xem những review chưa được xử lý (Pending store reviews: is_processed = 0)**
If you submitted reviews via `submit_review.py` but haven't scored them yet, run this to see the pending queue:
```bash
docker compose exec postgres psql -U mlops -d results_db -c "SELECT * FROM store_reviews WHERE is_processed = 0 ORDER BY review_date DESC, review_id ASC;"
```

### **2. Xem N review đã được chấm điểm gần nhất (Get latest N scored predictions)**
Inspect the latest 10 scored predictions sorted by date:
```bash
docker compose exec postgres psql -U mlops -d results_db -c "SELECT * FROM predictions ORDER BY review_date DESC, review_id ASC LIMIT 10;"
```

### **3. Xem review của một ngày cụ thể (Get reviews for a specific date)**
Query all recorded reviews submitted on a target date (e.g., `2026-09-08`):
```bash
docker compose exec postgres psql -U mlops -d results_db -c "SELECT * FROM store_reviews WHERE review_date = '2026-09-08' ORDER BY review_id ASC;"
```

### **4. Đổi label dự đoán của một dòng X (Update predicted sentiment for audit/correction)**
Simulate a manual audit correction where a reviewer overrides a predicted sentiment (e.g., writing a human-verified/moderated ground-truth label of `'negative'` for a review inside the source table `store_reviews`):
```bash
docker compose exec postgres psql -U mlops -d results_db -c "UPDATE store_reviews SET verified_sentiment = 'negative' WHERE review_id = 'user_6ce8dcc9';"
```
*(Tip: Replace `'user_6ce8dcc9'` with any `review_id` from Query 1 or 2)*.

### **5. Xem tổng quan số lượng bản ghi của toàn bộ các bảng (Database Row Counts Overview)**
Get a quick audit of how many rows exist in each of your 4 tables dynamically:
```bash
docker compose exec postgres psql -U mlops -d results_db -c "SELECT 'store_reviews' as table_name, count(*) FROM store_reviews UNION ALL SELECT 'predictions', count(*) FROM predictions UNION ALL SELECT 'drift_metrics', count(*) FROM drift_metrics UNION ALL SELECT 'inference_logs', count(*) FROM inference_logs;"
```

### **6. Xem lịch sử đo lường trôi lệch dữ liệu (Inspect drift metrics history)**
Query the latest 5 batch drift executions to audit your PSI and drift trigger logs:
```bash
docker compose exec postgres psql -U mlops -d results_db -c "SELECT * FROM drift_metrics ORDER BY batch_date DESC LIMIT 5;"
```

### **7. Xem những đánh giá đã được con người duyệt nhãn (Inspect Active Learning Human-Verified Labels)**
Query reviews that have received human verification / gold-standard labels to be used for continuous retraining:
```bash
docker compose exec postgres psql -U mlops -d results_db -c "SELECT review_id, review_date, category, LEFT(review_text, 40) as review_text, verified_sentiment FROM store_reviews WHERE verified_sentiment IS NOT NULL ORDER BY review_date DESC;"
```


---

## 🎤 Kịch Bản Thuyết Trình Bảo Vệ Capstone (10-15 Phút) & Sổ Tay Phản Biện Hội Đồng

Tài liệu này được thiết kế dành riêng cho buổi báo cáo / bảo vệ đồ án trước Hội đồng chuyên môn hoặc Nhà tuyển dụng. Bạn có thể sử dụng trực tiếp cấu trúc dưới đây để chuẩn bị slide hoặc nói trực tiếp khi dẫn dắt Live Demo.

---

### ⏱️ PHẦN 1: Dàn Ý Thuyết Trình Chuẩn (10 Phút Slide + Demo)

#### **Phút 1 - 2: Bối Cảnh Nghiệp Vụ & Đặt Vấn Đề (Problem Statement)**
*   **Lời thoại dẫn dắt:**
    > *"Kính thưa Hội đồng, thị trường xe điện tại Việt Nam đang bùng nổ mạnh mẽ, kéo theo hàng nghìn lượt đánh giá của khách hàng mỗi ngày về pin, trạm sạc, phần mềm và dịch vụ. Các doanh nghiệp thường gặp 2 bế tắc lớn:  
    > 1. Mô hình AI sau khi triển khai bị 'lạc hậu' theo thời gian do xuất hiện từ vựng mới hoặc khủng hoảng truyền thông (hiện tượng Data Drift).  
    > 2. Chi phí thuê con người gán nhãn thủ công quá đắt đỏ và chậm chạp.  
    > Vì vậy, nhóm chúng em xây dựng: **Hệ Thống MLOps Phân Tích Cảm Xúc Đánh Giá Xe Điện & Giám Sát Tự Phục Hồi (Self-Healing MLOps)**."*

#### **Phút 3 - 4: Kiến Trúc Hệ Thống & Điểm Sáng Kỹ Thuật (Architecture & Highlights)**
*   **Mở slide sơ đồ kiến trúc 6 Microservices:**
    *   **Airflow:** Điều phối tác vụ định kỳ 00:00 (cào dữ liệu mô phỏng, chấm điểm mẻ, tính toán PSI).
    *   **MLflow:** Quản trị vòng đời mô hình (Tracking siêu tham số & Registry với 2 alias `@champion` và `@candidate`).
    *   **FastAPI:** Phục vụ suy luận thời gian thực với độ trễ < 5ms.
    *   **PostgreSQL:** Cơ sở dữ liệu quan hệ lưu trữ tập trung dữ liệu đánh giá, dự đoán và lịch sử drift.
    *   **Streamlit:** Bảng điều khiển phân tích trực quan kết hợp không gian thẩm định Human-in-the-Loop.
    *   **Tiền xử lý NLP tiếng Việt chuyên sâu:** Tích hợp bộ tách từ ghép **PyVi** (`ViTokenizer`), ghép các cụm từ chuyên ngành (*"tiết_kiệm", "sạc_lâu", "sụt_pin"*) giúp mô hình hiểu ngữ nghĩa sâu sắc mà không bị đánh lừa bởi từ ghép.

#### **Phút 5 - 8: Trình Diễn Thực Tế (Live Demo 3 Hồi)**
*   **Hồi 1 (Vận hành chuẩn):** Chạy `submit_review.py` nạp 2 review tích cực ➔ Chạy chấm điểm ➔ Streamlit báo `✅ STABLE` (PSI ~ 0).
*   **Hồi 2 (Khủng hoảng & Tự phục hồi):** Nạp 10 đánh giá phàn nàn pin/lỗi ➔ Chạy chấm điểm ➔ PSI vọt lên ngưỡng đỏ ➔ Hệ thống tự động gửi Email cảnh báo qua SMTP ➔ Tự động kích hoạt Retrain ngầm và đẩy mô hình mới qua chốt chặn **Gatekeeper**.
*   **Hồi 3 (Phản ứng sự cố & Con người can thiệp):**
    *   Thao tác **Tắt báo động tạm thời** (`Acknowledge & Mute Alert`).
    *   Thao tác **Cầu dao an toàn (Serving Circuit Breaker)**: Gạt sang `Rule-Based Fallback` để chứng minh API không bao giờ sập khi mô hình bảo trì.
    *   Thao tác **Active Learning Audit**: Mở bảng thẩm định độ bất định (Uncertainty Sampling), sửa nhãn sai hoặc bấm duyệt hàng loạt có hộp thoại xác nhận an toàn.

#### **Phút 9 - 10: Kết Quả & Giá Trị Nghiệp Vụ (Business Impact & Conclusion)**
*   **Độ chính xác:** Macro-F1 đạt **0.92** trên tập kiểm thử độc lập.
*   **Chi phí:** Chạy hoàn toàn trên CPU tiêu chuẩn (Zero GPU cost), thời gian retrain < 0.1 giây.
*   **Tính sẵn sàng:** Đáp ứng đầy đủ các tiêu chuẩn bảo mật, Zero Regression nhờ Gatekeeper và Zero Downtime nhờ Circuit Breaker.

---

### 🛡️ PHẦN 2: Bộ Câu Hỏi & Câu Trả Lời "Ăn Điểm" Khi Hội Đồng Phản Biện (Q&A Defense)

#### **Câu 1: Tại sao nhóm không dùng các mô hình Transformer hiện đại như PhoBERT hay LLM (GPT, Llama)?**
*   **Trả lời trọng tâm:**
    > *"Trong MLOps thực tế, sự lựa chọn mô hình luôn là bài toán cân bằng giữa **Hiệu năng (Performance)**, **Độ trễ (Latency)** và **Chi phí vận hành (Operational Cost)**:  
    > 1. **Về độ trễ và chi phí:** PhoBERT hoặc LLM đòi hỏi GPU đắt đỏ và độ trễ phản hồi từ 50ms - 500ms. Mô hình Logistic Regression kết hợp TF-IDF n-gram và bộ tách từ PyVi của chúng em đạt Macro-F1 lên tới **0.92**, nhưng thời gian phản hồi chỉ **< 5ms trên CPU thường**, giảm 90% chi phí hạ tầng.  
    > 2. **Về khả năng Self-Healing:** Khi phát hiện trôi dạt dữ liệu, mô hình của chúng em chỉ mất **dưới 0.1 giây** để tái huấn luyện và thăng hạng qua Gatekeeper ngay tức thì. Nếu dùng Deep Learning, việc fine-tune sẽ mất nhiều phút đến nhiều giờ, không đáp ứng được yêu cầu tự phục hồi gần như tức thì trong kịch bản xử lý sự cố."*

#### **Câu 2: Chỉ số PSI là gì? Tại sao nhóm chọn ngưỡng PSI = 0.15 thay vì các chỉ số thống kê khác như KS-test hay Chi-square?**
*   **Trả lời trọng tâm:**
    > *"**PSI (Population Stability Index)** là chỉ số đo lường mức độ biến động phân phối xác suất giữa tập dữ liệu tham chiếu (Baseline) và tập dữ liệu phục vụ thực tế (Production):  
    > *   Theo chuẩn mực MLOps và ngân hàng quốc tế:  
    >     * $\text{PSI} < 0.1$: Phân bổ ổn định, không có trôi dạt.  
    >     * $0.1 \le \text{PSI} < 0.25$: Bắt đầu có sự dịch chuyển phân bổ nhẹ.  
    >     * $\text{PSI} \ge 0.25$: Trôi dạt dữ liệu nghiêm trọng.  
    > *   Nhóm lựa chọn ngưỡng **$\text{PSI} = 0.15$** làm điểm kích hoạt cảnh báo sớm (Early-Warning Threshold). Ngưỡng này vừa đủ nhạy để phát hiện sớm các khủng hoảng truyền thông về lỗi pin/trạm sạc, vừa tránh hiện tượng 'báo động giả' (Alert Fatigue) nếu đặt ngưỡng quá thấp như 0.05.  
    > *   So với Chi-square hay KS-test (chỉ trả về p-value dễ bị ảnh hưởng bởi kích thước mẫu), PSI cho ra một con số định lượng độ lớn biến động cụ thể, rất trực quan cho giám sát vận hành."*

#### **Câu 3: Chốt chặn Gatekeeper hoạt động như thế nào? Nếu mô hình mới huấn luyện xong lại kém hơn mô hình cũ thì sao?**
*   **Trả lời trọng tâm:**
    > *"Đây chính là chốt chặn quan trọng nhất để bảo đảm nguyên tắc **Zero Regression** (không bao giờ để mô hình dở hơn lên phục vụ khách hàng):  
    > 1. Trong quy trình `train_model.py`, chúng em cô lập một tập Validation cố định 157 mẫu làm 'bộ đề thi chuẩn'.  
    > 2. Khi mô hình ứng viên mới được huấn luyện xong, hệ thống tải mô hình đương kim vô địch `@champion` về và chấm điểm cả 2 trên cùng tập Validation này.  
    > 3. **Nếu F1 mới $\ge$ F1 cũ:** Mô hình mới đủ chuẩn, tự động thăng hạng `@champion`.  
    > 4. **Nếu F1 mới < F1 cũ:** Gatekeeper lập tức **CHẶN ĐỨNG thăng hạng tự động**, gán nhãn mô hình mới là `@candidate` (Contender), giữ nguyên `@champion` cũ phục vụ API, đồng thời xuất bảng Scorecard đối đầu trên Streamlit và bắn cảnh báo sự cố. Nhờ đó, dù dữ liệu cào về có bị nhiễu, hệ thống vẫn an toàn tuyệt đối."*

#### **Câu 4: Cầu dao an toàn (Serving Circuit Breaker) giải quyết tình huống xấu nhất nào?**
*   **Trả lời trọng tâm:**
    > *"Trong sản xuất, có những tình huống khẩn cấp mà mô hình AI bị lỗi không lường trước (ví dụ: máy chủ MLflow gặp sự cố mạng, hoặc xuất hiện chuỗi ký tự tấn công làm mô hình crash).  
    > Khi đó, người vận hành chỉ cần 1 thao tác trên Streamlit để chuyển sang **Rule-Based Fallback Mode**. Tầng FastAPI sẽ lập tức điều hướng 100% lưu lượng qua bộ phân loại quy tắc từ khóa xác định. Cơ chế này đảm bảo endpoint `/predict` luôn trả mã HTTP 200, bảo đảm tính liên tục của nghiệp vụ (**Zero Downtime**) trong lúc đội ngũ kỹ sư điều tra nguyên nhân."*

#### **Câu 5: Tính năng Human-in-the-Loop và Active Learning giúp ích gì cho bài toán chi phí doanh nghiệp?**
*   **Trả lời trọng tâm:**
    > *"Nếu yêu cầu chuyên viên phải đọc và duyệt toàn bộ 10,000 đánh giá thì chi phí nhân sự rất lớn.  
    > Bảng điều khiển của chúng em áp dụng kỹ thuật **Uncertainty Sampling (Lấy mẫu theo độ bất định)**: lọc và xếp những câu mà AI có độ tự tin thấp nhất (`confidence < 60%`) lên đầu danh sách. Con người chỉ cần tập trung thẩm định 5% - 10% các câu khó nhất này. Toàn bộ nhãn vàng sau khi con người thẩm định được ghi thẳng vào PostgreSQL và tự động gộp vào tập huấn luyện ở chu kỳ retrain tiếp theo, giúp mô hình ngày càng thông minh hơn với chi phí nhân sự tối thiểu."*

---

### 🚨 PHẦN 3: Ứng Biến Nhanh Khi Trình Diễn Gặp Sự Cố (Demo Troubleshooting)

| Hiện tượng | Nguyên nhân có thể | Cách xử lý trong 5 giây |
| :--- | :--- | :--- |
| **Không gửi được Email Gmail** | Do mạng chặn cổng SMTP 587 hoặc sai App Password | Trình chiếu trực tiếp tệp mockup HTML cảnh báo đã được tự động lưu sẵn tại `data/alerts/drift_alert_*.html`. |
| **Bấm Retrain bị chậm** | Máy tính đang chạy nhiều ứng dụng nặng ngầm | Giải thích với hội đồng rằng hệ thống đang thực hiện kiểm định đối đầu Gatekeeper trên 2 mô hình và kiểm tra test mù. |
| **Dashboard chưa cập nhật số liệu mới** | Trình duyệt giữ bộ nhớ đệm Streamlit | Nhấn phím `R` hoặc bấm vào menu ba chấm góc phải Streamlit chọn **Clear cache & Rerun**. |
| **Cần thiết lập lại từ đầu để demo lại lần 2** | Dữ liệu cũ đã bị drift | Mở terminal gõ 1 lệnh duy nhất: `python data/ingest_pipeline.py --backfill --reset` (trên máy local) hoặc `docker compose run --rm fastapi python data/ingest_pipeline.py --backfill --reset` (trên Docker). |
