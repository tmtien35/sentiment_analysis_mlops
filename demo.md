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

# 6. Unpause the Airflow DAG for Automated Daily Ingestion & Scoring
docker compose exec airflow-webserver airflow dags unpause daily_sentiment_analysis
```
*   **Websites to Open:** Replace `localhost` with your **`IP_Google_Cloud`** (e.g., `http://[IP_Google_Cloud]:8501`, `http://[IP_Google_Cloud]:5000`, `http://[IP_Google_Cloud]:8080`).
*   *(Note: Step 6 unpauses the Airflow DAG so reviews are automatically processed each midnight. You can also toggle it on/off in the Airflow Web UI at `http://[IP_Google_Cloud]:8080`)*.

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
   * To approve AI predictions in 1-Click: If the remaining AI predictions are correct, simply click **`✅ Bulk Approve Remaining AI Predictions`**!
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
If you make code, dashboard, or design updates on your laptop, push them to GitHub, and pull them on your Google Cloud VM, simply run this single command. Docker Compose V2 will hot-recreate only the modified container services in 2 seconds while preserving 100% of your persistent PostgreSQL history, predictions, and drift logs:
```bash
git pull && docker compose up -d --build
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

# 5. Bring serving servers back online and unpause DAG
docker compose up -d --build && \
docker compose exec airflow-webserver airflow dags unpause daily_sentiment_analysis
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
