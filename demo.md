# 🎬 MLOps Live Presentation & Demo Guide (100% Interactive & Multi-Mode)

This guide outlines the step-by-step storyboard and script to demonstrate your MLOps Capstone project.

The entire system is **100% offline, lightning-fast, and relies entirely on user input from your custom shop interface**.

---

## 🛠️ Step 0: Pre-Demo Setup & Reset (Run Before Presenting)

You can present this demo in either of your two mutually exclusive execution modes:

### **Option A: Local Python Mode (Laptop Demo)**
Ensure your servers are fully stopped. Open a terminal and run:
```bash
# 1. Reset results DB and backfill 25 days of stable historical reviews (Offline local templates!)
python data/ingest_pipeline.py --backfill

# 2. Start your serving servers cleanly (FastAPI, Streamlit, and MLflow UI)
python run_local.py
```
*   **Websites to Open:** Dashboard (`http://localhost:8501`), Swagger Docs (`http://localhost:8000/docs`), MLflow (`http://localhost:5000`).

### **Option B: Google Cloud VM / Containerized Mode (Professional Cloud Demo)**
Open your GCP SSH Terminal and run this **foolproof 4-step deployment sequence**:
```bash
# 1. Clean up and completely reset any old database volumes
docker compose down -v

# 2. Bootstrap and train the initial champion model (Run once on fresh VM)
docker compose run --rm fastapi python ml/train_model.py

# 3. Pre-populate the 25-day historical database into PostgreSQL
docker compose run --rm fastapi python data/ingest_pipeline.py --backfill

# 4. Start all 6 containers running in the background vĩnh viễn!
docker compose up -d --build
```
*   **Websites to Open:** Replace `localhost` with your **`IP_Google_Cloud`** (e.g., `http://[IP_Google_Cloud]:8501`, `http://[IP_Google_Cloud]:5000`, `http://[IP_Google_Cloud]:8080`).

---

## 🎭 Act I: Stable Operations & Live Customer Submission

### **What to do:**
1.  **Submit a Review (Interactive Loop):** Open a new terminal window (or SSH session) and run our continuous storefront submitter app:
    ```bash
    python data/submit_review.py
    ```
    *   **STEP 1:** Press **Enter** to accept the default date (today: `2026-09-04`).
    *   **STEP 2 (Continuously enter reviews - Category is automatically assigned under-the-hood!):**
        *   `Review #1 Text:` $\rightarrow$ Type: `"Amazing experience! Great product quality and fast shipping."` $\rightarrow$ Press **Enter**.
        *   `Review #2 Text:` $\rightarrow$ Type: `"Its okay, pretty standard item."` $\rightarrow$ Press **Enter**.
        *   `Review #3 Text:` $\rightarrow$ Simply press **Enter** (leave empty) to finish and submit.
    *(Both reviews are now transactionally saved in your shop database table `store_reviews` with status `is_processed = 0`)*.

2.  **Run Ingestion:** In your terminal, automatically process and score all outstanding reviews:
    ```bash
    python data/ingest_pipeline.py --ingest-daily --date 2026-09-04
    ```
    *(The pipeline automatically finds your 2 pending reviews, runs batch scoring, and locks their state by marking them `is_processed = 1`)*.

3.  **UI Verification:** Open your **Streamlit** dashboard and refresh.
    *   Today's date appears on the daily volume chart.
    *   The status card is a healthy green **`✅ STABLE`** (PSI is ~0.0).
    *   Your manually entered reviews appear live in the **Scored Reviews Explorer** table!

---

## 🚨 Act II: The Production Crisis (Creating Manual Drift!)

### **What to do:**
1.  **Submit Drifted Reviews (Simulating a shipping & customer crisis):**
    Open your terminal and launch the submitter loop again for tomorrow's date:
    ```bash
    python data/submit_review.py
    ```
    *   **STEP 1 (Set tomorrow's date):** Type `2026-09-05` and press **Enter**.
    *   **STEP 2 (Continuously Enter 10 Negative/Drifted Reviews):** To trigger drift, we will submit a highly-skewed batch of Spanish/weird negative complaints! Type each review followed by **Enter**:
        1. `"¡Muy mal servicio! El producto llegó roto y muy tarde."`
        2. `"Terrible quality, payment crashed at checkout screen."`
        3. `"Order is stuck in the DelayGator warehouse loop!"`
        4. `"¡Pésima calidad, el soporte al cliente no responde!"`
        5. `"Absolutely awful experience, returning it immediately."`
        6. `"Everything broke on first use! Unbelievable."`
        7. `"¡No comprar! El artículo es completamente inútil."`
        8. `"Stuck in delivery pending loop for 10 days."`
        9. `"Extremely disappointed, a complete waste of money."`
        10. `"Worst customer service, rude and slow."`
    *   **STEP 3:** Leave the next review text empty and press **Enter** to submit all 10 reviews.

2.  **Process the Drifted Batch:** Run the daily pipeline for tomorrow's date:
    ```bash
    python data/ingest_pipeline.py --ingest-daily --date 2026-09-05
    ```
    *(The pipeline scores the 10 reviews. Because 100% of tomorrow's reviews are negative/drifted, the prediction distribution shifts heavily. PSI spikes to `~4.08`—well above the `0.15` safety threshold!)*

3.  **UI & Self-Healing Verification:** 
    *   **Refresh Streamlit:** Watch the dashboard status instantly flip to a flashing red **`⚠️ DRIFT ALERT`**! The PSI Score plot spikes into the warning zone.
    *   **Alert Generation:** Open the local HTML email generated at `data/alerts/drift_alert_2026_09_05.html` in your browser.
    *   **Self-Healing Log:** Look at your server console. Show the judges how the system **automatically automatically detected the drift, trigger retraining, and update the champion model, triggered retraining, read the drifted reviews directly from the SQL database, auto-labeled them, evaluated the candidate model, registered version, and promoted it to `@champion` completely hands-free!**
        `🚨 [SELF-HEALING] Data Drift Detected! Triggering automated retraining...`
        `🚨 [SELF-HEALING] Retraining completed successfully! Model updated to @champion.`

---

## 🛠️ Act III: Manual Incident Response & Overrides (Control Panel)

Show the judges how you can actively manage production alerts and bypass model predictions in real-time directly from your **Streamlit Sidebar Control Panel**:

### **1. Test the "Acknowledge & Mute Alert" (Tắt báo động):**
1.  Under **MLOps Incident Control Panel** in the sidebar, click the **`Acknowledge & Mute Alert`** button.
2.  **Result:** Streamlit immediately updates. The flashing red **`⚠️ DRIFT ALERT`** status card flips into a calm, yellow **`⚠️ DRIFT MUTED`**, proving you've acknowledged the flash sale / event and silenced the alarm cleanly in PostgreSQL!

### **2. Test the "Trigger Retrain Manual" (Cưỡng bức học máy):**
1.  In the sidebar, under **Continuous Training**, click the **`Trigger Retrain Manual`** button.
2.  Watch the spinner run. In under 10 seconds, it will complete and pop a green:
    `🏆 Model Retrained Successfully! New @champion promoted.`
3.  **Result:** The background system executed `ml/train_model.py`, registered a brand new model version (e.g. Version 11), and automatically pointed the `@champion` alias to it! You can verify this Version increase live on **MLflow** (`http://[IP_Google_Cloud]:5000`).

### **3. Test the "Switch to Fallback Rules" (Gạt cầu chì ngắt AI - Circuit Breaker):**
Let's simulate a situation where your AI model behaves erratically, and you need to bypass it instantly to ensure business continuity.
1.  In the sidebar, click the **Active Serving Mode** dropdown and change it from `Machine Learning Model` to **`Rule-Based Fallback Rules`**.
2.  A yellow warning box appears: `🛡️ Safe-Mode Active: ML Model Bypassed!`.
3.  Go to **Live On-Demand Scoring**, type: `"The checkout payment-loop is crashing and poor DelayGator shipping is stuck!"` and click **Predict Sentiment**.
4.  **Result:** 
    *   It instantly returns **`NEGATIVE` (99.0% confidence)**.
    *   Scroll down to the **Live API Traffic Monitor** table. You will see that the logged record's `cleaned_text` has **`[RULE-BASED FALLBACK]`** appended to it!
    *   This proves that the serving API **completely bypassed the ML model** and ran your safe, deterministic rule-based fallback algorithm natively!
5.  Toggle the mode back to `Machine Learning Model` once you are done to re-enable your high-performing AI.

---

## 🔄 Presentation Rehearsal & Update Playbook

### **How to Update Code (Zero Data Loss - Standard Update):**
If you make code or design updates on your laptop, push them to GitHub, and pull them on your Google Cloud VM, simply run this single command. Docker Compose V2 will hot-recreate only the modified container services in 2 seconds while preserving 100% of your persistent PostgreSQL history, predictions, and drift logs:
```bash
git pull && docker compose up -d --build
```

### **How to Completely Reset & Re-rehearse (Wipe Data - Clean Slate):**
If you want to clear your persistent database (for another presentation or rehearsal) and backfill 25 days of stable historical data from scratch, run:
```bash
# 1. Stop containers and delete PostgreSQL volumes
docker compose down -v

# 2. Bootstrap model training
docker compose run --rm fastapi python ml/train_model.py

# 3. Seed 25-day historical backfill
docker compose run --rm fastapi python data/ingest_pipeline.py --backfill

# 4. Bring serving servers back online
docker compose up -d --build
```
This is fully idempotent, robust, and can be repeated infinite times!
