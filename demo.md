# 🎬 MLOps Live Presentation & Demo Guide (100% Interactive & Local)

This guide outlines the step-by-step storyboard and script to demonstrate your MLOps Capstone project.

The entire system is **100% offline, lightning-fast, and relies entirely on user input from your custom shop interface**.

---

## 🛠️ Step 0: Pre-Demo Setup & Reset (Run Before Presenting)

Ensure your servers are fully stopped (press `Ctrl + C` in any open terminal). Then, open a fresh terminal and run:

```bash
# 1. Reset results DB and backfill 25 days of stable historical reviews (Generated locally!)
python data/ingest_pipeline.py --backfill

# 2. Start your local serving servers cleanly (FastAPI & Streamlit)
python run_local.py
```
*   **Browser Preparation:** Open these ports: Streamlit (`:8501`), FastAPI (`:8000/docs`), MLflow (`:5000`).

---

## 🎭 Act I: Stable Operations & Live Customer Submission

### **What to do:**
1.  **Submit a Review (Interactive Loop):** Open a new terminal window and run our continuous storefront submitter app:
    ```bash
    python data/submit_review.py
    ```
    *   **STEP 1:** Press **Enter** to accept the default date (today: `2026-09-04`).
    *   **STEP 2:** Enter a standard positive review:
        *   `Review #1 Text:` $\rightarrow$ Type: `"Amazing experience! Great product quality and fast shipping."`
        *   `Category Choice:` $\rightarrow$ Type `3` (electronics) or press **Enter**.
    *   **STEP 3:** Submit another review:
        *   `Review #2 Text:` $\rightarrow$ Type: `"Its okay, pretty standard item."`
        *   `Category Choice:` $\rightarrow$ Press **Enter** to default to `other`.
    *   **STEP 4:** Finish and Exit:
        *   `Review #3 Text:` $\rightarrow$ Simply press **Enter** (or type `exit`).
    *(Both reviews are now transactionally saved in our shop database table `store_reviews` as `is_processed = 0`)*.

2.  **Run Ingestion:** In your terminal, process today's pending customer reviews:
    ```bash
    python data/ingest_pipeline.py --ingest-daily --date 2026-09-04
    ```
    *(The pipeline automatically finds your 2 pending reviews, runs batch scoring, and locks their state by marking them `is_processed = 1`)*.

3.  **UI Verification:** Go to **Streamlit** (`http://localhost:8501`) and refresh.
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
    *   **STEP 2 (Continuously Enter 10 Negative/Drifted Reviews):** To trigger drift, we will submit a highly-skewed batch of Spanish/weird negative complaints! Type each review followed by **Enter** (press **Enter** again to accept the default category):
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
    *(The pipeline scores the 10 reviews. Because 100% of tomorrow's reviews are negative/drifted, the prediction distribution shifts heavily. PSI spikes to `~0.88`—well above the `0.15` safety threshold!)*

3.  **UI & Self-Healing Verification:** 
    *   **F5 Streamlit:** Watch the dashboard status instantly flip to a flashing red **`⚠️ DRIFT ALERT`**! The PSI Score plot spikes into the warning zone.
    *   **Alert Generation:** Show the local HTML email generated at `data/alerts/drift_alert_2026_09_05.html` in your browser.
    *   **Self-Healing Log:** Look at your server console. Show the judges how the system **automatically detected the drift, launched training, evaluated the candidate model, registered version, and promoted it to `@champion` completely hands-free!**
        `🚨 [SELF-HEALING] Data Drift Detected! Triggering automated retraining...`
        `🚨 [SELF-HEALING] Retraining completed successfully! Model updated to @champion.`

---

## 🔄 How to Repeat the Demo
To reset everything back to the beginning for another presentation, simply stop your local servers (`Ctrl + C` in the `run_local.py` terminal) and run **Step 0** again:
```bash
python data/ingest_pipeline.py --backfill
python run_local.py
```
This is fully idempotent, robust, and can be repeated infinite times!

