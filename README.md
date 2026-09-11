# 🛍️ E-Commerce Review Sentiment & Drift Monitor (MLOps Pipeline)

An end-to-end, reproducible MLOps pipeline for classifying e-commerce product reviews and monitoring data drift. Built with lightweight, highly efficient Scikit-Learn classifiers and managed through interactive containerized orchestration.

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

To deploy the production stack on any environment (such as a fresh local system or a clean Google Cloud Platform VM), execute this **foolproof 4-step sequence**:

#### **Step 1: Clean Up and Reset Existing Database Volumes**
Ensure that any legacy containers or corrupt database volumes are wiped completely:
```bash
docker compose down -v
```

#### **Step 2: Bootstrap and Train the Initial Model (Run Once)**
Since local SQLite model tracking (`mlflow.db`) is excluded by `.gitignore` to keep the repository lightweight, you must train and register your initial champion model once inside the container environment (this compiles and builds the image first to ensure the code is updated):
```bash
docker compose build fastapi && docker compose run --rm fastapi python ml/train_model.py
```
*(This command will compile requirements, train your 4 model candidates on the training partition, select the best model based on Validation Macro-F1, and register it as `@champion` in under 15 seconds!)*

#### **Step 3: Pre-populate the 25-Day Historical Database**
Seed the PostgreSQL database with 25 days of stable, balanced historical data and run predictions with your new champion:
```bash
docker compose run --rm fastapi python data/ingest_pipeline.py --backfill
```

#### **Step 4: Launch the Entire Serving Stack in the Background**
Bring up all 6 containerized services (including Postgres, MLflow, Airflow, FastAPI, and Streamlit) running vĩnh viễn:
```bash
docker compose up -d --build
```
*This starts the complete ecosystem in the background: Postgres (port `5432`), MLflow (port `5000`), Airflow (port `8080`), FastAPI (port `8000`), and Streamlit (port `8501`).*

*   **The Auto-Backfill Magic:** On boot, the Airflow container automatically detects that PostgreSQL is empty and instantly backfills all 25 days of reviews, populating your database and dashboard with rich history automatically!
*   **Where to Open in Browser:**
    *   **Streamlit Analytics Dashboard:** [http://localhost:8501](http://localhost:8501) *(connected live to PostgreSQL)*
    *   **Orchestration UI (Airflow):** [http://localhost:8080](http://localhost:8080) *(Username: `mlops` \| Password: `mlops`)*
        *⚠️ IMPORTANT: New DAGs are paused by default in Airflow! To enable truly automated daily ingestion at midnight, you must UNPAUSE your DAG by running this command:*
        ```bash
        docker compose exec airflow-webserver airflow dags unpause daily_sentiment_analysis
        ```
        *Or by clicking the blue toggle switch next to `daily_sentiment_analysis` inside the Airflow Web UI!*
    *   **Experiment Registry (MLflow):** [http://localhost:5000](http://localhost:5000)
    *   **On-Demand Serving (FastAPI Docs):** [http://localhost:8000/docs](http://localhost:8000/docs)
*   **How to Stop (Keep Data):** To cleanly stop all background containers while preserving your persistent database history, run:
    ```bash
    docker compose down
    ```

*   **How to Update Code (Zero Data Loss - Standard Update Playbook):**
    Each time you push code updates to GitHub and pull them on your GCP VM, simply run this single command. Docker Compose V2 will hot-recreate only the changed services in 2 seconds while preserving 100% of your persistent PostgreSQL history, users, predictions, and drift metrics:
    ```bash
    git pull && docker compose up -d --build
    ```

*   **How to Completely Reset & Re-rehearse (Wipe Data - Clean Slate Setup):**
    If you want to wipe all persistent database history (for another rehearsal or presentation) and backfill 25 days of stable historical data from scratch, run:
    ```bash
    # 1. Stop containers and delete PostgreSQL volumes
    docker compose down -v
    
    # 2. Build the fastapi image first to ensure all code is updated
    docker compose build fastapi
    
    # 3. Bootstrap and train the initial model (Run once on fresh database)
    docker compose run --rm fastapi python ml/train_model.py
    
    # 4. Seed the 25-day historical backfill into Postgres
    docker compose run --rm fastapi python data/ingest_pipeline.py --backfill
    
    # 5. Bring the serving servers back online
    docker compose up -d --build
    
    # 6. Unpause the Airflow DAG for Automated Ingestion
    docker compose exec airflow-webserver airflow dags unpause daily_sentiment_analysis
    ```

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
3.  **Train & Select Champion:** `python ml/train_model.py` *(Runs the automated model retraining, registers Version, and promotes to `@champion`)*.
4.  **Test API Locally:** `python api/main.py` *(Launches FastAPI on `:8000`)*.
5.  **Run Quality Assurances:** `python -m pytest ml/test_pipeline.py` *(Runs lint and structural syntax checks)*.

---

## 🌟 Advanced Production Features (MLOps Maturity Level Up)

While standard academic projects stop at basic drift detection, this production-ready pipeline implements advanced enterprise-grade features:

*   **Active Learning & Human-in-the-Loop Audit (Ground-truth Feedback Loop):** Operators can audit model predictions in bulk directly on the Streamlit dashboard using an interactive, spreadsheet-like grid (`st.data_editor`). Features a smart **1-Click Bulk Approval** mechanism that clones predictions into human-verified ground-truth labels. The stateless retraining loop (`train_model.py`) natively scans `store_reviews` for these human overrides (`verified_sentiment IS NOT NULL`), merges them as gold training labels, and expands the model's vocabulary dynamically!
*   **Dual-Model Canary Splitting:** We map `@champion` and `@contender` model aliases in MLflow. When a candidate model is registered, an interactive Canary Traffic Split slider (0-100%) appears on the Streamlit sidebar, allowing operators to direct a randomized percentage of live API traffic to the challenger while prefixing logs (`[CANARY RUN]` vs `[CHAMPION RUN]`) to safely validate performance before full promotion.
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
