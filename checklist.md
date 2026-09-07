# MLOps Capstone Project: E-Commerce Review Sentiment Analysis Checklist

This checklist outlines the step-by-step implementation of the E-Commerce Review Sentiment Analysis MLOps Pipeline. 
To ensure we build a robust system, **Model Evaluation and Champion Selection are performed first (Phase 2)**. Deployment, orchestration, serving, and automation are built only after the winning model is selected and registered.

---

## Phase 1: Environment & Training Data Preparation
- [x] **1.1 Set up Repo Scaffolding**
  - Create the folder structure:
    ```
    ecommerce-sentiment-mlops/
    ├── data/
    │   └── incoming/
    ├── ml/
    ├── airflow/
    │   └── dags/
    ├── dashboard/
    ├── api/
    └── .github/
        └── workflows/
    ```
- [x] **1.2 Configure Dependencies**
  - Create a `requirements.txt` with exact pinned versions:
    - `scikit-learn`
    - `pandas`
    - `mlflow`
    - `apache-airflow`
    - `streamlit`
    - `fastapi`
    - `uvicorn`
    - `psycopg2-binary`
    - `sqlalchemy`
    - `pytest`
    - `flake8`
  - Install dependencies locally to verify compatibility with Python 3.12.
- [x] **1.3 Fetch & Split Primary Training Data**
  - Programmatically download `IberaSoft/ecommerce-reviews-sentiment` (20,000 rows) from Hugging Face.
  - Handle SSL bypass context if required for local network compatibility.
  - Perform a **stratified split** (e.g., 80/10/10) using a fixed seed (`random_state=42`) to maintain class balance:
    - `data/train_v1.csv` (16,000 rows)
    - `data/val_v1.csv` (2,000 rows)
    - `data/test_v1.csv` (2,000 rows)
  - Generate and log a SHA-256 hash of the dataset files to enforce lightweight data versioning.
- [x] **1.4 Create the Complementary Daily Feed Simulator**
  - Write `data/generate_reviews.py` to synthesize **unlabeled** e-commerce product reviews for daily batches (~400–600 rows spread across 20–30 simulated days).
  - Script a deliberate **vocabulary shift** starting from Day 15 (introducing new features/slang/complaints to simulate drift).
  - Save a hidden ground-truth file (`data/incoming_ground_truth.csv`, inaccessible to the Airflow DAG) to compute accuracy on slides/presentation only.

---

## Phase 2: Model Testing, Experiment Tracking & Champion Selection 🎯
> **Goal:** Run a transparent model bake-off, track metrics, and select/register the winner *prior* to any deployment.

- [x] **2.1 Implement Shared Text Preprocessing**
  - Write `ml/preprocess.py` containing a clean-text utility (e.g., removing HTML tags, lowercasing, stripping excessive punctuation) that will be shared between training and serving to avoid training-serving skew.
- [x] **2.2 Configure Local MLflow Server**
  - Initialize MLflow tracking locally.
  - Configure the tracking URI and create a new experiment named `ecommerce-sentiment-analysis`.
- [x] **2.3 Create Model Training and Logging Pipeline**
  - Write `ml/train_model.py` to:
    - Load `data/train_v1.csv` and `data/val_v1.csv`.
    - Apply TF-IDF Vectorizer (`max_features=5000`, unigrams + bigrams) fitted on the training split only.
    - Train and evaluate 4 candidate models:
      1. **Logistic Regression** (baseline, fast, highly interpretable)
      2. **Linear SVM** (`SGDClassifier(loss="log_loss")` or calibrated `LinearSVC`)
      3. **Multinomial Naive Bayes** (highly efficient text classifier)
      4. **Random Forest** (ensemble tree-based baseline, depth-capped)
    - Track and log the following to MLflow for each model run:
      - Parameters (regularization, vectorizer settings, etc.)
      - Training metrics (Accuracy, Macro-F1, Precision, Recall)
      - Validation metrics (Accuracy, Macro-F1)
      - Confusion Matrix plot as an MLflow artifact
      - Data version tag (SHA-256 hash)
- [x] **2.4 Execute Bake-off & Evaluate Leaderboard**
  - Run `python ml/train_model.py`.
  - Open the MLflow UI (`mlflow ui` at `http://localhost:5000`) and compare metrics.
  - Confirm that the best model is identified using **Macro-F1 score** on the validation set.
- [x] **2.5 Register the Winning Model**
  - Programmatically register the winning model in the MLflow Model Registry under the name `ecommerce-sentiment-model`.
  - Log and report the winner's performance on the unseen `data/test_v1.csv` partition **exactly once** to avoid test-set leakage.
- [x] **2.6 Promote to Champion**
  - Assign the **`champion`** tag/alias to the registered winning version in MLflow.
  - Ensure the alias assignment is fully automated within the training script.

---

## Phase 3: Real-Time Serving & Batch Orchestration Deployment
> **Goal:** Build the infrastructure that serves the registered champion model in real-time and scores daily incoming batches.

- [x] **3.1 Build the FastAPI Real-time Serving Endpoint**
  - Write `api/main.py` creating a lightweight API using FastAPI.
  - Implement a `POST /predict` route that:
    - Loads the registered model directly from MLflow using the `models:/ecommerce-sentiment-model@champion` URI at startup.
    - Processes incoming single-review text, runs inference, and returns predicted sentiment (positive, neutral, negative) and confidence score.
- [x] **3.2 Establish the Batch Orchestration Pipeline (Airflow DAG)**
  - Create `airflow/dags/daily_sentiment_dag.py` scheduled to run `@daily` with `catchup=True` (starting ~20 days ago to simulate historical data).
  - Implement 6 DAG tasks (using PythonOperators):
    1. **`ingest_new_reviews`**: Calls the generator to fetch today's unlabeled reviews and save to `data/incoming/`.
    2. **`preprocess`**: Applies the shared text cleaning from `ml/preprocess.py` on the ingested batch.
    3. **`load_model_and_predict`**: Pulls the model tagged `@champion` from MLflow and generates predictions + confidence scores.
    4. **`compute_proxy_drift`**: Measures input drift (e.g., Population Stability Index (PSI) or vocabulary overlap) of today's batch vs. the baseline training set, along with average prediction confidence.
    5. **`upsert_to_postgres`**: Writes predictions + drift metrics to a Postgres database. Must use an **idempotent write operation** (e.g., `INSERT ... ON CONFLICT DO UPDATE` or delete-before-insert scoped to execution date) to prevent double-counting on retries.
    6. **`log_run_to_mlflow`**: Logs a daily DAG execution run to MLflow tracking (recording row count, average confidence, drift alerts).

---

## Phase 4: Streamlit Analytics Dashboard & Containerization
- [x] **4.1 Create the Streamlit Dashboard**
  - Write `dashboard/app.py` reading live from the Postgres database.
  - Include the following visualizations:
    - Current day and cumulative sentiment breakdown (positive/neutral/negative pie/bar charts).
    - Daily sentiment volume and ratio trend line.
    - **Drift and Confidence Panel**: Visualizes PSI and prediction confidence trends over time (which will clearly trigger around Day 15 due to scripted drift).
    - Table of recent reviews with filter options by sentiment and search capabilities.
    - An on-demand single-review test section that sends requests to the FastAPI `/predict` endpoint and displays real-time predictions.
- [x] **4.2 Dockerize and Integrate the Stack**
  - Write a `Dockerfile` installing the pinned requirements.
  - Write `docker-compose.yml` defining 6 interconnected services:
    1. `postgres` (storing reviews, predictions, and drift logs)
    2. `mlflow` (experiment tracking + model registry server)
    3. `airflow-webserver` (UI for monitoring runs)
    4. `airflow-scheduler` (orchestrating the daily runs and drift monitoring)
    5. `streamlit` (business/analytics frontend)
    6. `fastapi` (live prediction API)
  - Configure port forwarding and persistent volumes for Postgres and MLflow artifacts.

---

## Phase 5: Testing, CI/CD, Documentation & Polish
- [x] **5.1 Setup GitHub Actions CI**
  - Write `.github/workflows/ci.yml` triggered on pushes/pull-requests to verify:
    - Code linting (via `flake8` or `ruff`).
    - Unit tests (testing `preprocess.clean_text` on known outputs, and a training dry-run on a tiny mock dataset).
    - Airflow DAG import-error checks (`airflow dags list-import-errors`) to catch syntax or import issues before deployment.
- [x] **5.2 Write README.md**
  - Design a readable document covering:
    - Project Architecture Diagram.
    - Prerequisites and exact `docker compose up --build` setup instructions.
    - How to trigger the Airflow DAG backfill.
    - URLs for Streamlit (`:8501`), FastAPI (`:8000/docs`), Airflow (`:8080`), and MLflow (`:5000`).
    - A **"Known Gaps"** section explaining out-of-scope items (e.g., ground-truth accuracy monitoring, canary/A-B testing, production secrets, autoscaling) to showcase MLOps maturity.
- [x] **5.3 Run Historical Backfill Demo**
  - Run the Airflow DAG backfill for the last 20 days.
  - Verify that no duplicate records exist in Postgres.
  - Verify that the Streamlit dashboard successfully populates 20 days of data and shows a visible drift signal at Day 15.
- [x] **5.4 Record Backup Video**
  - Record a 2–3 minute screen capture of the working system as insurance.

---

## 3-Step Manual Test for Choosing the Finalist (Non-Coder Verification)
To verify that the model testing and finalist selection worked successfully before any deployment, run these steps in your terminal and browser:
1. **Train & Log Models:** Run `python ml/train_model.py`. This trains all 4 models and records them.
2. **Launch MLflow UI:** Run `mlflow ui --port 5000` and open `http://localhost:5000` in your browser.
3. **Inspect the Leaderboard:** Click on the `ecommerce-sentiment-analysis` experiment. You will see a leaderboard comparing the 4 models side-by-side. The model with the highest validation Macro-F1 score is automatically registered as `ecommerce-sentiment-model` with the alias `@champion`.
