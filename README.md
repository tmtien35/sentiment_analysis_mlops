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
Since local SQLite model tracking (`mlflow.db`) is excluded by `.gitignore` to keep the repository lightweight, you must train and register your initial champion model once inside the container environment:
```bash
docker compose run --rm fastapi python ml/train_model.py
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
    
    # 2. Bootstrap and train the initial model (Run once on fresh database)
    docker compose run --rm fastapi python ml/train_model.py
    
    # 3. Seed the 25-day historical backfill into Postgres
    docker compose run --rm fastapi python data/ingest_pipeline.py --backfill
    
    # 4. Bring the serving servers back online
    docker compose up -d --build
    ```

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

## 🏆 Offline Model Selection Leaderboard

During our offline experiment tracking phase, we conducted a multi-model bake-off logged to MLflow using a balanced, stratified split of 9,000 product reviews.

| Model Candidate | Validation Macro-F1 (Selection Metric) | Selection Status |
| :--- | :---: | :---: |
| **Multinomial Naive Bayes** | **0.7604** | **🏆 Champion (Registered & Mapped to @champion)** |
| **Logistic Regression** | 0.7403 | Baseline |
| **Linear SVM** | 0.7393 | Contender |
| **Random Forest** | 0.7055 | Contender |

*The winning Multinomial Naive Bayes pipeline was scored once on the unseen test split, achieving an unbiased **Test Accuracy of 75.67% and Macro-F1 of 0.7552**.*

---

### 🧠 Model Selection Rationale (Why Multinomial Naive Bayes?)

We chose **Multinomial Naive Bayes (MNB)** as our active `@champion` serving model based on four critical production and mathematical factors:

1. **Text Classification Excellence (High-Dimensional Suitability):** MNB is a probabilistic classifier based on Bayes' Theorem that thrives on sparse high-dimensional feature spaces, such as those generated by our TF-IDF vectorizer (5,000 features).
2. **Laplace Smoothing Robustness against Out-of-Vocabulary (OOV) Drift:** MNB applies Laplace smoothing (`alpha=1.0`). When customers submit reviews containing unfamiliar words, foreign text (such as Spanish words during our simulated drift event), or brand new vocabulary, Laplace smoothing prevents the model from assigning a $0$ probability to unseen words. MNB degrades gracefully under drift compared to complex linear models that can overfit to specific high-weight tokens.
3. **Computational and Serving Efficiency (Zero-Downtime, CPU-only):** Training takes less than $0.1$ seconds, and inference runs in sub-milliseconds on single-thread standard CPUs. This enables instant real-time serving on inexpensive hardware without requiring GPU resources.
4. **Stable and Non-Overfitting Retraining:** MNB is mathematically simple and has very few hyper-parameters. This makes it extremely robust and immune to parameter explosion, which is essential for our **automated, closed-loop, hands-free background retraining** triggered by data drift.

---

### 🔬 Evaluation Tests & Techniques Employed

To ensure complete fairness, scientific rigor, and prevent data leakage, we utilized the following methodologies:

*   **Stratified Holdout Testing:** We performed a **stratified split (80/10/10)** on our 20,000 IberaSoft e-commerce review dataset to preserve perfectly balanced Positive, Neutral, and Negative label ratios across training, validation, and testing partitions.
*   **Macro-F1 as the Selection Metric:** Since e-commerce sentiment data can suffer from category-specific distribution shifts, we selected **Macro-F1** (average of class-specific F1 scores) rather than basic Accuracy as our primary selection metric. This forces the model to perform highly on all three sentiment classes (Positive, Neutral, Negative) rather than biasing towards the majority class.
*   **Unbiased Test Set Verification:** The final champion was evaluated only once on the fully isolated, unseen test partition to obtain an unbiased indicator of real-world generalization.
*   **Confusion Matrix Diagnosis:** We utilized `ConfusionMatrixDisplay` to diagnose class-specific bottlenecks. This test confirmed that MNB maintains clean decision boundaries and handles the hard semantic boundaries between `neutral` and other classes highly effectively.
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

## 🧠 Scoped Gaps & Production Trade-offs (MLOps Maturity)

To maintain lightweight grading agility, several enterprise-level elements were consciously scoped out:

*   **Ground-truth accuracy monitoring:** Our pipeline detects *input drift* without labels (using PSI on predictions). Production environments require a feedback loop (sending a 5% sample of predictions to human labeling queues like AWS SageMaker Ground Truth).
*   **Canary/Shadow Deployments:** Model updates currently promote immediately via the `@champion` alias. Production systems require traffic-splitting proxies (like Seldon Core or BentoML) to run candidates in shadow mode.
*   **Autoscaling Infrastructure:** Docker Compose is suitable for single-host VM setups. High-throughput loads require Kubernetes (EKS/GKE) with Horizontal Pod Autoscalers (HPA).
*   **Production Secrets Management:** plain-text files are used for this demo. Enterprise platforms require secured secret key vaults (like HashiCorp Vault or AWS Secrets Manager).
