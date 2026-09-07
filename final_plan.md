# Capstone Project Plan: Scented Candle Review Sentiment Analysis (MLOps Pipeline)

## 0. TL;DR

- **Data:** Primary source = **IberaSoft/ecommerce-reviews-sentiment** (Hugging Face), 20,000 real, pre-labeled e-commerce/SaaS reviews with 3 balanced classes (Positive/Negative/Neutral). Trained on the full 20k since it costs almost nothing in time. A small synthetic "scented candle in HCMC" generator supplies the daily "incoming batch" that the Airflow DAG discovers and scores — **unlabeled**, like real scraped data would be, with a scripted vocabulary shift partway through to give the monitoring step something real to catch.
- **Model:** scikit-learn only, TF-IDF features feeding a small model bake-off — Logistic Regression, Linear SVM, Multinomial Naive Bayes, and (optionally) Random Forest — compared via MLflow, with the best by macro-F1 registered for serving.
- **Orchestration:** Apache Airflow DAG, scheduled daily, that (1) ingests new "scraped" reviews, (2) preprocesses, (3) runs inference with a registry-aliased model, (4) logs proxy drift signals, (5) idempotently writes results to a database.
- **Tracking/versioning:** MLflow (experiment tracking + model registry via **aliases**, not the deprecated stage labels) + lightweight dataset-hash logging for data versioning.
- **Serving/App:** Streamlit dashboard reading from the results database, refreshed daily by the DAG, plus a small FastAPI `/predict` endpoint for on-demand single-review scoring.
- **Packaging:** Docker Compose spinning up Airflow, Postgres, MLflow, and Streamlit together — a demo/dev setup, explicitly not claimed as production-grade.
- **CI:** A lightweight GitHub Actions workflow (lint + a couple of unit tests + Airflow DAG-import check) — real automation, distinct from the DAG itself.

Everything below is tiered into **must-fix** (cheap, protects the demo or fixes a real correctness issue), **worth adding if time allows**, and **explicitly out of scope** (named on purpose, not silently skipped) — see Section 9.

---

## 1. Dataset

### Primary training data: IberaSoft/ecommerce-reviews-sentiment (Hugging Face)
20,000 labeled customer reviews from e-commerce and SaaS platforms (Amazon, Yelp, G2, Capterra, TrustRadius), each tagged with one of three sentiment labels: **negative, neutral, positive**. Labels are derived from star ratings (1–2★ → negative, 3★ → neutral, 4–5★ → positive), with manual correction of edge cases (e.g., a 3-star review with glowing language moved to positive). Classes are reasonably balanced (~40% positive, 35% negative, 25% neutral), and it's public/documented, which matters for reproducibility.

Why this over the synthetic generator as the *training* set: it's real text with real label noise, which makes the "we evaluated multiple models and picked the better one" MLflow story more meaningful than it would be on cleanly rule-labeled synthetic data. And since training on all 20k takes well under a minute (see Section 10), there's no reason to subsample for speed — use the full dataset.

**Framing for the presentation:** "We use a public e-commerce sentiment corpus as a stand-in for scraped candle reviews, since the assignment explicitly allows a substitute dataset and our focus is the MLOps pipeline rather than domain-specific NLP." This is a legitimate, defensible design choice — say it plainly in Q&A rather than trying to disguise the data as candle-specific.

### Why not scrape real candle reviews live?
Scraping live e-commerce/Shopee/Facebook data is fragile (site changes, rate limits, login walls) and risky to depend on for a graded demo. So: **simulate the scrape** for the *daily* portion of the pipeline instead — this is realistic (many companies mock external data sources in dev/staging) and gives you full control over data arrival during the demo.

### Complementary piece: synthetic "scented candle reviews in HCMC" generator (for the daily feed only) — redesigned
An earlier draft of this generator had two problems, flagged correctly in review: (1) it produced *labeled* daily data, which is unrealistic — real scraped reviews wouldn't arrive pre-labeled, and having labels invited the pipeline to quietly claim "accuracy monitoring" that it can't actually do (see Section 4a). (2) it drew from the *same* vocabulary bank as training, so genuine drift could never occur — there was nothing for the monitoring step to catch.

Corrected design for `generate_reviews.py`:
1. Combines templates + vocabulary banks (scent notes: vanilla, sandalwood, lemongrass, ocean breeze; aspects: burn time, throw/scent strength, price, packaging, shipping in HCMC districts; brand name placeholders) into review sentences.
2. Outputs reviews **unlabeled** — only `review_text`, `review_date`, `brand`. The pipeline scores them; it never sees a ground-truth label for daily data, same as a real deployment would face.
3. Keeps a **hidden** ground-truth label per row in a separate file (`data/incoming_ground_truth.csv`, not read by the DAG) purely so you can compute a "what if we'd known" accuracy number for your own presentation slide — a nice thing to show in Q&A without pretending the pipeline itself does this.
4. **Scripts a deliberate vocabulary shift**: reviews for the first ~14 simulated days draw from the same style/vocabulary as the training data; from day ~15 onward, inject a batch of new aspect terms and phrasing not seen in training (e.g., sudden complaints about a specific shipping courier, a new scent line, slang not present in the training corpus). This gives the drift monitor in Section 4a something real to detect during the live demo, instead of a panel that never changes.

This is what the DAG ingests and scores daily and what populates the Streamlit dashboard's day-by-day trend. Suggested size: ~400–600 rows spread over ~20–30 simulated days (~20/day) is plenty for a convincing dashboard history and a clean before/after drift moment.

### Data versioning (lightweight, not full DVC)
Full DVC is real value but adds a new tool + more time risk than this project's budget supports. Cheap substitute that still counts as genuine data versioning: on every training run, log to MLflow as run tags — the **source dataset name/version** (`IberaSoft/ecommerce-reviews-sentiment`), a **hash of the exact `train.csv`/`val.csv`/`test.csv` used** (e.g., `hashlib.sha256` of the file bytes), and the row count. Save split files with a version suffix (`train_v1.csv`) rather than overwriting them in place. This gives you a real, checkable answer to "which data produced this model?" without adding DVC to the stack. Name DVC explicitly as an out-of-scope stretch in Section 9 rather than silently skipping it.

### Backup/alternative option (unverified — check before using)
A Kaggle dataset, *omjaiswal07/e-commerce-product-reviews-sentiment-dataset*, also turned up in research but its page is JS-rendered and didn't expose row counts, columns, or confirmed label set through search/fetch. Several similarly-named Amazon/Flipkart Kaggle datasets are binary-only (Positive/Negative), which wouldn't meet the 3-label requirement. If you want to consider it, open the Data tab on Kaggle yourself and confirm it has 3 real labels before relying on it — otherwise stick with IberaSoft.

---

## 2. Model

### Recommendation: TF-IDF features + a small bake-off of scikit-learn classifiers
- **Why this is the right call, not a compromise:** the rubric weights "MLOps practice applied" (3 pts) and "Presentation" (3 pts) far more than raw model sophistication, which isn't scored at all. A transformer (e.g., DistilBERT) adds GPU/dependency complexity, longer training/inference time, and heavier Docker images — all of which *hurt* your "Working demo" score (more things to break) for zero rubric benefit.
- **Why compare multiple models instead of just one:** this is exactly what MLflow experiment tracking is for, and it makes your "why this design?" answer much stronger in Q&A than "we picked the first thing that worked." All four candidates below train in seconds on the full 20,000-row dataset (see Section 10), so there's no time cost to running the full comparison.

**Candidates:**

| Model | Role | Notes |
|---|---|---|
| **Logistic Regression** | Primary baseline | Fast, interpretable (inspect top weighted words per class), gives calibrated-ish probabilities out of the box, tiny artifact. |
| **Linear SVM** (`LinearSVC`, or `SGDClassifier(loss="hinge")`) | Strong contender | SVMs are a classic go-to for high-dimensional sparse text features like TF-IDF and often match or beat Logistic Regression on this kind of task. `LinearSVC` has no `predict_proba`; if you want probability/confidence scores for the drift-monitoring dashboard panel, either use `SGDClassifier(loss="log_loss")` (SVM-equivalent but supports `predict_proba`) or wrap `LinearSVC` in `CalibratedClassifierCV`. |
| **Multinomial Naive Bayes** | Cheap baseline | Classic text-classification baseline; useful contrast case since its independence assumption tends to under-perform on reviews with negation/sarcasm — a good Q&A talking point. |
| **Random Forest** (optional 4th) | Different model family | Tree-based instead of linear; usually *not* the best fit for sparse high-dimensional TF-IDF, but including it and showing it gets outperformed makes your MLflow comparison look rigorous rather than cherry-picked. Keep `n_estimators` modest (e.g., 100–200) and cap `max_depth` to keep training fast and the artifact small. |

**Selection process:** stratified split so the ~40/35/25 class balance is preserved across train/val/test (`train_test_split(..., stratify=y, random_state=42)`), with a **fixed seed** stated in the plan and code so the split is reproducible. Train all candidates on the same TF-IDF features fit on train only, select the winner using val, and touch **test only once**, at the end, to report the final number — reusing test for model selection quietly inflates the reported score. Log accuracy + macro-F1 + confusion matrix per candidate to MLflow, then register the best-performing pipeline (by macro-F1, since classes aren't perfectly balanced) to the MLflow Model Registry as `candle-sentiment-model`.

**Model Registry note:** MLflow deprecated the old `Staging`/`Production`/`Archived` **stage** labels as of version 2.9 in favor of **model version aliases and tags** — using `stage="Production"` now triggers a deprecation warning and won't be supported in a future major release. Use an alias instead: `client.set_registered_model_alias("candle-sentiment-model", "champion", version)`, and load it for inference via `models:/candle-sentiment-model@champion`. Same concept (a stable pointer to "the model currently serving"), current API.

### Pipeline (repeated per candidate, same TF-IDF step)
```
raw_text -> clean_text() -> TfidfVectorizer(max_features=5000, ngram_range=(1,2))
         -> {LogisticRegression | LinearSVC/SGDClassifier | MultinomialNB | RandomForestClassifier}
```
Wrap vectorizer + model together in a single `sklearn.pipeline.Pipeline` per candidate so each is one self-contained, versionable artifact.

### Metrics to log
Accuracy, macro-F1 (important since classes aren't perfectly balanced), and a confusion matrix image — log all of these to MLflow per run, per candidate, so the registry ends up with a clear leaderboard.

### Reproducibility
- Set `random_state=42` (or any fixed value) on every split and every model that takes one — otherwise re-running training produces a slightly different model and your MLflow comparisons aren't apples-to-apples.
- Pin **exact** versions in `requirements.txt` (`scikit-learn==1.5.1`, not a bare `scikit-learn`) — floating versions are a common source of "works on my machine" failures right before a demo.
- Use `mlflow.sklearn.autolog()` where practical — it captures library/environment metadata per run with almost no extra code, which is most of what "model reproducibility" needs without building custom logging.

---

## 3. Should this be "an app"? — Yes, here's the shape

Build it as **three cooperating pieces, containerized together**:

1. **Airflow** — the daily automation brain (DAG).
2. **A results store** — Postgres (or even SQLite if you want to minimize moving parts) holding scored reviews + a `predictions` table + a `model_runs` table (metrics per day).
3. **Streamlit dashboard** — a thin read-only app that queries the results store and renders charts. It does **not** run the model itself; it just visualizes what the DAG produced. This separation is a real MLOps pattern (batch inference decoupled from serving/visualization) and is easy to explain in Q&A.
4. **A small FastAPI `/predict` endpoint** — loads the aliased model from the MLflow registry and scores a single review on demand. Given how cheap this is (roughly 30–45 minutes: one route, one Pydantic request model, one `model.predict`), it's worth including rather than treating as optional — it demonstrates real-time **deployment**, not just batch automation, which directly strengthens the MLOps criterion.

Package everything with **Docker Compose** so the whole demo is `docker compose up` → open a couple of browser tabs (Airflow UI, Streamlit dashboard, FastAPI docs). This directly serves the "Working demo — smooth" (2/2) bar. Be precise in the README and presentation about what this setup is: a **demo/dev packaging**, not a production deployment — real production would add secrets management (no plaintext DB credentials), health checks, resource limits, and separated dev/staging/prod environments, none of which Docker Compose alone provides. Naming this limitation explicitly is a stronger answer than letting a Q&A question expose it.

---

## 4. Architecture

```
                ┌─────────────────────────┐
                │  generate_reviews.py     │  (simulates scraping,
                │  writes new UNLABELED    │   unlabeled — see 4a)
                │  rows daily (+ scripted  │
                │  vocab shift ~day 15)    │
                └────────────┬─────────────┘
                             │
                             ▼
                 ┌────────────────────────┐
                 │   Airflow DAG (daily)  │
                 │ ──────────────────────│
                 │ 1. ingest_new_reviews  │
                 │ 2. preprocess_text     │
                 │ 3. load_model @alias   │  (MLflow: @champion, not "stage")
                 │ 4. predict_sentiment   │
                 │ 5. compute_proxy_drift │  (confidence + PSI, NOT accuracy)
                 │ 6. upsert_to_postgres  │  (idempotent — keyed on
                 │                        │   (review_id, batch_date))
                 │ 7. log_run_to_mlflow   │
                 └────────────┬───────────┘
                              │
                              ▼
                     ┌─────────────────┐
                     │   Postgres DB    │
                     │  predictions     │
                     │  model_runs      │
                     └────────┬─────────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │ Streamlit Dashboard│
                    │ - sentiment mix    │
                    │ - daily trend      │
                    │ - drift/confidence │
                    │ - sample reviews   │
                    └───────────────────┘

   ┌───────────────────────┐        (separate, offline)
   │  FastAPI /predict      │◄──── loads same aliased    ┌──────────────────────────┐
   │  (on-demand scoring)   │      model from registry   │ train_model.py            │
   └───────────────────────┘                             │ TF-IDF + LogReg/SVM/NB/RF │
                                                           │ logs metrics to MLflow    │
                                                           │ registers best (@champion)│
                                                           └──────────────────────────┘

   ┌───────────────────────────┐
   │  GitHub Actions CI         │  lint + unit tests (preprocess, train)
   │  (on push)                 │  + Airflow DAG import-error check
   └───────────────────────────┘
```

Draw this (or a cleaner version) as your architecture diagram for the presentation — it's worth 1.5 of the 3 presentation points on its own.

## 4a. Monitoring — corrected design

The earlier draft of this plan conflated two different things under "drift monitoring," which is worth getting right since it's a common real mistake:

- **True input drift** — whether today's incoming reviews look statistically different from the training distribution. Measurable without labels. Use a proper signal, not a vibe check: compute the **Population Stability Index (PSI)** (or a simpler proxy like average TF-IDF sparsity/vocabulary overlap) comparing today's batch against the training set, and track the **predicted-class distribution** (not the true class distribution, which you don't have) over time.
- **Accuracy / concept drift** — whether the model's predictions are actually getting *worse*. This **cannot be measured on the unlabeled daily batch** — there's no ground truth to compare against. Claiming to monitor "accuracy" on unlabeled data was a real error in the original plan, not just a simplification. Be upfront about this in the presentation: real systems solve this with a periodic human-labeled sample of production traffic (e.g., label 50 random daily predictions a week) — worth naming as the correct follow-up even if you don't build it (see Section 9).

So `compute_proxy_drift` should log: (1) **average prediction confidence** for the batch (a confident model suddenly hedging is a real signal), (2) **predicted-class distribution vs. training-class distribution** via PSI, flagged if it crosses a threshold. Because the generator now scripts a genuine vocabulary shift around day 15 (Section 1), this panel will show a real, visible change partway through your demo history — a much stronger Q&A moment than a static panel that never triggers.

---

## 5. Step-by-step setup

### Step 1 — Repo scaffolding
```
candle-sentiment-mlops/
├── README.md
├── Dockerfile
├── docker-compose.yml
├── requirements.txt          (pinned exact versions — see Section 2 reproducibility)
├── .github/
│   └── workflows/
│       └── ci.yml            (lint + unit tests + DAG import-error check)
├── data/
│   ├── generate_reviews.py
│   ├── train_v1.csv, val_v1.csv, test_v1.csv   (versioned splits from IberaSoft)
│   └── incoming/              (daily simulated, UNLABELED "scraped" files)
├── ml/
│   ├── train_model.py         (trains all candidates + logs to MLflow, registers best via alias)
│   └── preprocess.py          (shared text-cleaning function)
├── airflow/
│   └── dags/
│       └── daily_sentiment_dag.py
├── dashboard/
│   └── app.py                 (Streamlit)
├── api/
│   └── main.py                (FastAPI /predict endpoint)
├── mlflow/                    (mlflow server config / artifact store)
└── diagrams/
    └── architecture.png
```

### Step 2 — Get the training data + generate the daily feed
- Download the **IberaSoft/ecommerce-reviews-sentiment** dataset from Hugging Face (20,000 rows) into `data/train_raw.csv`. Do a **stratified** train/val/test split (e.g., 80/10/10) with a fixed `random_state` — save as `train_v1.csv` / `val_v1.csv` / `test_v1.csv`.
- Write `generate_reviews.py` as described in Section 1 to produce the **unlabeled** synthetic candle-review "incoming" pool as dated CSVs in `data/incoming/`, including the scripted vocabulary shift around day 15 (plus a separate, DAG-inaccessible `incoming_ground_truth.csv` for your own evaluation slide).

### Step 3 — Train the candidate models, log to MLflow
`ml/train_model.py`:
- Load `data/train_v1.csv` / `val_v1.csv` (the IberaSoft split).
- Fit TF-IDF on train only, then fit each candidate: Logistic Regression, Linear SVM (`SGDClassifier(loss="log_loss")` or `LinearSVC`+calibration), Multinomial Naive Bayes, and optionally Random Forest.
- For each: log params, accuracy, macro-F1, confusion matrix artifact to MLflow as a separate run, plus the dataset hash/version tag (Section 1) — this gives you a leaderboard in the MLflow UI.
- Select the winner by macro-F1 on val, report the final number on `test_v1.csv` **once**.
- Register the best-performing pipeline as `candle-sentiment-model` in the MLflow Model Registry and point the `champion` **alias** at it (not the deprecated `stage` field — see Section 2).

Run this manually first (`python ml/train_model.py`) to produce v1 of the model before wiring up Airflow — makes debugging much easier.

### Step 4 — Build the Airflow DAG
`airflow/dags/daily_sentiment_dag.py`, scheduled `@daily`, tasks (PythonOperators are fine, no need for anything fancier):
1. `ingest_new_reviews` — calls the generator for "today's" batch, saves to `data/incoming/`.
2. `preprocess` — cleans text, saves intermediate parquet/csv.
3. `load_model_and_predict` — loads `models:/candle-sentiment-model@champion` from the MLflow registry, scores the (unlabeled) batch.
4. `compute_proxy_drift` — PSI (or a simpler vocabulary-overlap proxy) of today's batch vs. training distribution, plus average prediction confidence. Logs a flag if either crosses a threshold. This measures **input drift**, not accuracy — see Section 4a for why accuracy can't be computed here.
5. `upsert_to_postgres` — writes predictions + drift stats keyed on `(review_id, batch_date)` using `INSERT ... ON CONFLICT DO UPDATE` (or delete-then-insert scoped to `{{ ds }}`). This makes the task **idempotent**: rerunning a DAG execution date (from a retry or backfill) doesn't create duplicate rows — worth calling out explicitly since it's an easy way to have a "smooth" demo silently produce doubled numbers on a rerun.
6. `log_run_to_mlflow` — logs a lightweight MLflow run per DAG execution (row count, drift flags) so you have a full audit trail of every day's batch, not just training runs.

Set `catchup=True` with a start date ~20 days ago so that when you demo, you can trigger a **backfill** and instantly populate 20 days of history — this makes the dashboard look "alive" without waiting 20 real days. Because writes are idempotent, re-triggering the backfill during rehearsal won't corrupt your data.

### Step 5 — Build the Streamlit dashboard + FastAPI endpoint
`dashboard/app.py`, connects to Postgres, shows:
- Sentiment distribution (pie/bar) for latest day and cumulative.
- Daily sentiment trend line (last N days).
- Proxy drift/confidence trend (ties back to Step 4, task 4 — should visibly shift around the scripted day-15 event).
- A filterable table of recent reviews with predicted label + confidence.
- A text box calling the FastAPI `/predict` endpoint for live single-review testing.

`api/main.py`: a single FastAPI route (`POST /predict`) that loads `models:/candle-sentiment-model@champion` once at startup and scores a submitted review text. Small, but it's the piece that demonstrates real-time deployment rather than only batch inference.

### Step 6 — Containerize
- `docker-compose.yml` services: `postgres`, `mlflow`, `airflow-webserver`, `airflow-scheduler`, `streamlit`, `fastapi`. Mount `./airflow/dags` and `./data` as volumes.
- One `Dockerfile` (or a couple of small ones) installing the **pinned** `requirements.txt`: `scikit-learn`, `pandas`, `mlflow`, `apache-airflow`, `streamlit`, `fastapi`, `uvicorn`, `psycopg2-binary`, `sqlalchemy`.
- `docker compose up` should bring up everything; Airflow UI on `:8080`, MLflow UI on `:5000`, Streamlit on `:8501`, FastAPI docs on `:8000/docs`.
- In the README, describe this explicitly as a demo/dev packaging (see Section 3) rather than implying it's production-ready.

### Step 7 — Add lightweight CI
`.github/workflows/ci.yml`, triggered on push: run `flake8`/`ruff` lint, a couple of `pytest` unit tests (e.g., `preprocess.clean_text` on known inputs, `train_model` runs end-to-end on a tiny fixture CSV), and `airflow dags list-import-errors` to catch a broken DAG file before it ever reaches the demo machine. This is maybe 1–2 hours of work and is a distinct, genuine piece of automation from the DAG itself — worth having as its own bullet in the MLOps criterion rather than folding it into "automation = Airflow."

### Step 8 — README + reproducibility polish
Write a README with: project summary, architecture diagram, exact `docker compose up` instructions, how to trigger the DAG manually/backfill, where to view the dashboard and call the FastAPI endpoint, how to retrain the model, and a short **"known gaps"** section (Section 9) so reviewers see you scoped these consciously. This directly covers the full 2 points for "Repo & reproducibility."

### Step 9 — Record a backup demo video
Even if you're confident in the live demo, record a 2–3 minute screen capture (compose up → Airflow DAG graph → trigger/backfill → Postgres rows appear → Streamlit dashboard updates → a live FastAPI `/predict` call) as insurance against the "smooth" vs "minor bugs" distinction in criterion #1.

---

## 6. Suggested team split (adjust to your team size)

| Role | Owns |
|---|---|
| Data & Modeling | `generate_reviews.py` (incl. scripted drift event), `train_model.py`, MLflow tracking/registry, reproducibility (seeds, pinned deps) |
| Orchestration | Airflow DAG, idempotent Postgres writes, proxy drift logic |
| App/Serving | Streamlit app, FastAPI `/predict` endpoint |
| DevOps/Docs | Docker Compose, Dockerfile, CI workflow, README, architecture diagram |

Everyone should be able to explain the *whole* flow end-to-end for the Q&A — the rubric explicitly checks that each member can explain their part **and** the overall "why this design."

---

## 7. Rubric alignment (so you can self-check before submitting)

| # | Criterion (pts) | How this plan covers it |
|---|---|---|
| 1 | Working demo (2) | Single `docker compose up`; idempotent DAG backfill instantly populates history without risk of duplicate data on rerun; dashboard + FastAPI both live; backup video recorded as insurance. |
| 2 | MLOps practice applied (3) | **Automation:** Airflow DAG (daily schedule + backfill) + a separate GitHub Actions CI workflow. **Tracking:** MLflow logs metrics/params for every candidate model and every daily DAG run, plus a dataset-hash tag for lightweight data versioning. **Versioning:** MLflow Model Registry using the current `@champion` alias API (not the deprecated stage labels). **Deployment:** FastAPI real-time endpoint + Airflow batch inference — both loading the same registered model. **Monitoring:** proxy drift (PSI + confidence) on unlabeled daily data, explicitly *not* claiming accuracy monitoring it can't do. You can articulate *why each matters* and *where the honest limits are* — that combination reads as more mature than a checklist with no caveats. |
| 3 | Repo & reproducibility (2) | README with run instructions + known-gaps section (1) + Dockerfile/docker-compose + pinned requirements.txt + CI (1). |
| 4 | Presentation & Q&A (3) | Architecture diagram from Section 4 (1.5) + clear "why this design" and "here's what we scoped out and why" answers below + each member owns and can explain a component (1.5). |

### Likely Q&A questions to prepare answers for
- "Why scikit-learn instead of a transformer?" → speed, simplicity, CPU-only, interpretability, and the rubric rewards MLOps maturity over model sophistication.
- "Why compare four models instead of just picking one?" → cheap to do (all four train in under a minute combined) and it's a genuine demonstration of experiment tracking, not just automation theater — the MLflow leaderboard is your evidence.
- "Why simulate scraping instead of real scraping?" → reliability for a graded, repeatable demo; real scraping is swappable later — only `ingest_new_reviews` would change.
- "Are you monitoring model accuracy in production?" → No, and say so directly: the daily batch is unlabeled, like real scraped data, so there's no ground truth to check against. What's monitored is *input drift* (PSI on the batch vs. training distribution, plus prediction confidence) — a real production system would close this gap with periodic human labeling of a small sample of live predictions.
- "What happens if a DAG run is retried or backfilled twice?" → Writes are idempotent (upsert keyed on `review_id` + `batch_date`), so reruns don't duplicate data — this was a deliberate fix, not an oversight.
- "Why decouple the dashboard from the model?" → batch inference and visualization scale and fail independently; the dashboard stays up even if a DAG run fails.
- "Is this production-ready?" → No, and that's fine to say plainly: Docker Compose is a demo/dev packaging. Name the real gaps (Section 9) rather than implying otherwise — that's a stronger answer than being caught overclaiming.

---

## 9. Explicitly out of scope (named on purpose, not silently skipped)

A capstone on this timeline can't cover every real-world MLOps concern, and pretending otherwise is worse than naming the gap. Keep this list handy for Q&A — "we scoped this out because X, and here's what we'd do next" is a strong answer:

| Gap | Why it's out of scope here | What "doing it right" would add |
|---|---|---|
| Full data versioning (DVC or similar) | New tool + setup/debug time not justified by rubric weight; the lightweight dataset-hash logging in Section 1 covers the reproducibility need cheaply | Full lineage tracking, ability to diff/rollback entire datasets, remote storage |
| Ground-truth accuracy monitoring | Requires a human-labeling process for a sample of production predictions, which is a workflow, not just a code change | A labeling queue + periodic accuracy report, feeding back into retrain triggers |
| Canary/shadow deployment, A/B testing of models | Needs traffic-splitting infrastructure well beyond a capstone's single FastAPI route | Safe rollout of new model versions with automatic rollback on regression |
| Secrets management, access control, environment separation (dev/staging/prod) | Docker Compose + plaintext `.env` is standard for a demo; real secrets management is its own infrastructure project | Vault/Secrets Manager, RBAC on the model registry, separate deployed environments |
| Kubernetes / autoscaling | Massive overkill for a batch job scoring a few hundred rows a day | Horizontal scaling for real production traffic volumes |

---

## 10. Time budget (rough, for a 1–2 week sprint)

| Task | Est. time |
|---|---|
| Download/split IberaSoft data (stratified) + build the synthetic candle-review generator (incl. scripted drift event) | 3–4 hrs |
| Train script (4 candidates) + MLflow tracking/registry (aliases) + dataset-hash logging | 2–3 hrs |
| Airflow DAG incl. idempotent upsert + proxy drift logic | 4–6 hrs |
| Streamlit dashboard | 3–4 hrs |
| FastAPI `/predict` endpoint | 0.5–1 hr |
| Lightweight CI (GitHub Actions: lint + tests + DAG import check) | 1–2 hrs |
| Docker Compose integration/debugging | 2–4 hrs |
| README (incl. known-gaps section), diagram, demo video, rehearsal | 2–3 hrs |
| **Total** | **~18–27 hrs** |

This is still a realistic scope for a 1–2 week team sprint — the added items (idempotency, CI, FastAPI, corrected monitoring) are all cheap individually; the discipline is in not also chasing the Section 9 out-of-scope items, which would blow the budget for no rubric benefit.

### Model training time on the full 20,000-row dataset
This is one of the practical benefits of sticking with scikit-learn instead of a transformer — training is essentially instant, even on the full dataset with no need to subsample for speed:

| Step | Time (typical laptop CPU) |
|---|---|
| Load 20,000 rows + text cleaning | ~1–3 sec |
| TF-IDF vectorization (`max_features=5000`, unigrams+bigrams) | ~2–5 sec |
| Fit Logistic Regression | ~2–10 sec |
| Fit Linear SVM (`SGDClassifier`/`LinearSVC`) | ~2–10 sec |
| Fit Multinomial Naive Bayes | <1 sec |
| Fit Random Forest (optional, ~150 trees, capped depth) | ~10–30 sec |
| **Total per `python ml/train_model.py` run (all 4 candidates)** | **~20–60 sec** |

Even adding 5-fold cross-validation or a small grid search over Logistic Regression's `C` is still well under a couple of minutes. Practical implications:
- Train on the **full 20,000 rows** — there's no accuracy-vs-speed tradeoff worth making here.
- A weekly (or even daily) automated retrain task inside the Airflow DAG is cheap and low-risk to add — worth mentioning in Q&A as a natural extension, and a good contrast with how expensive a transformer retrain job would be operationally.
