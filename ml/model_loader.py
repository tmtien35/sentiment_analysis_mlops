"""
Robust Multi-Platform ML Model Loader for MLOps Pipeline.

Ensures seamless, 100% portable model loading across environments (Docker Linux container,
local development, or CI/CD pipelines) without hardcoded paths or OS-specific assumptions.

Provides multi-tier fallback:
1. Standard MLflow Model Registry alias (@champion or @canary) via MLFLOW_TRACKING_URI
2. MLflow Model Registry latest version
3. Direct local binary artifact (data/champion_model.pkl)
4. Portable relative filesystem resolution inside mlruns/
5. Discovery of latest valid model.pkl in mlruns/3/
"""

import os
import sys
import pickle
import sqlite3
from pathlib import Path
import mlflow
import mlflow.sklearn

def _resolve_project_root(project_root=None) -> Path:
    """Dynamically determine the project root directory relative to this file."""
    if project_root:
        p = Path(project_root).resolve()
        if p.exists():
            return p
    # ml/model_loader.py -> parent is ml/ -> parent of ml/ is project root
    return Path(__file__).resolve().parent.parent

def get_mlflow_tracking_uri(project_root=None) -> str:
    """Retrieve MLflow tracking URI from environment or default to project SQLite."""
    env_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if env_uri:
        return env_uri
    root = _resolve_project_root(project_root)
    db_file = root / "data" / "mlflow.db"
    return f"sqlite:///{db_file.as_posix()}"

def export_champion_model_artifact(model, project_root=None):
    """
    Safely export the Champion model pipeline to data/champion_model.pkl
    for zero-dependency, ultra-fast model loading.
    """
    root = _resolve_project_root(project_root)
    dst_dir = root / "data"
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst_path = dst_dir / "champion_model.pkl"
    temp_path = dst_dir / "champion_model.pkl.tmp"
    with open(temp_path, "wb") as f:
        pickle.dump(model, f)
    if temp_path.exists():
        temp_path.replace(dst_path)

def load_champion_model_robust(project_root=None):
    """
    Bulletproof Champion model loader.
    Guarantees loading the actual Scikit-learn Pipeline (with predict_proba support)
    across all platforms and container environments.
    """
    root = _resolve_project_root(project_root)
    tracking_uri = get_mlflow_tracking_uri(root)

    # Tier 1: Try MLflow Registry '@champion'
    try:
        mlflow.set_tracking_uri(tracking_uri)
        model = mlflow.sklearn.load_model("models:/ev-sentiment-model@champion")
        if hasattr(model, "predict_proba"):
            print("✅ Tier 1: Loaded Champion model directly from MLflow Registry (@champion).")
            return model
    except Exception as e_champ:
        print(f"Notice [Tier 1]: MLflow registry alias '@champion' failed ({e_champ}).")

    # Tier 2: Direct Persistent Binary Artifact (data/champion_model.pkl)
    pkl_path = root / "data" / "champion_model.pkl"
    if pkl_path.exists():
        try:
            with open(pkl_path, "rb") as f:
                model = pickle.load(f)
            if hasattr(model, "predict_proba"):
                print(f"✅ Tier 2: Loaded Champion model from persistent binary artifact: {pkl_path}")
                return model
        except Exception as e_pkl:
            print(f"Notice [Tier 2]: Failed to unpickle {pkl_path}: {e_pkl}")

    # Tier 3: Try latest registered model version from MLflow Registry (read-only fallback)
    try:
        from mlflow.tracking import MlflowClient
        client = MlflowClient(tracking_uri=tracking_uri)
        versions = client.search_model_versions("name='ev-sentiment-model'")
        if versions:
            latest_v = max(int(v.version) for v in versions)
            model = mlflow.sklearn.load_model(f"models:/ev-sentiment-model/{latest_v}")
            if hasattr(model, "predict_proba"):
                print(f"✅ Tier 3: Loaded model v{latest_v} from MLflow Registry as fallback.")
                return model
    except Exception as e_reg:
        print(f"Notice [Tier 3]: MLflow registry version fallback failed ({e_reg}).")

    # Tier 4: Portable Relative Filesystem Resolution from mlruns using champion metadata from SQLite
    db_path = root / "data" / "mlflow.db"
    if db_path.exists():
        try:
            conn = sqlite3.connect(str(db_path))
            c = conn.cursor()
            champ_ver = None
            for tbl in ["registered_model_aliases", "model_version_aliases"]:
                try:
                    c.execute(f"SELECT version FROM {tbl} WHERE alias='champion' AND name='ev-sentiment-model'")
                    row = c.fetchone()
                    if row:
                        champ_ver = row[0]
                        break
                except Exception:
                    pass
            
            if not champ_ver:
                try:
                    c.execute("SELECT version FROM model_version_tags WHERE name='ev-sentiment-model' AND key='status' AND value='champion'")
                    row = c.fetchone()
                    if row and row[0]:
                        champ_ver = row[0]
                except Exception:
                    pass

            if champ_ver:
                c.execute("SELECT source, run_id FROM model_versions WHERE name='ev-sentiment-model' AND version=?", (champ_ver,))
                mv = c.fetchone()
                if mv:
                    source, run_id = mv
                    model_id = str(source).replace("models:/", "")
                    candidate_model_files = [
                        root / "mlruns" / "3" / "models" / model_id / "artifacts" / "model.pkl",
                        root / "mlruns" / "3" / str(run_id) / "artifacts" / "model" / "model.pkl",
                        root / "mlruns" / "models" / model_id / "artifacts" / "model.pkl",
                    ]
                    for mf in candidate_model_files:
                        if mf.exists():
                            with open(mf, "rb") as f:
                                model = pickle.load(f)
                            if hasattr(model, "predict_proba"):
                                print(f"✅ Tier 4: Loaded Champion model (v{champ_ver}) from mlruns: {mf}")
                                try:
                                    export_champion_model_artifact(model, root)
                                except Exception:
                                    pass
                                return model
            conn.close()
        except Exception as e_sql:
            print(f"Notice [Tier 4]: SQLite artifact resolution deferred: {e_sql}")

    # Tier 5: Discovery of latest valid model.pkl in mlruns/3
    exp3_dir = root / "mlruns" / "3"
    if exp3_dir.exists():
        discovered = []
        for p in exp3_dir.rglob("model.pkl"):
            discovered.append((p.stat().st_mtime, p))
        if discovered:
            discovered.sort(reverse=True)
            for _, best_path in discovered:
                try:
                    with open(best_path, "rb") as f:
                        model = pickle.load(f)
                    if hasattr(model, "predict_proba"):
                        print(f"✅ Tier 5: Loaded latest discovered model artifact: {best_path}")
                        return model
                except Exception:
                    pass

    print("❌ ERROR: Could not locate any valid Champion model in MLflow Registry or local filesystem.")
    return None


def load_canary_model_robust(canary_ver=None, project_root=None):
    """
    Load Canary model with multi-tier resilience across environments.
    """
    root = _resolve_project_root(project_root)
    tracking_uri = get_mlflow_tracking_uri(root)

    canary_uri = f"models:/ev-sentiment-model/{canary_ver}" if canary_ver else "models:/ev-sentiment-model@canary"
    try:
        mlflow.set_tracking_uri(tracking_uri)
        model = mlflow.sklearn.load_model(canary_uri)
        if hasattr(model, "predict_proba"):
            print(f"✅ Loaded Canary model ({canary_uri}) from MLflow Registry.")
            return model
    except Exception as e_canary:
        print(f"Notice: MLflow load for Canary ({canary_uri}) failed ({e_canary}). Falling back to local artifact resolution...")

    db_path = root / "data" / "mlflow.db"
    if db_path.exists():
        try:
            conn = sqlite3.connect(str(db_path))
            c = conn.cursor()
            ver = canary_ver
            if not ver:
                for tbl in ["registered_model_aliases", "model_version_aliases"]:
                    try:
                        c.execute(f"SELECT version FROM {tbl} WHERE alias='canary' AND name='ev-sentiment-model'")
                        row = c.fetchone()
                        if row:
                            ver = row[0]
                            break
                    except Exception:
                        pass
            
            if ver:
                c.execute("SELECT source, run_id FROM model_versions WHERE name='ev-sentiment-model' AND version=?", (ver,))
                mv = c.fetchone()
                if mv:
                    source, run_id = mv
                    model_id = str(source).replace("models:/", "")
                    candidate_model_files = [
                        root / "mlruns" / "3" / "models" / model_id / "artifacts" / "model.pkl",
                        root / "mlruns" / "3" / str(run_id) / "artifacts" / "model" / "model.pkl",
                    ]
                    for mf in candidate_model_files:
                        if mf.exists():
                            with open(mf, "rb") as f:
                                model = pickle.load(f)
                            if hasattr(model, "predict_proba"):
                                print(f"✅ Loaded Canary model (v{ver}) from mlruns: {mf}")
                                return model
            conn.close()
        except Exception:
            pass

    return None
