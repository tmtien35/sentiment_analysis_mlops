"""
Sanitize MLflow database and MLmodel files to ensure 100% portable, environment-agnostic execution.
Replaces absolute host paths (e.g. file:///path/to/project/) with relative paths (mlruns/...).
Safe to run in any environment (Linux Docker or host).
"""

import os
import sqlite3
import shutil
from pathlib import Path

def sanitize_database(db_path="data/mlflow.db"):
    if not os.path.exists(db_path):
        print(f"Database not found: {db_path}")
        return
    
    # Create safety backup
    shutil.copy(db_path, db_path + ".bak")
    
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    # Dynamic project root
    project_root = Path(__file__).resolve().parent.parent.as_posix()
    patterns_to_strip = [
        f"file:{project_root}/",
        f"file:///{project_root}/",
        "file:D:/MLOps/capstone-project/",
        "file:///app/"
    ]
    
    for pat in patterns_to_strip:
        c.execute("UPDATE runs SET artifact_uri = REPLACE(artifact_uri, ?, '')", (pat,))
        c.execute("UPDATE experiments SET artifact_location = REPLACE(artifact_location, ?, '')", (pat,))
        c.execute("UPDATE logged_models SET artifact_location = REPLACE(artifact_location, ?, '')", (pat,))
        c.execute("UPDATE model_versions SET storage_location = REPLACE(storage_location, ?, '')", (pat,))
    
    conn.commit()
    conn.close()
    print("✅ data/mlflow.db successfully sanitized with portable relative paths.")

def sanitize_mlmodel_files(mlruns_dir="mlruns"):
    if not os.path.exists(mlruns_dir):
        return
    
    project_root = Path(__file__).resolve().parent.parent.as_posix()
    patterns_to_strip = [
        f"file:{project_root}/",
        f"file:///{project_root}/",
        "file:D:/MLOps/capstone-project/",
        "file:///app/"
    ]
    
    count = 0
    for root, _, files in os.walk(mlruns_dir):
        for f in files:
            if f in ("MLmodel", "meta.yaml"):
                fp = os.path.join(root, f)
                try:
                    with open(fp, "r", encoding="utf-8") as fh:
                        content = fh.read()
                    
                    new_content = content
                    for pat in patterns_to_strip:
                        if pat in new_content:
                            new_content = new_content.replace(pat, "")
                    
                    if new_content != content:
                        with open(fp, "w", encoding="utf-8") as fh:
                            fh.write(new_content)
                        count += 1
                except Exception as e:
                    print(f"Warning sanitizing {fp}: {e}")
    print(f"✅ Sanitized {count} MLmodel / meta.yaml files in {mlruns_dir}.")

if __name__ == "__main__":
    sanitize_database()
    sanitize_mlmodel_files()
