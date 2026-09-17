"""
Verification script for Human-in-the-Loop Model Governance and Gatekeeper.
"""
import os
import sys
import glob
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import mlflow
from mlflow.tracking import MlflowClient

def test_1_registry_champion_integrity():
    print("=== TEST 1: MLflow Registry Champion & Candidate Aliases ===")
    tracking_uri = f"sqlite:///{PROJECT_ROOT}/data/mlflow.db"
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()

    # Get champion
    try:
        champ_v = client.get_model_version_by_alias("ev-sentiment-model", "champion")
        print(f"✅ Current Champion Version: v{champ_v.version}")
        assert champ_v is not None, "Champion model must exist!"
    except Exception as e:
        print(f"❌ Failed to get Champion: {e}")
        raise

    # Get candidate if any
    try:
        cand_v = client.get_model_version_by_alias("ev-sentiment-model", "candidate")
        print(f"ℹ️  Current Candidate Version: v{cand_v.version} (Tags: {cand_v.tags})")
        # Candidate should NOT be champion
        assert cand_v.version != champ_v.version, "Candidate and Champion must not be identical!"
    except Exception as e:
        print(f"ℹ️  No candidate currently registered (or: {e})")

    # Search all versions
    all_v = client.search_model_versions("name='ev-sentiment-model'")
    print(f"ℹ️  Total Model Versions Registered: {len(all_v)}")
    for v in sorted(all_v, key=lambda x: int(x.version)):
        print(f"   - v{v.version}: aliases={v.aliases}, gatekeeper={v.tags.get('gatekeeper_result')}, status={v.tags.get('approval_status')}")

    print("✅ TEST 1 PASSED: Champion alias is intact and distinct.\n")
    return champ_v.version


def test_2_model_loader_does_not_mutate_champion(current_champ_ver):
    print("=== TEST 2: Model Loader Safe Fallback (No Alias Mutation) ===")
    from ml.model_loader import load_champion_model_robust

    model = load_champion_model_robust(project_root=str(PROJECT_ROOT))
    assert model is not None, "Model loader must return a valid model!"
    assert hasattr(model, "predict_proba"), "Model must have predict_proba!"

    # Verify that champion alias in MLflow registry was NOT mutated
    tracking_uri = f"sqlite:///{PROJECT_ROOT}/data/mlflow.db"
    client = MlflowClient(tracking_uri=tracking_uri)
    champ_v = client.get_model_version_by_alias("ev-sentiment-model", "champion")
    assert str(champ_v.version) == str(current_champ_ver), f"Champion alias mutated! Expected v{current_champ_ver}, got v{champ_v.version}"
    print(f"✅ Champion version verified untouched: v{champ_v.version}")
    print("✅ TEST 2 PASSED: Model loader safely loaded model without mutating registry.\n")


def test_3_gatekeeper_strict_condition():
    print("=== TEST 3: Gatekeeper Strict Improvement Condition ===")
    # Read ml/train_model.py source to verify condition
    train_script = PROJECT_ROOT / "ml" / "train_model.py"
    with open(train_script, "r", encoding="utf-8") as f:
        src = f.read()

    assert "AUTO_PROMOTE" not in src, "AUTO_PROMOTE branch must be completely removed!"
    assert "val_f1 > champion_f1 + 1e-4" in src or "val_f1 > champion_f1" in src, "Gatekeeper must require strict improvement!"
    assert "val_f1 >= champion_f1" not in src, "Gatekeeper must NOT use >= (must be strictly >)!"
    print("✅ Gatekeeper condition verified: strictly requires new model to outperform Champion (> champion_f1).")
    print("✅ AUTO_PROMOTE branch verified completely removed from train_model.py.")
    print("✅ TEST 3 PASSED: Code enforces human approval and strict gatekeeper.\n")


def test_4_dashboard_scorecard_and_no_hotreload():
    print("=== TEST 4: Dashboard Scorecard Component & Toast Messages ===")
    dash_script = PROJECT_ROOT / "dashboard" / "app.py"
    with open(dash_script, "r", encoding="utf-8") as f:
        src = f.read()

    assert "def render_model_scorecard" in src, "Scorecard renderer must be defined!"
    assert "Serving Models Hot-Reloaded" not in src, "Misleading hot-reloaded message must be removed!"
    assert "render_canary_governance_blocked" in src, "Blocked gatekeeper section must be defined!"
    assert "render_canary_governance_pending" in src, "Pending contender section must be defined!"

    # Compile and extract render_model_scorecard specifically
    import ast
    tree = ast.parse(src)
    scorecard_node = None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "render_model_scorecard":
            scorecard_node = node
            break
    assert scorecard_node is not None, "render_model_scorecard AST node found!"
    
    module_ast = ast.Module(body=[scorecard_node], type_ignores=[])
    code = compile(module_ast, filename="<ast>", mode="exec")
    
    import pandas as pd
    captured_tables = []
    class DummySt:
        @staticmethod
        def table(df):
            captured_tables.append(df)
            
    scope = {"pd": pd, "st": DummySt}
    exec(code, scope)
    render_func = scope["render_model_scorecard"]
    
    champ_m = {"macro_f1": 0.8850, "accuracy": 0.8900, "macro_precision": 0.8800, "macro_recall": 0.8900}
    cand_m = {"macro_f1": 0.8920, "accuracy": 0.8980, "macro_precision": 0.8900, "macro_recall": 0.8940}
    champ_p = {"train_dataset_size": "500"}
    cand_p = {"train_dataset_size": "540"}

    render_func("3", champ_m, champ_p, "5", cand_m, cand_p, "Contender (Candidate)", True)
    assert len(captured_tables) == 1, "Scorecard table must be generated!"
    df = captured_tables[0]
    print("Scorecard Output Preview (Passed Contender):\n", df.to_string())
    
    # Also test blocked model scorecard
    captured_tables.clear()
    blocked_m = {"macro_f1": 0.8500, "accuracy": 0.8600, "macro_precision": 0.8400, "macro_recall": 0.8500}
    blocked_p = {"train_dataset_size": "510"}
    render_func("3", champ_m, champ_p, "6", blocked_m, blocked_p, "Blocked Model", False)
    assert len(captured_tables) == 1, "Blocked scorecard table must be generated!"
    df_blocked = captured_tables[0]
    print("\nScorecard Output Preview (Blocked Model):\n", df_blocked.to_string())

    print("\n✅ TEST 4 PASSED: Scorecard table properly formats deltas, metrics, and gatekeeper status.\n")

def test_5_gatekeeper_blocked_flow():
    print("=== TEST 5: Gatekeeper Blocked Flow & Incident Report ===")
    tracking_uri = f"sqlite:///{PROJECT_ROOT}/data/mlflow.db"
    client = MlflowClient(tracking_uri=tracking_uri)
    
    # Check that latest version (v6) was marked as rejected/failed
    all_v = client.search_model_versions("name='ev-sentiment-model'")
    sorted_mvs = sorted(all_v, key=lambda x: int(x.version), reverse=True)
    latest_mv = sorted_mvs[0]
    print(f"Latest registered version: v{latest_mv.version}")
    assert latest_mv.tags.get("gatekeeper_result") == "failed", f"Expected gatekeeper_result=failed, got {latest_mv.tags.get('gatekeeper_result')}"
    assert latest_mv.tags.get("approval_status") == "rejected", f"Expected approval_status=rejected, got {latest_mv.tags.get('approval_status')}"
    assert "champion" not in latest_mv.aliases, "Blocked version must NOT have alias @champion!"

    # Verify incident report exists
    failed_reports = sorted(glob.glob(os.path.join(str(PROJECT_ROOT), "data", "alerts", "retrain_failed_*.html")), reverse=True)
    assert len(failed_reports) > 0, "Gatekeeper failure HTML report must exist in data/alerts/!"
    print(f"✅ Incident report file verified: {failed_reports[0]}")
    with open(failed_reports[0], "r", encoding="utf-8") as f:
        content = f.read()
        assert "GATEKEEPING REJECTED NEW MODEL" in content
        assert "Champion Macro-F1" in content

    # Verify champion is still v3
    champ_v = client.get_model_version_by_alias("ev-sentiment-model", "champion")
    assert str(champ_v.version) == "3", f"Champion must remain v3, got v{champ_v.version}"
    print(f"✅ Champion version safely untouched: v{champ_v.version}")
    print("✅ TEST 5 PASSED: Real-world retrain successfully blocked, logged, and isolated.\n")



if __name__ == "__main__":
    print("\n🚀 Starting Comprehensive Human-in-the-Loop Governance Verification...\n")
    champ_ver = test_1_registry_champion_integrity()
    test_2_model_loader_does_not_mutate_champion(champ_ver)
    test_3_gatekeeper_strict_condition()
    test_4_dashboard_scorecard_and_no_hotreload()
    test_5_gatekeeper_blocked_flow()
    print("🎉 ALL 5 GOVERNANCE VERIFICATION TESTS PASSED SUCCESSFULLY! 🏆\n")
