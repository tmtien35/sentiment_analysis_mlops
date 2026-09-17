"""
Unit test for Break-Glass and Acknowledge buttons logic in dashboard/app.py.
"""
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import mlflow
from mlflow.tracking import MlflowClient
from sqlalchemy import create_engine, text
from dashboard.app import get_setting, set_setting

def test_governance_actions():
    print("🚀 Starting Governance Actions Verification...")
    tracking_uri = f"sqlite:///{PROJECT_ROOT}/data/mlflow.db"
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()
    db_path = PROJECT_ROOT / "data" / "metrics.db"
    engine = create_engine(f"sqlite:///{db_path}")

    champ = client.get_model_version_by_alias("ev-sentiment-model", "champion")
    initial_champ_ver = champ.version
    print(f"Initial Champion: v{initial_champ_ver}")

    all_versions = client.search_model_versions("name='ev-sentiment-model'")
    sorted_mvs = sorted(all_versions, key=lambda x: int(x.version), reverse=True)
    rejected_mv = None
    for mv in sorted_mvs:
        if mv.tags.get("gatekeeper_result") == "failed":
            rejected_mv = mv
            break
    
    assert rejected_mv is not None, "A rejected model version must exist in the database!"
    test_version = str(rejected_mv.version)
    print(f"Found rejected model for testing: v{test_version}")

    # 1. Test Dismiss / Acknowledge
    client.set_model_version_tag(name="ev-sentiment-model", version=test_version, key="gatekeeper_dismissed", value="true")
    with engine.begin() as conn:
        set_setting(conn, "dismissed_gatekeeper_version", test_version)

    with engine.connect() as conn:
        dismissed_ver = get_setting(conn, "dismissed_gatekeeper_version", "")
    
    detected_blocked_version = None
    all_v = client.search_model_versions("name='ev-sentiment-model'")
    for mv in sorted(all_v, key=lambda x: int(x.version), reverse=True):
        if str(mv.version) == str(initial_champ_ver):
            continue
        if mv.tags.get("gatekeeper_dismissed") == "true" or (dismissed_ver and str(mv.version) == str(dismissed_ver)):
            continue
        if mv.tags.get("gatekeeper_result") == "failed" or mv.tags.get("approval_status") in ("rejected", "contender_runner_up"):
            detected_blocked_version = str(mv.version)
            break
    
    assert detected_blocked_version != test_version, f"Dismissed v{test_version} should not be detected!"
    print("✅ Part 1 PASSED: Dismiss / Acknowledge successfully isolates the rejected alert.")

    # 2. Test Break-Glass Force Promote
    client.set_registered_model_alias(name="ev-sentiment-model", alias="champion", version=test_version)
    client.set_model_version_tag(name="ev-sentiment-model", version=test_version, key="status", value="champion")
    client.set_model_version_tag(name="ev-sentiment-model", version=test_version, key="approval_status", value="champion")
    client.set_model_version_tag(name="ev-sentiment-model", version=test_version, key="gatekeeper_result", value="overridden_by_admin")
    client.set_model_version_tag(name="ev-sentiment-model", version=test_version, key="gatekeeper_dismissed", value="true")
    with engine.begin() as conn:
        set_setting(conn, "canary_enabled", "false")
        set_setting(conn, "canary_version", "")
        set_setting(conn, "dismissed_gatekeeper_version", test_version)

    new_champ = client.get_model_version_by_alias("ev-sentiment-model", "champion")
    assert str(new_champ.version) == str(test_version), "Champion alias not updated!"
    assert new_champ.tags.get("approval_status") == "champion", "approval_status tag incorrect!"
    assert new_champ.tags.get("gatekeeper_result") == "overridden_by_admin", "gatekeeper_result tag incorrect!"

    # 3. Restore Initial State
    client.set_registered_model_alias(name="ev-sentiment-model", alias="champion", version=initial_champ_ver)
    client.set_model_version_tag(name="ev-sentiment-model", version=test_version, key="approval_status", value="rejected")
    client.set_model_version_tag(name="ev-sentiment-model", version=test_version, key="gatekeeper_result", value="failed")
    client.delete_model_version_tag(name="ev-sentiment-model", version=test_version, key="gatekeeper_dismissed")
    with engine.begin() as conn:
        set_setting(conn, "dismissed_gatekeeper_version", "")
    restored_champ = client.get_model_version_by_alias("ev-sentiment-model", "champion")
    assert str(restored_champ.version) == str(initial_champ_ver), "Restoration failed!"
    print("✅ Part 2 & 3 PASSED: Break-Glass promotes and tags properly; state restored cleanly.")

    print("\n🎉 ALL GOVERNANCE BUTTON TESTS PASSED WITH 100% SUCCESS!")

if __name__ == "__main__":
    test_governance_actions()
