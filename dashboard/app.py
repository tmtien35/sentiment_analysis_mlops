import os
import streamlit as st
import pandas as pd
import requests
from sqlalchemy import create_engine, text

st.set_page_config(
    page_title="Inference Monitor",
    layout="wide",
    initial_sidebar_state="collapsed"
)

def get_db_engine():
    db_url = os.environ.get("DATABASE_URL", "sqlite:///data/results.db")
    return create_engine(db_url)

def initialize_settings_table(conn):
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS system_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
    """))
    # Seed default value if empty
    res = conn.execute(text("SELECT value FROM system_settings WHERE key = 'serving_mode'"))
    if res.fetchone() is None:
        conn.execute(text("INSERT INTO system_settings VALUES ('serving_mode', 'ml')"))
    
    # Idempotently add verified_sentiment column if missing
    try:
        conn.execute(text("ALTER TABLE store_reviews ADD COLUMN verified_sentiment TEXT DEFAULT NULL;"))
    except Exception:
        pass

def get_setting(conn, key, default):
    try:
        initialize_settings_table(conn)
        res = conn.execute(text("SELECT value FROM system_settings WHERE key = :key"), {"key": key})
        row = res.fetchone()
        return row[0] if row else default
    except Exception:
        return default

def set_setting(conn, key, value):
    try:
        initialize_settings_table(conn)
        conn.execute(text("DELETE FROM system_settings WHERE key = :key"), {"key": key})
        conn.execute(text("INSERT INTO system_settings VALUES (:key, :value)"), {"key": key, "value": value})
    except Exception as e:
        print(f"Error setting: {e}")

def render_canary_governance_active(client, engine, df_logs, champion_version, canary_version):
    st.info(f"🐤 **Canary Routing Active:** Đang điều phối lưu lượng **90% Champion (v{champion_version})** / **10% Canary (v{canary_version})** trên FastAPI.")
    if df_logs is not None and len(df_logs) > 0 and 'model_route' in df_logs.columns:
        c_logs = df_logs[df_logs['model_route'] == 'champion']
        k_logs = df_logs[df_logs['model_route'] == 'canary']
        n_tot = len(df_logs)
        n_c = len(c_logs)
        n_k = len(k_logs)
        c_conf = (c_logs['confidence'].mean() * 100) if n_c > 0 else 0.0
        k_conf = (k_logs['confidence'].mean() * 100) if n_k > 0 else 0.0
        c_lat = c_logs['latency_ms'].mean() if (n_c > 0 and 'latency_ms' in c_logs.columns) else 0.0
        k_lat = k_logs['latency_ms'].mean() if (n_k > 0 and 'latency_ms' in k_logs.columns) else 0.0
        st.markdown("#### 📊 Operational KPIs: Champion vs Canary (Proxy Monitoring)")
        col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)
        with col_kpi1:
            st.metric("🏆 Champion Traffic", f"{n_c:,} reqs", f"{((n_c/n_tot)*100) if n_tot > 0 else 0:.1f}% share")
        with col_kpi2:
            st.metric("🐤 Canary Traffic", f"{n_k:,} reqs", f"{((n_k/n_tot)*100) if n_tot > 0 else 0:.1f}% share")
        with col_kpi3:
            st.metric("🐤 Canary Confidence", f"{k_conf:.1f}%", delta=f"{k_conf - c_conf:+.1f}% vs Champ")
        with col_kpi4:
            st.metric("🐤 Canary Latency", f"{k_lat:.1f} ms", delta=f"{k_lat - c_lat:+.1f} ms vs Champ", delta_color="inverse")
        if n_k >= 3:
            if k_conf >= 70.0:
                st.success("✅ **Proxy KPIs Passed:** Mô hình Canary vận hành đạt tiêu chuẩn (Confidence >= 70%, Latency ổn định). Đủ điều kiện thăng hạng!")
            else:
                st.warning("⚠️ **Proxy KPIs Warning:** Độ tự tin của Canary đang thấp hơn kỳ vọng. Nên kiểm tra kỹ log trước khi thăng hạng.")
        else:
            st.caption("ℹ️ *Đang tích lũy thêm dữ liệu inference từ người dùng để đánh giá toàn diện proxy KPIs.*")
            
    col_act1, col_act2 = st.columns(2)
    with col_act1:
        if st.button("🚀 1-Click Promote Canary to Champion (100% Traffic)", key="promote_canary_main_btn", type="primary", use_container_width=True):
            try:
                client.set_registered_model_alias(name="ev-sentiment-model", alias="champion", version=canary_version)
                client.set_model_version_tag(name="ev-sentiment-model", version=canary_version, key="status", value="champion")
                with engine.begin() as conn:
                    set_setting(conn, "canary_enabled", "false")
                    set_setting(conn, "canary_version", "")
                try:
                    client.delete_registered_model_alias(name="ev-sentiment-model", alias="canary")
                except Exception:
                    pass
                try:
                    client.delete_registered_model_alias(name="ev-sentiment-model", alias="candidate")
                except Exception:
                    pass
                notify_api_reload()
                st.success(f"🏆 Thăng hạng thành công Version {canary_version} lên @champion (100% lưu lượng)!")
                st.cache_data.clear()
                st.rerun()
            except Exception as ex:
                st.error(f"Lỗi thăng hạng: {ex}")
    with col_act2:
        if st.button("🔙 1-Click Rollback / Hủy Bỏ Canary", key="rollback_canary_main_btn", use_container_width=True):
            try:
                with engine.begin() as conn:
                    set_setting(conn, "canary_enabled", "false")
                    set_setting(conn, "canary_version", "")
                try:
                    client.delete_registered_model_alias(name="ev-sentiment-model", alias="canary")
                except Exception:
                    pass
                client.set_model_version_tag(name="ev-sentiment-model", version=canary_version, key="approval_status", value="canary_aborted")
                notify_api_reload()
                st.warning(f"🛡️ Đã hủy bỏ Canary Version {canary_version}! 100% lưu lượng đã phục hồi về Champion an toàn.")
                st.cache_data.clear()
                st.rerun()
            except Exception as ex:
                st.error(f"Lỗi: {ex}")

def render_canary_governance_pending(client, engine, champion_version, candidate_version, champ_metrics, cand_metrics):
    st.warning(f"🎉 **Contender Model Version {candidate_version} ĐÃ VƯỢT QUA Champion trên tập Validation!**")
    st.markdown(f"""
    * 🏆 **Champion Macro-F1 (v{champion_version}):** `{champ_metrics.get('macro_f1', 0.0):.4f}`
    * 🥊 **Contender Macro-F1 (v{candidate_version}):** `{cand_metrics.get('macro_f1', 0.0):.4f}` *(+{(cand_metrics.get('macro_f1', 0.0) - champ_metrics.get('macro_f1', 0.0)):.4f})*
    * 📋 **Chính sách:** Mô hình mới không tự động thăng hạng. Cần Human Approval để mở Canary Routing 10% an toàn.
    """)
    col_can1, col_can2 = st.columns(2)
    with col_can1:
        if st.button("✅ Phê Duyệt & Bật Canary (10% Traffic)", key="approve_canary_main_btn", type="primary", use_container_width=True):
            try:
                client.set_registered_model_alias(name="ev-sentiment-model", alias="canary", version=candidate_version)
                client.set_model_version_tag(name="ev-sentiment-model", version=candidate_version, key="approval_status", value="canary_active")
                with engine.begin() as conn:
                    set_setting(conn, "canary_enabled", "true")
                    set_setting(conn, "canary_version", candidate_version)
                    set_setting(conn, "canary_traffic_pct", "10")
                notify_api_reload()
                st.success(f"🐤 Đã kích hoạt Canary Version {candidate_version} với 10% lưu lượng!")
                st.cache_data.clear()
                st.rerun()
            except Exception as ex:
                st.error(f"Lỗi: {ex}")
    with col_can2:
        if st.button("❌ Từ Chối Contender", key="reject_contender_main_btn", use_container_width=True):
            try:
                client.set_model_version_tag(name="ev-sentiment-model", version=candidate_version, key="approval_status", value="rejected_by_human")
                try:
                    client.delete_registered_model_alias(name="ev-sentiment-model", alias="candidate")
                except Exception:
                    pass
                st.info(f"Đã từ chối mô hình Version {candidate_version}.")
                st.cache_data.clear()
                st.rerun()
            except Exception as ex:
                st.error(f"Lỗi: {ex}")

def render_canary_governance(client, engine, df_logs, champion_version, candidate_version, canary_version, has_candidate, has_canary, candidate_status, canary_is_enabled, champ_metrics, cand_metrics):
    st.markdown("---")
    st.markdown("### 🚦 Phê Duyệt & Điều Phối Canary (Enterprise Governance & Traffic Routing)")
    if has_canary and canary_is_enabled:
        render_canary_governance_active(client, engine, df_logs, champion_version, canary_version)
    elif has_candidate and candidate_status == "pending_human_approval":
        render_canary_governance_pending(client, engine, champion_version, candidate_version, champ_metrics, cand_metrics)
    else:
        st.success(f"✅ **Trạng thái phục vụ:** 100% Champion (Version {champion_version}) — Hệ thống vận hành ổn định.")



@st.cache_data(ttl=2)
def load_data():
    engine = get_db_engine()
    try:
        with engine.connect() as conn:
            df_preds = pd.read_sql("SELECT * FROM predictions", con=conn.connection)
            df_drift = pd.read_sql("SELECT * FROM drift_metrics ORDER BY batch_date ASC", con=conn.connection)
            try:
                df_logs = pd.read_sql("SELECT * FROM inference_logs WHERE review_text != 'init' ORDER BY timestamp DESC", con=conn.connection)
            except Exception:
                df_logs = pd.DataFrame()
            try:
                query = """
                    SELECT 
                        s.review_id,
                        s.review_date,
                        s.category,
                        s.review_text,
                        p.predicted_sentiment,
                        p.confidence,
                        s.verified_sentiment
                    FROM store_reviews s
                    LEFT JOIN predictions p ON s.review_id = p.review_id
                """
                df_audit = pd.read_sql(query, con=conn.connection)
            except Exception:
                df_audit = pd.DataFrame()
        return df_preds, df_drift, df_logs, df_audit, None
    except Exception as e:
        return None, None, None, None, str(e)

# ------------------------------------------------------------------
# 🛠️ MLOps Incident Control Panel (Sidebar)
# ------------------------------------------------------------------
st.sidebar.markdown("## 🛠️ MLOps Incident Control Panel")
st.sidebar.markdown("Use these manual overrides to respond to production incidents in real-time.")

engine = get_db_engine()
df_preds, df_drift, df_logs, df_audit, error = load_data()

# Fetch active model versions from MLflow Registry for audit trail
champion_version = "None"
candidate_version = "None"
canary_version = "None"
has_candidate = False
has_canary = False
candidate_status = "contender"
champ_metrics = {}
cand_metrics = {}
canary_metrics = {}
champ_params = {}
cand_params = {}
canary_params = {}

canary_is_enabled = False
try:
    with engine.connect() as conn:
        canary_is_enabled = get_setting(conn, "canary_enabled", "false").lower() == "true"
except Exception:
    pass

def notify_api_reload():
    api_url = os.environ.get("FASTAPI_URL", "http://localhost:8000/predict").replace("/predict", "/reload-models")
    try:
        requests.post(api_url, timeout=2)
    except Exception:
        pass

try:
    import mlflow
    from mlflow.tracking import MlflowClient
    mlflow.set_tracking_uri("sqlite:///data/mlflow.db")
    client = MlflowClient()
    
    # Get active @champion version
    try:
        version_info_champ = client.get_model_version_by_alias("ev-sentiment-model", "champion")
        if version_info_champ:
            champion_version = str(version_info_champ.version)
            champ_run = client.get_run(version_info_champ.run_id)
            champ_metrics = champ_run.data.metrics
            champ_params = champ_run.data.params
    except Exception:
        pass

    # Get active @canary version (if any)
    try:
        version_info_canary = client.get_model_version_by_alias("ev-sentiment-model", "canary")
        if version_info_canary and str(version_info_canary.version) != champion_version:
            canary_version = str(version_info_canary.version)
            has_canary = True
            canary_run = client.get_run(version_info_canary.run_id)
            canary_metrics = canary_run.data.metrics
            canary_params = canary_run.data.params
    except Exception:
        pass

    # Get active @candidate version (if any contender exists)
    try:
        version_info_cand = client.get_model_version_by_alias("ev-sentiment-model", "candidate")
        if version_info_cand and str(version_info_cand.version) != champion_version:
            candidate_version = str(version_info_cand.version)
            has_candidate = True
            candidate_status = version_info_cand.tags.get("approval_status", "contender")
            cand_run = client.get_run(version_info_cand.run_id)
            cand_metrics = cand_run.data.metrics
            cand_params = cand_run.data.params
    except Exception:
        pass
except Exception:
    pass

st.sidebar.markdown("### 🏷️ Active Registry Version")
st.sidebar.markdown(f"🏆 **Champion Model:** `Version {champion_version}`")

if has_canary and canary_is_enabled:
    st.sidebar.markdown(f"🐤 **Canary Model:** `Version {canary_version}` *(10% Traffic)*")
    if st.sidebar.button("🔙 Quick Abort Canary", key="quick_abort_canary"):
        try:
            with engine.begin() as conn:
                set_setting(conn, "canary_enabled", "false")
                set_setting(conn, "canary_version", "")
            try:
                client.delete_registered_model_alias(name="ev-sentiment-model", alias="canary")
            except Exception:
                pass
            notify_api_reload()
            st.sidebar.success("Đã ngắt Canary!")
            st.rerun()
        except Exception as e:
            st.sidebar.error(f"Lỗi: {e}")

if has_candidate:
    status_label = "Chờ duyệt Canary" if candidate_status == "pending_human_approval" else "Contender"
    st.sidebar.markdown(f"🥊 **Contender Model:** `Version {candidate_version}` *({status_label})*")
    
    with st.sidebar.expander("⚖️ So Sánh Champion vs Contender", expanded=True):
        champ_f1 = champ_metrics.get("macro_f1", 0.0)
        cand_f1 = cand_metrics.get("macro_f1", 0.0)
        champ_size = champ_params.get("train_dataset_size", "N/A")
        cand_size = cand_params.get("train_dataset_size", "N/A")
        champ_acc = champ_metrics.get("accuracy", 0.0)
        cand_acc = cand_metrics.get("accuracy", 0.0)
        
        st.markdown(f"""
        | Chỉ số | 🏆 Champ (v{champion_version}) | 🥊 Contender (v{candidate_version}) |
        | :--- | :---: | :---: |
        | **Macro-F1** | `{champ_f1:.4f}` | `{cand_f1:.4f}` |
        | **Accuracy** | `{champ_acc*100:.1f}%` | `{cand_acc*100:.1f}%` |
        | **Train Size** | `{champ_size}` mẫu | `{cand_size}` mẫu |
        """)
        
        if candidate_status == "pending_human_approval":
            st.success("🎉 Contender đã vượt qua Champion! Kéo xuống khu vực Gatekeeper để bật Canary 10%.")
        
        if st.button("⚠️ Chấp nhận đánh đổi: Ép lên Champion 🏆", key="force_promote_btn", type="primary"):
            try:
                client.set_registered_model_alias(name="ev-sentiment-model", alias="champion", version=candidate_version)
                client.set_model_version_tag(name="ev-sentiment-model", version=candidate_version, key="status", value="champion")
                try:
                    client.delete_registered_model_alias(name="ev-sentiment-model", alias="candidate")
                except Exception:
                    pass
                with engine.begin() as conn:
                    set_setting(conn, "canary_enabled", "false")
                    set_setting(conn, "canary_version", "")
                notify_api_reload()
                st.success(f"Đã ép thăng hạng Version {candidate_version} lên @champion thành công!")
                st.rerun()
            except Exception as e:
                st.error(f"Lỗi thăng hạng: {e}")
else:
    st.sidebar.markdown("🥊 **Contender Model:** *None (Hệ thống tối ưu)*")

st.sidebar.markdown("---")

# 1. Circuit Breaker (ML vs Fallback)
try:
    with engine.connect() as conn:
        current_mode = get_setting(conn, "serving_mode", "ml")
except Exception:
    current_mode = "ml"

mode_index = 0 if current_mode == "ml" else 1
new_mode = st.sidebar.selectbox(
    "Active Serving Mode:",
    ["Machine Learning Model", "Rule-Based Fallback Rules"],
    index=mode_index
)
target_mode_val = "ml" if new_mode == "Machine Learning Model" else "fallback"

if target_mode_val != current_mode:
    try:
        with engine.begin() as conn:
            set_setting(conn, "serving_mode", target_mode_val)
        st.sidebar.success(f"Bypassed serving mode to: {target_mode_val.upper()}")
        st.rerun()
    except Exception as e:
        st.sidebar.error(f"Failed to toggle: {e}")

# Display active mode indicator
if target_mode_val == "fallback":
    st.sidebar.warning("🛡️ Safe-Mode Active: ML Model Bypassed!")

# 2. Trigger Retraining
st.sidebar.markdown("---")
st.sidebar.markdown("### 🧠 Continuous Training")
if st.sidebar.button("Trigger Retrain Manual"):
    with st.spinner("Retraining model in background..."):
        import subprocess, sys
        env = os.environ.copy()
        env["PYTHONPATH"] = os.getcwd()
        try:
            res = subprocess.run([sys.executable, "ml/train_model.py"], env=env, capture_output=True, text=True)
            if res.returncode == 0:
                st.sidebar.success("🏆 Retraining Completed!")
                st.cache_data.clear()
                st.rerun()
            else:
                st.sidebar.error("Retrain failed/aborted. Check data/alerts/ for reports.")
        except Exception as e:
            st.sidebar.error(f"Error executing retrain: {e}")

# 4. Mute/Acknowledge Alert
# Only show this if there is an active alert!
try:
    latest_drift_val = df_drift.iloc[-1]
    is_drifted_val = latest_drift_val['drift_detected'] == 1
    drift_date_val = latest_drift_val['batch_date']
except Exception:
    is_drifted_val = False
    drift_date_val = None

if is_drifted_val:
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🚨 Alert Acknowledgement")
    if st.sidebar.button("Acknowledge & Mute Alert"):
        try:
            with engine.begin() as conn:
                conn.execute(text("UPDATE drift_metrics SET drift_detected = 2 WHERE batch_date = :ds"), {"ds": drift_date_val})
            st.cache_data.clear()
            st.sidebar.success("Alert Muted successfully!")
            st.rerun()
        except Exception as e:
            st.sidebar.error(f"Failed to mute: {e}")

st.title("🚗 Vietnamese EV Review Sentiment & Drift Monitor")
st.markdown("---")

if error:
    st.error(f"Error connecting to results database: {error}")
    st.info("Please make sure you have run the data generator and the batch scoring backfill.")
elif df_preds is None or len(df_preds) == 0:
    st.warning("Database connected but contains no prediction records.")
else:
    total_reviews = len(df_preds)
    sent_counts = df_preds['predicted_sentiment'].value_counts()
    
    pos_pct = (sent_counts.get('positive', 0) / total_reviews) * 100
    neg_pct = (sent_counts.get('negative', 0) / total_reviews) * 100
    
    latest_drift = df_drift.iloc[-1]
    is_drifted = latest_drift['drift_detected'] == 1
    is_muted = latest_drift['drift_detected'] == 2
    latest_psi = latest_drift['psi_score']
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Reviews Scored", f"{total_reviews:,}")
    with col2:
        st.metric("Positive Sentiment Ratio", f"{pos_pct:.1f}%")
    with col3:
        st.metric("Negative Sentiment Ratio", f"{neg_pct:.1f}%")
    with col4:
        if is_drifted:
            st.metric("Drift Status", "⚠️ DRIFT ALERT", delta=f"PSI: {latest_psi:.3f}", delta_color="inverse")
        elif is_muted:
            st.metric("Drift Status", "⚠️ DRIFT MUTED", delta=f"PSI: {latest_psi:.3f} (Muted)", delta_color="off")
        else:
            st.metric("Drift Status", "✅ STABLE", delta=f"PSI: {latest_psi:.3f}")

    st.markdown("### 📊 Live Analytics Dashboard")
    col_chart1, col_chart2 = st.columns(2)
    
    with col_chart1:
        st.subheader("Sentiment Count Breakdown")
        st.bar_chart(sent_counts, color="#3498db")
        
    with col_chart2:
        st.subheader("Daily Sentiment Volume Timeline")
        daily_trends = df_preds.groupby(['review_date', 'predicted_sentiment']).size().unstack(fill_value=0)
        for col in ['negative', 'neutral', 'positive']:
            if col not in daily_trends.columns:
                daily_trends[col] = 0
        st.line_chart(daily_trends[['negative', 'neutral', 'positive']], color=["#e74c3c", "#f1c40f", "#2ecc71"])

    col_sub1, col_sub2 = st.columns(2)
    with col_sub1:
        st.subheader("Category Sentiment Breakdown")
        if 'category' in df_preds.columns and len(df_preds['category'].dropna()) > 0:
            cat_trends = df_preds.groupby(['category', 'predicted_sentiment']).size().unstack(fill_value=0)
            for col in ['negative', 'neutral', 'positive']:
                if col not in cat_trends.columns:
                    cat_trends[col] = 0
            st.bar_chart(cat_trends[['negative', 'neutral', 'positive']], color=["#e74c3c", "#f1c40f", "#2ecc71"])
        else:
            st.info("Category breakdown will appear once category data is ingested.")

    with col_sub2:
        st.subheader("Average Confidence by Sentiment Class")
        if 'confidence' in df_preds.columns and 'predicted_sentiment' in df_preds.columns:
            conf_scale = 100 if df_preds['confidence'].max() <= 1.0 else 1
            conf_by_class = (df_preds.groupby('predicted_sentiment')['confidence'].mean() * conf_scale).round(1)
            for col in ['positive', 'neutral', 'negative']:
                if col not in conf_by_class.index:
                    conf_by_class[col] = 0.0
            st.bar_chart(conf_by_class[['positive', 'neutral', 'negative']], color="#16a085")

    st.markdown("### ⚠️ Model Health & Data Drift Monitoring")
    col_drift1, col_drift2 = st.columns(2)
    
    with col_drift1:
        st.subheader("Population Stability Index (PSI) Trend (Threshold: 0.15)")
        df_psi = df_drift.set_index('batch_date')[['psi_score']]
        st.line_chart(df_psi, color="#9b59b6")
        
    with col_drift2:
        st.subheader("Average Prediction Confidence")
        df_conf = df_drift.set_index('batch_date')[['avg_confidence']]
        st.line_chart(df_conf, color="#1abc9c")

    col_unc1, col_unc2 = st.columns([1, 1])
    with col_unc1:
        st.subheader("Confidence Uncertainty Distribution")
        if 'confidence' in df_preds.columns and len(df_preds) > 0:
            c_scale = 100 if df_preds['confidence'].max() <= 1.0 else 1
            conf_vals = df_preds['confidence'] * c_scale
            high_cnt = int((conf_vals >= 80).sum())
            mod_cnt = int(((conf_vals >= 60) & (conf_vals < 80)).sum())
            low_cnt = int((conf_vals < 60).sum())
            
            bucket_df = pd.DataFrame({
                "Uncertainty Tier": ["🟢 High (≥80%)", "🟡 Moderate (60-79%)", "🔴 Low / Uncertain (<60%)"],
                "Review Count": [high_cnt, mod_cnt, low_cnt]
            }).set_index("Uncertainty Tier")
            st.bar_chart(bucket_df, color="#3498db")
            
    with col_unc2:
        st.subheader("Uncertainty Tier Breakdown")
        total_c = len(df_preds)
        if total_c > 0 and 'confidence' in df_preds.columns:
            pct_high = (high_cnt / total_c) * 100
            pct_mod = (mod_cnt / total_c) * 100
            pct_low = (low_cnt / total_c) * 100
            
            col_u1, col_u2, col_u3 = st.columns(3)
            with col_u1:
                st.metric("🟢 High Confidence", f"{high_cnt:,} ({pct_high:.1f}%)")
            with col_u2:
                st.metric("🟡 Moderate", f"{mod_cnt:,} ({pct_mod:.1f}%)")
            with col_u3:
                st.metric("🔴 Uncertain (<60%)", f"{low_cnt:,} ({pct_low:.1f}%)")
            st.info("💡 **MLOps Insight:** Reviews in the **🔴 Low / Uncertain** tier represent candidate samples prioritized for Active Learning human audit.")

    st.markdown("---")
    col_test, col_table = st.columns([2, 3])
    
    with col_test:
        st.markdown("### ⚡ Live On-Demand Scoring")
        st.markdown("Test scoring using our real-time **FastAPI endpoint**.")
        user_input = st.text_area("Enter custom review text to score:", placeholder="Type a review here...")
        
        # Check and display previous prediction in session state
        if "last_prediction" in st.session_state:
            sent = st.session_state.get("last_sentiment")
            pred_text = st.session_state.get("last_prediction")
            if sent == "positive":
                st.success(pred_text)
            elif sent == "negative":
                st.error(pred_text)
            else:
                st.warning(pred_text)

        if st.button("Predict Sentiment"):
            if not user_input.strip():
                st.warning("Review text cannot be empty!")
            else:
                api_url = os.environ.get("FASTAPI_URL", "http://localhost:8000/predict")
                try:
                    res = requests.post(api_url, json={"review_text": user_input}, timeout=2)
                    if res.status_code == 200:
                        data = res.json()
                        sent = data["predicted_sentiment"]
                        conf = data["confidence"] * 100
                        # Store in session state
                        st.session_state["last_sentiment"] = sent
                        st.session_state["last_prediction"] = f"**Predicted Sentiment:** {sent.upper()} ({conf:.1f}% confidence)"
                        # Trigger an instant rerun so that load_data() executes again and fetches the new database row immediately!
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(f"FastAPI error code: {res.status_code}")
                except Exception as e:
                    st.error(f"Failed to connect to FastAPI at {api_url}: {e}")
                    
    with col_table:
        st.markdown("### 🔍 Scored Reviews Explorer")
        sentiment_filter = st.selectbox("Filter table by predicted sentiment:", ["All", "positive", "neutral", "negative"])
        filtered_df = df_preds if sentiment_filter == "All" else df_preds[df_preds['predicted_sentiment'] == sentiment_filter]
        
        st.dataframe(
            filtered_df[['review_date', 'category', 'review_text', 'predicted_sentiment', 'confidence']].sort_values('review_date', ascending=False),
            height=220
        )

    # Render Enterprise Canary Governance & Approval Panel
    render_canary_governance(
        client, engine, df_logs, champion_version, candidate_version, canary_version,
        has_candidate, has_canary, candidate_status, canary_is_enabled, champ_metrics, cand_metrics
    )

    st.markdown("---")
    st.markdown("### 🖥️ Live API Traffic Monitor (Real-Time Inference Logs)")
    if df_logs is not None and len(df_logs) > 0:
        cols_to_show = ['timestamp', 'model_route', 'latency_ms', 'review_text', 'predicted_sentiment', 'confidence']
        avail_cols = [c for c in cols_to_show if c in df_logs.columns]
        st.dataframe(
            df_logs[avail_cols],
            height=200
        )
    else:
        st.info("No live on-demand API requests logged yet. Submit a prediction above to test!")

    st.markdown("---")
    st.markdown("### 🧠 Active Learning & Human-in-the-Loop Audit")
    
    # Check if there are any failed gatekeeper reports
    import glob
    failed_reports = glob.glob(os.path.join("data", "alerts", "retrain_failed_*.html"))
    has_gatekeeper_failure = len(failed_reports) > 0
    
    # Check drift status across batches
    is_drift_active = False
    drift_date_val = None
    drifted_dates = []
    if df_drift is not None and len(df_drift) > 0:
        latest_drift_val = df_drift.iloc[-1]
        is_drift_active = latest_drift_val['drift_detected'] == 1
        drift_date_val = latest_drift_val['batch_date']
        drifted_dates = df_drift[df_drift['drift_detected'] >= 1]['batch_date'].tolist()
        
    # Check if any historical drifted date still has unverified reviews pending audit
    pending_drift_dates = []
    if df_audit is not None and len(df_audit) > 0 and len(drifted_dates) > 0:
        unverified_mask = (
            df_audit['verified_sentiment'].isna() | 
            (df_audit['verified_sentiment'] == '') | 
            (df_audit['verified_sentiment'].astype(str).str.lower() == 'none') |
            (df_audit['verified_sentiment'].astype(str).str.lower() == 'nan') |
            (df_audit['verified_sentiment'].astype(str).str.lower() == 'null')
        )
        pending_drift_dates = sorted(list(df_audit[unverified_mask & df_audit['review_date'].isin(drifted_dates)]['review_date'].dropna().unique()), reverse=True)
    has_pending_drift_audit = len(pending_drift_dates) > 0

    col_title_space, col_override = st.columns([3, 1])
    with col_override:
        manual_unlock = st.toggle("🔓 Mở khóa thủ công", value=False, help="Mở khóa bảng thẩm định để xem và gán nhãn cho bất kỳ mẻ dữ liệu nào trong lịch sử kể cả khi hệ thống đang Stable.")

    is_unlocked = has_gatekeeper_failure or is_drift_active or has_pending_drift_audit or manual_unlock
    
    if not is_unlocked:
        st.success("🔒 **Audit Panel Locked (Healthy)**: The current production champion model is running smoothly, and no unverified drift batches remain. Human intervention is not required at this time.")
        st.caption("💡 *Mẹo: Nếu muốn chủ động kiểm tra hoặc gán nhãn cho các mẻ dữ liệu trong quá khứ, hãy bật công tắc '🔓 Mở khóa thủ công' ở góc phải trên.*")
    else:
        if manual_unlock:
            st.info("🔓 **Audit Panel Unlocked (Chế độ thủ công)**: Bạn đang kích hoạt chế độ mở khóa thủ công. Bạn có thể tự do lọc và thẩm định bất kỳ mẻ dữ liệu nào trong lịch sử.")
        elif has_gatekeeper_failure:
            st.warning("🔓 **Audit Panel Unlocked (Gatekeeping Failure Detected)**: The last automated model retraining failed the gatekeeper because the candidate's validation score did not beat the champion. Human auditing is required for reviews in the drifted batch!")
        elif is_drift_active:
            st.warning(f"🔓 **Audit Panel Unlocked (Active Data Drift Alert)**: A data drift alert (unmuted) was detected on batch {drift_date_val}! Human operators should audit and label reviews in this drifted batch to ensure retraining is highly accurate.")
        elif has_pending_drift_audit:
            st.warning(f"🔓 **Audit Panel Unlocked (Dữ liệu Drift lịch sử tồn đọng)**: Phát hiện đợt drift ngày **{', '.join(pending_drift_dates)}** vẫn còn đánh giá chưa được thẩm định! Vui lòng hoàn tất gắn nhãn để cung cấp nhãn vàng cho các đợt Retrain tiếp theo.")
            
        st.markdown("Double-click cells in the **Human Verified Label** column to assign correct ground-truth sentiments in bulk, then click the **Save All Bulk Edits** button!")
        
        if df_audit is not None and len(df_audit) > 0:

            # Human-in-the-Loop Agreement & Calibration Metrics
            audited_mask = (
                df_audit['verified_sentiment'].notna() &
                (df_audit['verified_sentiment'] != '') &
                (df_audit['verified_sentiment'].astype(str).str.lower() != 'none') &
                (df_audit['verified_sentiment'].astype(str).str.lower() != 'nan') &
                (df_audit['verified_sentiment'].astype(str).str.lower() != 'null')
            )
            df_verified = df_audit[audited_mask]
            total_audited = len(df_verified)
            total_pool = len(df_audit)
            
            if total_audited > 0:
                agreements = (df_verified['predicted_sentiment'].str.lower() == df_verified['verified_sentiment'].str.lower()).sum()
                agreement_rate = (agreements / total_audited) * 100
                overrides = total_audited - agreements
            else:
                agreement_rate = None
                overrides = 0

            st.markdown("#### 🤝 Human-AI Calibration & Audit Progress")
            col_m1, col_m2, col_m3, col_m4 = st.columns(4)
            with col_m1:
                st.metric("Audited Reviews", f"{total_audited:,}", help="Total reviews with ground-truth verified by human operators")
            with col_m2:
                if agreement_rate is not None:
                    st.metric("Human-AI Agreement", f"{agreement_rate:.1f}%", help="Percentage of AI predictions confirmed correct by human audit")
                else:
                    st.metric("Human-AI Agreement", "N/A", delta="Pending audit")
            with col_m3:
                st.metric("Human Overrides", f"{overrides:,}", help="Cases where human operator corrected AI prediction")
            with col_m4:
                cov = (total_audited / total_pool * 100) if total_pool > 0 else 0
                st.metric("Audit Coverage", f"{cov:.1f}%", help="Percentage of total review dataset audited")
            
            st.markdown("---")

            # Auditing Scope & Filter options
            col_scope, col_verified = st.columns(2)
            with col_scope:
                only_drifted = st.checkbox("Show only reviews from drifted dates", value=True)
            with col_verified:
                hide_verified = st.checkbox("Show only unverified reviews (where Human Verified Label is NULL)", value=True)
                
            # Optional Date Filter
            audit_dates = ["All Dates"] + sorted(list(df_audit['review_date'].dropna().unique()), reverse=True)
            selected_audit_date = st.selectbox("Filter audit table by date:", audit_dates)
                
            # Apply filters
            filtered_audit = df_audit.copy()
            if selected_audit_date != "All Dates":
                filtered_audit = filtered_audit[filtered_audit['review_date'] == selected_audit_date]
                
            if hide_verified:
                # Safely handle potential None, NaN, '' and 'none' strings using .str.lower()
                filtered_audit = filtered_audit[
                    filtered_audit['verified_sentiment'].isna() | 
                    (filtered_audit['verified_sentiment'] == '') | 
                    (filtered_audit['verified_sentiment'].astype(str).str.lower() == 'none') |
                    (filtered_audit['verified_sentiment'].astype(str).str.lower() == 'nan')
                ]
                
            if only_drifted:
                is_drifted_date = filtered_audit['review_date'].isin(drifted_dates)
                filtered_audit = filtered_audit[is_drifted_date]
                
            # Sort so lowest confidence is at the top (Uncertainty Sampling!)
            filtered_audit = filtered_audit.sort_values('confidence', ascending=True)
        
            if len(filtered_audit) > 0:
                display_df = filtered_audit[['review_id', 'review_date', 'category', 'review_text', 'predicted_sentiment', 'confidence', 'verified_sentiment']].copy()
                display_df['verified_sentiment'] = display_df['verified_sentiment'].apply(
                    lambda v: v if pd.notnull(v) and str(v).strip() != "" and str(v).lower() != "none" and str(v).lower() != "null" and str(v).lower() != "nan" else None
                )
                
                edited_df = st.data_editor(
                    display_df,
                    column_config={
                        "review_id": st.column_config.TextColumn("Review ID", disabled=True),
                        "review_date": st.column_config.TextColumn("Date", disabled=True),
                        "category": st.column_config.TextColumn("Category", disabled=True),
                        "review_text": st.column_config.TextColumn("Review Text", disabled=True, width="large"),
                        "predicted_sentiment": st.column_config.TextColumn("AI Prediction", disabled=True),
                        "confidence": st.column_config.NumberColumn("AI Confidence", disabled=True, format="%.4f"),
                        "verified_sentiment": st.column_config.SelectboxColumn(
                            "Human Verified Label",
                            options=["positive", "neutral", "negative"],
                            required=False
                        )
                    },
                    disabled=["review_id", "review_date", "category", "review_text", "predicted_sentiment", "confidence"],
                    key="bulk_audit_editor",
                    use_container_width=True, hide_index=True
                )
                
                # Confirmation Dialog for Bulk Approval
                if hasattr(st, "dialog"):
                    @st.dialog("⚠️ Xác Nhận Phê Duyệt Hàng Loạt (Bulk Approval)")
                    def confirm_bulk_approve_dialog(updates_to_run):
                        st.warning(f"Bạn có chắc chắn muốn phê duyệt **{len(updates_to_run)}** dự đoán AI làm nhãn vàng (Ground Truth) không?")
                        st.markdown("""
                        * **Dòng đã chỉnh sửa thủ công:** Giữ nguyên nhãn bạn đã chọn.
                        * **Dòng còn trống:** Tự động lấy nhãn dự đoán của AI (`predicted_sentiment`).
                        * **Hệ quả:** Dữ liệu sẽ lưu trực tiếp vào cơ sở dữ liệu (`store_reviews`) để cung cấp nhãn vàng cho các đợt Retrain tiếp theo.
                        """)
                        col_d1, col_d2 = st.columns(2)
                        with col_d1:
                            if st.button("✅ Đồng Ý Phê Duyệt", key="dlg_confirm_bulk", use_container_width=True, type="primary"):
                                try:
                                    with engine.begin() as conn:
                                        for upd in updates_to_run:
                                            conn.execute(
                                                text("UPDATE store_reviews SET verified_sentiment = :label WHERE review_id = :id"),
                                                {"label": upd["label"], "id": upd["id"]}
                                            )
                                    st.success(f"🎉 Đã phê duyệt và lưu thành công {len(updates_to_run)} bản ghi vào cơ sở dữ liệu!")
                                    st.cache_data.clear()
                                    st.rerun()
                                except Exception as ex:
                                    st.error(f"Lỗi khi phê duyệt hàng loạt: {ex}")
                        with col_d2:
                            if st.button("❌ Hủy Bỏ", key="dlg_cancel_bulk", use_container_width=True):
                                st.rerun()


                col_btn1, col_btn2 = st.columns(2)
                with col_btn1:
                    if st.button("💾 Save Manually Edited Rows Only", use_container_width=True):
                        updates = []
                        for _, row in edited_df.iterrows():
                            r_id = row['review_id']
                            orig_row = display_df[display_df['review_id'] == r_id].iloc[0]
                            orig_val = orig_row['verified_sentiment']
                            new_val = row['verified_sentiment']
                            
                            orig_val_str = str(orig_val).strip().lower() if pd.notnull(orig_val) else "none"
                            new_val_str = str(new_val).strip().lower() if pd.notnull(new_val) else "none"
                            
                            if orig_val_str != new_val_str:
                                db_val = new_val if pd.notnull(new_val) and str(new_val).strip() != "" else None
                                updates.append({"id": r_id, "label": db_val})
                        
                        if len(updates) > 0:
                            try:
                                with engine.begin() as conn:
                                    for upd in updates:
                                        conn.execute(
                                            text("UPDATE store_reviews SET verified_sentiment = :label WHERE review_id = :id"),
                                            {"label": upd["label"], "id": upd["id"]}
                                        )
                                st.success(f"🎉 Successfully saved {len(updates)} bulk edits!")
                                st.cache_data.clear()
                                st.rerun()
                            except Exception as ex:
                                st.error(f"Error saving bulk edits: {ex}")
                        else:
                            st.info("No modifications detected.")
                            
                with col_btn2:
                    if st.button("✅ Bulk Approve Remaining AI Predictions", use_container_width=True):
                        updates = []
                        for _, row in edited_df.iterrows():
                            r_id = row['review_id']
                            new_val = row['verified_sentiment']
                            
                            # If no manual verified_sentiment is entered, approve the AI predicted sentiment
                            if not pd.notnull(new_val) or str(new_val).strip() == "" or str(new_val).lower() == "none" or str(new_val).lower() == "nan":
                                db_val = row['predicted_sentiment']
                            else:
                                db_val = new_val
                                
                            updates.append({"id": r_id, "label": db_val})
                            
                        if len(updates) > 0:
                            if hasattr(st, "dialog"):
                                confirm_bulk_approve_dialog(updates)
                            else:
                                st.session_state["pending_bulk_updates"] = updates
                                st.rerun()
                        else:
                            st.info("No predictions found to approve.")

                # Fallback inline confirmation for environments without st.dialog
                if not hasattr(st, "dialog") and "pending_bulk_updates" in st.session_state and st.session_state["pending_bulk_updates"]:
                    pending = st.session_state["pending_bulk_updates"]
                    st.warning(f"⚠️ **Xác nhận:** Bạn có chắc chắn muốn phê duyệt **{len(pending)}** dự đoán AI làm nhãn vàng không?")
                    c_f1, c_f2 = st.columns(2)
                    with c_f1:
                        if st.button("✅ Đồng Ý Phê Duyệt", key="fallback_confirm_bulk", use_container_width=True, type="primary"):
                            try:
                                with engine.begin() as conn:
                                    for upd in pending:
                                        conn.execute(
                                            text("UPDATE store_reviews SET verified_sentiment = :label WHERE review_id = :id"),
                                            {"label": upd["label"], "id": upd["id"]}
                                        )
                                st.session_state.pop("pending_bulk_updates", None)
                                st.success(f"🎉 Đã phê duyệt và lưu thành công {len(pending)} bản ghi!")
                                st.cache_data.clear()
                                st.rerun()
                            except Exception as ex:
                                st.error(f"Lỗi: {ex}")
                    with c_f2:
                        if st.button("❌ Hủy Bỏ", key="fallback_cancel_bulk", use_container_width=True):
                            st.session_state.pop("pending_bulk_updates", None)
                            st.rerun()
            else:
                st.success("🎉 No reviews requiring manual audit found for the selected filters!")
        else:
            st.info("No reviews found in database for auditing.")
