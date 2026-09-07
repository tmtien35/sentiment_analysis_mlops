import os
import streamlit as st
import pandas as pd
import requests
from sqlalchemy import create_engine

st.set_page_config(
    page_title="Inference Monitor",
    layout="wide",
    initial_sidebar_state="collapsed"
)

def get_db_engine():
    db_url = os.environ.get("DATABASE_URL", "sqlite:///data/results.db")
    return create_engine(db_url)

@st.cache_data(ttl=2)
def load_data():
    engine = get_db_engine()
    try:
        with engine.connect() as conn:
            df_preds = pd.read_sql("SELECT * FROM predictions", con=conn)
            df_drift = pd.read_sql("SELECT * FROM drift_metrics ORDER BY batch_date ASC", con=conn)
            try:
                df_logs = pd.read_sql("SELECT * FROM inference_logs WHERE review_text != 'init' ORDER BY timestamp DESC", con=conn)
        except Exception:
            df_logs = pd.DataFrame()
        return df_preds, df_drift, df_logs, None
    except Exception as e:
        return None, None, None, str(e)

st.title("🛍️ E-Commerce Review Sentiment & Drift Monitor")
st.markdown("---")

df_preds, df_drift, df_logs, error = load_data()

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
        for col in ['positive', 'neutral', 'negative']:
            if col not in daily_trends.columns:
                daily_trends[col] = 0
        st.line_chart(daily_trends, color=["#e74c3c", "#95a5a6", "#2ecc71"])

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

    st.markdown("---")
    col_test, col_table = st.columns([2, 3])
    
    with col_test:
        st.markdown("### ⚡ Live On-Demand Scoring")
        st.markdown("Test scoring using our real-time **FastAPI endpoint**.")
        user_input = st.text_area("Enter custom review text to score:", placeholder="Type a review here...")
        
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
                        if sent == "positive":
                            st.success(f"**Predicted Sentiment:** POSITIVE ({conf:.1f}% confidence)")
                        elif sent == "negative":
                            st.error(f"**Predicted Sentiment:** NEGATIVE ({conf:.1f}% confidence)")
                        else:
                            st.warning(f"**Predicted Sentiment:** NEUTRAL ({conf:.1f}% confidence)")
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

    st.markdown("---")
    st.markdown("### 🖥️ Live API Traffic Monitor (Real-Time Inference Logs)")
    if df_logs is not None and len(df_logs) > 0:
        st.dataframe(
            df_logs[['timestamp', 'review_text', 'cleaned_text', 'predicted_sentiment', 'confidence']],
            height=200
        )
    else:
        st.info("No live on-demand API requests logged yet. Submit a prediction above to test!")
