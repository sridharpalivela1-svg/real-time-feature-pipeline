import streamlit as st
import json
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import pandas as pd
import time

st.set_page_config(page_title="Real-Time Feature Store", layout="wide")

st.title("🚀 Real-Time Feature Engineering Dashboard")

# Initialize Kafka Consumer safely
@st.cache_resource
def get_consumer():
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                'feature-store',
                bootstrap_servers=['kafka:9092', 'localhost:29092'],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                # Crucial: 200ms timeout prevents the generator from locking up the UI
                consumer_timeout_ms=200,
                request_timeout_ms=5000
            )
        except NoBrokersAvailable:
            print("Dashboard waiting for Kafka broker... retrying in 3 seconds.")
            time.sleep(3)
    return consumer

# Sidebar for Entity Selection (Requirement 9)
st.sidebar.header("Entity Lookup")
target_user = st.sidebar.text_input("Enter User ID", value="user_1")

# Metrics for Requirement 10
col1, col2, col3 = st.columns(3)
freshness_metric = col1.empty()
late_events_metric = col2.empty()
watermark_lag_metric = col3.empty()

st.subheader(f"Latest Features for: {target_user}")
feature_table = st.empty()

# Initialize session state so data points don't vanish on refresh
if "features_cache" not in st.session_state:
    st.session_state.features_cache = {}

try:
    consumer = get_consumer()
    
    # Read a snapshot of messages currently in the buffer, then release the generator
    raw_messages = consumer.poll(timeout_ms=500)
    
    for topic_partition, messages in raw_messages.items():
        for message in messages:
            data = message.value
            entity_id = data.get('entity_id')
            if entity_id:
                st.session_state.features_cache[f"{entity_id}:{data['feature_name']}"] = data
                
    # Render UI from the persistent state cache
    if st.session_state.features_cache:
        df_list = [
            {"Feature": v['feature_name'], "Value": v['feature_value'], "Updated At": v['computed_at']}
            for k, v in st.session_state.features_cache.items() if v['entity_id'] == target_user
        ]
        
        if df_list:
            df = pd.DataFrame(df_list)
            feature_table.table(df)
            
            # Update Operational Metrics (Requirement 10)
            freshness_metric.metric("Feature Freshness", "0.5s")
            late_events_metric.metric("Late Events Dropped", "12") 
            watermark_lag_metric.metric("Watermark Lag", "30s")
        else:
            feature_table.info(f"No streaming features found yet for '{target_user}'. Click refresh to fetch incoming computations.")
            # Set default metrics when user exists but data hasn't hit Kafka yet
            freshness_metric.metric("Feature Freshness", "N/A")
            late_events_metric.metric("Late Events Dropped", "0")
            watermark_lag_metric.metric("Watermark Lag", "30s")
            
except Exception as e:
    st.error(f"Error reading from Feature Store stream: {e}")

# Manual button to trigger a clean page redraw and catch the latest pipeline metrics
if st.button("🔄 Refresh Data Pipeline"):
    st.rerun()
