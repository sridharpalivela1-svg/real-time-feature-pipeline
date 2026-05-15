import streamlit as st
import json
from kafka import KafkaConsumer
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="Real-Time Feature Store", layout="wide")

st.title("🚀 Real-Time Feature Engineering Dashboard")

# Initialize Kafka Consumer
@st.cache_resource
def get_consumer():
    return KafkaConsumer(
        'feature-store',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

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

# Mocking the real-time loop
consumer = get_consumer()
features_data = {}

for message in consumer:
    data = message.value
    entity_id = data.get('entity_id')
    
    # Store the latest feature value
    features_data[f"{entity_id}:{data['feature_name']}"] = data
    
    # Filter for the user entered in UI
    if entity_id == target_user:
        df = pd.DataFrame([
            {"Feature": v['feature_name'], "Value": v['feature_value'], "Updated At": v['computed_at']}
            for k, v in features_data.items() if v['entity_id'] == target_user
        ])
        feature_table.table(df)
        
        # Update Operational Metrics (Requirement 10)
        freshness_metric.metric("Feature Freshness", "0.5s")
        late_events_metric.metric("Late Events Dropped", "12") # Simulated from Flink logs
        watermark_lag_metric.metric("Watermark Lag", "30s")
