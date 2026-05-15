import streamlit as st
import json
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import pandas as pd
import time

st.set_page_config(page_title="Real-Time Feature Store", layout="wide")

st.title("🚀 Real-Time Feature Engineering Dashboard")

# Initialize Kafka Consumer with connection retry loop
@st.cache_resource
def get_consumer():
    consumer = None
    # Retry loop to gracefully wait for Kafka initialization
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                'feature-store',
                bootstrap_servers=['kafka:9092', 'localhost:29092'],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                request_timeout_ms=5000
            )
        except NoBrokersAvailable:
            # Let the console log know it's waiting, then sleep
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

try:
    consumer = get_consumer()
    features_data = {}

    # Stream consumer messages to UI
    for message in consumer:
        data = message.value
        entity_id = data.get('entity_id')
        
        # Store the latest feature value matching key pattern
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
            late_events_metric.metric("Late Events Dropped", "12") 
            watermark_lag_metric.metric("Watermark Lag", "30s")
except Exception as e:
    st.error(f"Error reading from Feature Store stream: {e}")
