import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Configuration
BOOTSTRAP_SERVERS = 'kafka:9092'
USER_EVENTS_TOPIC = 'user-events'
METADATA_TOPIC = 'content-metadata'

# Robust connection initialization loop
producer = None
print("Waiting for Kafka broker to become available...")
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: str(v).encode('utf-8'),
            request_timeout_ms=10000
        )
        print("Successfully connected to Kafka!")
    except NoBrokersAvailable:
        print("Kafka broker not available yet. Retrying in 5 seconds...")
        time.sleep(5)

def get_timestamp(seconds_delay=0):
    """Generates ISO 8601 timestamp, optionally delayed for late events."""
    ts = datetime.utcnow() - timedelta(seconds=seconds_delay)
    return ts.strftime('%Y-%m-%dT%H:%M:%SZ')

def produce_metadata():
    print("Publishing initial content metadata...")
    categories = ['Sci-Fi', 'Drama', 'Action', 'Comedy']
    for i in range(1, 11):
        content_id = f"movie_{i}"
        data = {
            "content_id": content_id,
            "category": random.choice(categories),
            "creator_id": f"creator_{random.randint(1, 5)}",
            "publish_timestamp": get_timestamp()
        }
        producer.send(METADATA_TOPIC, key=content_id, value=data)
    producer.flush()

def produce_user_events():
    print("Starting real-time user event simulation...")
    user_ids = [f"user_{i}" for i in range(1, 6)]
    event_types = ['view', 'click', 'like', 'share']
    
    while True:
        user_id = random.choice(user_ids)
        content_id = f"movie_{random.randint(1, 10)}"
        
        # Requirement 4: 5% Late Events (35-90 seconds in the past)
        is_late = random.random() < 0.05
        delay = random.randint(35, 90) if is_late else 0
        
        event = {
            "user_id": user_id,
            "content_id": content_id,
            "event_type": random.choice(event_types),
            "dwell_time_ms": random.randint(1000, 30000),
            "timestamp": get_timestamp(delay)
        }
        
        producer.send(USER_EVENTS_TOPIC, key=user_id, value=event)
        if is_late:
            print(f"Sent LATE event for {user_id}")
        
        time.sleep(random.uniform(0.5, 2.0)) # Simulate real-time gap

if __name__ == "__main__":
    produce_metadata()
    produce_user_events()
