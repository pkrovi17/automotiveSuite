#!/usr/bin/env python3
"""
recent_kafka_messages.py
Reads and prints recent messages from Kafka topic 'iot_video'.
"""

from kafka import KafkaConsumer
import json

# Kafka connection details
KAFKA_BROKER = "localhost:9092"   # change if your broker is elsewhere
TOPIC_NAME = "iot_video"

def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="latest",  # change to "earliest" to see all past messages
        enable_auto_commit=True,
        value_deserializer=lambda m: m.decode("utf-8", errors="ignore"),
    )

    print(f"Listening for messages on topic '{TOPIC_NAME}'...\n")
    try:
        for msg in consumer:
            value = msg.value
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                parsed = value  # not JSON, print raw
            print(f"[partition={msg.partition}, offset={msg.offset}] {parsed}")
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
