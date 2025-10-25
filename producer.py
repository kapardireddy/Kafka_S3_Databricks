from kafka import KafkaProducer
import json, time, random, uuid
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=10
)

def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.randint(1000, 9999),
        "amount": round(random.uniform(1.0, 10000.0), 2),
        "currency": random.choice(["USD", "EUR", "GBP"]),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "category": random.choice(["food", "electronics", "travel", "fashion"])
    }

if __name__ == "__main__":
    try:
        while True:
            msg = generate_transaction()
            producer.send("transaction_logs", msg)
            producer.flush()
            print("sent:", msg)
            time.sleep(0.5)   # produce ~2 messages/sec
    except KeyboardInterrupt:
        print("shutting down producer")
        producer.close()
