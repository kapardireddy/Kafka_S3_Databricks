from kafka import KafkaConsumer
import json, time, os
import pandas as pd
from datetime import datetime

BOOTSTRAP = 'localhost:9092'
TOPIC = 'transaction_logs'
BATCH_SIZE = 100
BATCH_TIMEOUT = 30  # seconds

REQUIRED = ["transaction_id", "user_id", "amount", "currency", "timestamp"]

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='transaction-validators',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

os.makedirs('./out/transactions/valid_transactions', exist_ok=True)
os.makedirs('./out/transactions/invalid_transactions', exist_ok=True)

batch = []
last_flush = time.time()

def flush_batch(batch_list):
    if not batch_list:
        return
    df = pd.DataFrame(batch_list)
    # ensure required columns exist so operations below don't fail
    for c in REQUIRED:
        if c not in df.columns:
            df[c] = None
    valid = df.dropna(subset=REQUIRED)
    invalid = df[~df.index.isin(valid.index)]
    timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
    if not valid.empty:
        path = f'./out/transactions/valid_transactions/valid_{timestamp}.parquet'
        valid.to_parquet(path, index=False)
        print(f'WROTE {len(valid)} valid rows -> {path}')
    if not invalid.empty:
        path = f'./out/transactions/invalid_transactions/invalid_{timestamp}.parquet'
        invalid.to_parquet(path, index=False)
        print(f'WROTE {len(invalid)} invalid rows -> {path}')

try:
    for msg in consumer:
        batch.append(msg.value)
        now = time.time()
        if len(batch) >= BATCH_SIZE or (now - last_flush) >= BATCH_TIMEOUT:
            flush_batch(batch)
            batch = []
            last_flush = now
except KeyboardInterrupt:
    print("Interrupted, flushing remaining")
    flush_batch(batch)
finally:
    consumer.close()
