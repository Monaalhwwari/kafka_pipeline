from confluent_kafka import Consumer
import pandas as pd
import time
import json
import pyarrow as pa
consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "test-group",
    "auto.offset.reset": "earliest",
}

batch_data = []
batch_interval = 120
start_time = time.time()

consumer = Consumer(consumer_config)
consumer.subscribe(["binance-crypto-tickers"])

consumer.subscribe(["binance-crypto-tickers"])

print("Consumer is initialized and listening")

try:

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        try:
                data = json.loads(msg.value().decode())
                batch_data.append(data)
                current_time = time.time()
                if current_time - start_time > batch_interval:
                    df = pd.DataFrame(data=batch_data)
                    file_name = f"data_batch_{int(time.time())}.parquet"
                    df.to_parquet(file_name, engine='pyarrow', index=False)
                    batch_data = []
                    start_time = time.time()
                    print("done creating file")
        except Exception as e:
            print(f"Failed to parse message: {e}")
except KeyboardInterrupt:
        print("Closing stream")
        consumer.close()
finally:
        print("end")