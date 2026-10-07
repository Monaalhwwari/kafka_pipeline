import websocket
from confluent_kafka import Producer
import json
import time

conf = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(conf)

TOPIC_NAME = "binance-crypto-tickers"

symbols = [
    "btcusdt", "ethusdt", "solusdt", "bnbusdt", "xrpusdt",
    "adausdt", "dogeusdt", "avaxusdt", "linkusdt", "dotusdt",
]

streams = "/".join([f"{symbol}@ticker" for symbol in symbols])
SOCKET = f"wss://stream.binance.com:9443/stream?streams={streams}"


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def on_open(ws):
    print("Connected successfully to Binance WebSocket")


def on_message(ws, message):
    try:
        data = json.loads(message)
        payload = data.get("data", {})

        symbol = payload.get("s")
        price = payload.get("c")
        high = payload.get("h")
        low = payload.get("l")

        if symbol:
            crypto_data = {
                "symbol": symbol,
                "price": price,
                "high": high,
                "low": low,
            }

            json_bytes = json.dumps(crypto_data).encode('utf-8')

            producer.produce(TOPIC_NAME, value=json_bytes, callback=delivery_report)
            producer.poll(0)

    except Exception as e:
        print(f"Parsing error: {e}")


def on_close(ws, close_status_code, close_msg):
    print(f"Connection closed. Code: {close_status_code}, Message: {close_msg}")
    producer.flush()


if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        SOCKET,
        on_open=on_open,
        on_message=on_message,
        on_close=on_close,
    )

    ws.run_forever(ping_interval=30, ping_timeout=10)