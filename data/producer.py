import json
import datetime
import time
from kafka import KafkaProducer
import websocket

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    api_version=(0, 10, 1),
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def on_message(ws, message):
    data = json.loads(message)
    for asset, price in data.items():
        payload = {
            "order_id": str(datetime.datetime.now().timestamp()),
            "customer_id": "LIVE_API",
            "product_id": asset.upper(),
            "quantity": float(price),
            "order_date": datetime.datetime.now().isoformat(),
            "status": "active"
        }
        producer.send('user_orders', value=payload)
        print(f"📡 API Ingest: {asset.upper()} at ${price}")

def on_error(ws, error):
    print(f"⚠️ API Notice: {error}. Retrying...")

def on_close(ws, close_status_code, close_msg):
    print("🔌 Connection closed. Restarting in 3 seconds...")

def run_api():
    socket_url = "wss://ws.coincap.io/prices?assets=bitcoin,ethereum,solana,dogecoin"
    ws = websocket.WebSocketApp(
        socket_url, 
        on_message=on_message, 
        on_error=on_error, 
        on_close=on_close
    )
    ws.run_forever()

if __name__ == "__main__":
    print("🚀 Starting Persistent Crypto API Stream...")
    while True:
        try:
            run_api()
        except Exception as e:
            print(f"❌ Critical Error: {e}")
        time.sleep(3) # Wait before reconnecting
