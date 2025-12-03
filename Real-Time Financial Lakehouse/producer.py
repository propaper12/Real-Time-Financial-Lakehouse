import json
import websocket
from kafka import KafkaProducer
from datetime import datetime

KAFKA_TOPIC = 'market_data'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def on_message(ws, message):
    data = json.loads(message)
    processed_data = {
        'symbol': 'BTCUSDT',
        'price': float(data['p']),
        'quantity': float(data['q']),
        'timestamp': datetime.fromtimestamp(data['T'] / 1000).isoformat()
    }
    producer.send(KAFKA_TOPIC, value=processed_data)
    print(f"Sent to Kafka: {processed_data}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Bağlantı Kapandı")

def on_open(ws):
    print("Binance WebSocket Bağlandı - Veri Akıyor")

if __name__ == "__main__":
    socket_url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    ws = websocket.WebSocketApp(socket_url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()