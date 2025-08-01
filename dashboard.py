from flask import Flask, render_template, jsonify
from confluent_kafka import Consumer
import json
import threading
from collections import deque
from datetime import datetime

app = Flask(__name__)
fraud_alerts = deque(maxlen=100)  # Keep last 100 alerts

def consume_alerts():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'dashboard-group',
        'auto.offset.reset': 'latest'
    })
    consumer.subscribe(['fraud-alerts'])
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
                
            alert = json.loads(msg.value().decode('utf-8'))
            alert['received_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            fraud_alerts.appendleft(alert)
            print(f"New fraud alert: {alert['transaction_id']}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/api/alerts')
def get_alerts():
    return jsonify(list(fraud_alerts))

if __name__ == '__main__':
    # Start Kafka consumer in background
    consumer_thread = threading.Thread(target=consume_alerts, daemon=True)
    consumer_thread.start()
    
    app.run(debug=True, host='0.0.0.0', port=9001)