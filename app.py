from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
import json
import threading
import os
from kafka import KafkaProducer, KafkaConsumer
import requests
from bs4 import BeautifulSoup

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key'
socketio = SocketIO(app, cors_allowed_origins="*")

BROKER = 'localhost:9092'
DEFINITION_TOPIC = 'definitions'
RESULTS_TOPIC = 'word-count-results'

def fetch_definition(word):
    """Fetches word definition from dictionary.com"""
    url = f"https://www.dictionary.com/browse/{word}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    definition_tag = soup.find('div', id='dictionary-entry-1')
    return definition_tag.text.strip() if definition_tag else None

def kafka_consumer_thread():
    """Background thread to consume Kafka messages and emit to WebSocket"""
    consumer = KafkaConsumer(
        RESULTS_TOPIC,
        bootstrap_servers=BROKER,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest'
    )
    
    for message in consumer:
        result = message.value
        socketio.emit('word_count_result', result)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/submit_word', methods=['POST'])
def submit_word():
    word = request.json.get('word', '').strip()
    
    if not word:
        return jsonify({'error': 'Word is required'}), 400
    
    definition = fetch_definition(word)
    if not definition:
        return jsonify({'error': 'Definition not found'}), 404
    
    # Send to Kafka
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    message = {
        "word": word,
        "definition": definition,
        "response_topic": RESULTS_TOPIC
    }
    
    producer.send(DEFINITION_TOPIC, value=message)
    producer.close()
    
    return jsonify({'success': True, 'word': word, 'definition': definition})

if __name__ == '__main__':
    # Start Kafka consumer in background thread
    consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()
    
    socketio.run(app, debug=True, host='0.0.0.0', port=9001, allow_unsafe_werkzeug=True)