# DataFlux Word Count Pipeline - Web Interface

A real-time data processing pipeline that fetches word definitions and analyzes word frequency using Kafka, Spark Streaming, and Flask.

## Architecture

- **Web Interface** (Flask): Submit words and view results in real-time
- **Producer** (Web/Python): Fetches word definitions from dictionary.com
- **Spark Streaming** (Scala): Processes definitions and counts word frequency
- **Consumer** (Web/Python): Displays results via WebSocket

## Quick Start

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Start Kafka 
   ```
   ./kafka-start.sh
   ```

3. Start Spark application:
   ```
   cd spark-word-count-streaming
   ./sbt package
   ./spark-env.sh
   ./run-app.sh
   ```

4. Start web interface:
   ```
   python app.py
   ```
   Or use: `run_web.bat`

5. Open browser to `http://localhost:9001`
ß
## Usage

1. Enter a word in the web interface
2. The system fetches the definition and sends it to Kafka
3. Spark processes the definition and counts word frequency
4. Results appear in real-time on the web page

## Files

- `app.py` - Flask web application with WebSocket support
- `templates/index.html` - Web interface
- `producer.py` - Original console producer (still functional)
- `consumer.py` - Original console consumer (still functional)
