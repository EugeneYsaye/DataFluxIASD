# Real-Time Fraud Detection Pipeline

A complete real-time analytics pipeline for detecting fraudulent payment transactions using Kafka and Spark Structured Streaming.

## Architecture

- **Kafka Producer**: Simulates payment transactions (2 to 5 times per second)
- **Spark Streaming**: Processes transactions and detects fraud patterns
- **Output Sinks**: Console, Parquet files, and Kafka alerts topic
- **Dashboard**: Flask web UI for real-time fraud monitoring

## Fraud Detection Rules

1. **High-value transactions**: Amount > $1000
2. **High frequency**: >3 transactions from same user in 1 minute
3. **Location anomaly**: Multiple countries within 5 minutes

## Setup

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Start Kafka** (requires Docker):
   ```bash
   python start_pipeline.py
   ```

## Running the Pipeline

1. **Start the transaction producer**:
   ```bash
   python producer.py
   ```

2. **Set up the kafka connection**:
   ```bash
   python setup-kafka.py
   ```

3. **Start the fraud detection pipeline**:
   ```bash
   python fraud_detector.py
   ```

4. **Start the dashboard**:
   ```bash
   python dashboard.py
   ```
   Then visit: http://localhost:9001

## Output

- **Console**: Real-time fraud alerts
- **Parquet files**: Stored in `./fraud_output/`
- **Kafka topic**: `fraud-alerts` for downstream consumers
- **Dashboard**: Web UI at http://localhost:5000

## Sample Transaction

```json
{
  "user_id": "u1234",
  "transaction_id": "t-001",
  "amount": 185.20,
  "currency": "EUR",
  "timestamp": "2025-06-04T10:12:33Z",
  "location": "Paris",
  "method": "credit_card"
}
```