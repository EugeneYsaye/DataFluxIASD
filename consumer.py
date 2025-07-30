import json

from kafka import KafkaConsumer

BROKER = 'localhost:9092'
TOPIC = 'word-count-results'


def consume_word_counts():
    """
    Consumes word count results from Kafka and displays them.
    """
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

    print("Waiting for word count results...")
    for message in consumer:
        result = message.value
        print(f"Word Count Result: {result}")


if __name__ == "__main__":
    consume_word_counts()
