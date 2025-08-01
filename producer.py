import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

def generate_transaction():
    return {
        "user_id": f"u{random.randint(1000, 9999)}",
        "transaction_id": f"t-{random.randint(100000, 999999)}",
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "currency": random.choice(["EUR", "USD", "GBP"]),
        "timestamp": datetime.now().isoformat(),
        "location": fake.city(),
        "method": random.choice(["credit_card", "debit_card", "paypal", "crypto"])
    }

def main():
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    
    print("Starting transaction producer...")
    try:
        while True:
            transaction = generate_transaction()
            producer.produce('transactions', json.dumps(transaction))
            producer.poll(0)
            print(f"Sent: {transaction['transaction_id']} - ${transaction['amount']}")
            time.sleep(random.uniform(.2, .5))
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
