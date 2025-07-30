import json
from kafka import KafkaProducer
import requests
from bs4 import BeautifulSoup

BROKER = 'localhost:9092'
TOPIC = 'definitions'


def fetch_definition(word):
    """
    Fetches the definition of a word from a dictionary API or web scraping.
    """
    url = f"https://www.dictionary.com/browse/{word}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    # print(soup)
    definition_tag = soup.find('div', id='dictionary-entry-1')
    return definition_tag.text.strip() if definition_tag else None


def produce_word_and_definition():
    """
    Continuously reads words from the user, fetches definitions, and sends them to Kafka.
    """
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    try:
        while True:
            word = input("Enter a word (or type 'exit' to quit): ").strip()
            if word.lower() == 'exit':
                print("Exiting producer...")
                break

            definition = fetch_definition(word)
            if not definition:
                print("ERROR: Definition not found.")
                continue
            message = {
                "word": word, 
                "definition": definition,
                "response_topic": "word-count-results"  # Add this field
            }
            producer.send(TOPIC, value=message)
            print(f"Sent to Kafka: {message}")
    except KeyboardInterrupt:
        print("\nProducer stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    produce_word_and_definition()
