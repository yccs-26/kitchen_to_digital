from producer import KafkaEventProducer
from equipment import EQUIPMENTS
from main import generate_event

def main():
    producer = KafkaEventProducer()

    for i in range(100):
        event = generate_event(EQUIPMENTS[0])

        print(f"send attempt: {i}")

        producer.send(event)

if __name__ == "__main__":
    main()