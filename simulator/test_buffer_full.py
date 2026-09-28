from simulator.producer import KafkaEventProducer
from simulator.equipment import EQUIPMENTS
from simulator.main import generate_event

def main():
    producer = KafkaEventProducer()

    for i in range(100):
        event = generate_event(EQUIPMENTS[0])

        print(f"send attempt: {i}")

        producer.send(key=event.equipment_id, payload=event.to_dict())

    producer.flush()

if __name__ == "__main__":
    main()
