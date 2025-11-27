from confluent_kafka import Producer
import json
import uuid



producer_config = {'bootstrap.servers': 'localhost:9092'}

producer = Producer(producer_config)
order = {
    "order_id":str(uuid.uuid4()),
    "user": "Aadi",
    "item": "Yellow egg",
    "quantity": 10 
}


def delivery_report(err,msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Deliverd: {msg.value().decode("utf-8")}")
        print(f"Delivered to {msg.topic()}: partition {msg.partition()}: at offset {msg.offset()}")
value = json.dumps(order).encode("utf-8")

producer.produce(topic='orders',value=value,callback = delivery_report)

producer.flush()
