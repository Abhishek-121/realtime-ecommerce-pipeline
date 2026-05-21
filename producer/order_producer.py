import json
import random
import time
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

PRODUCTS = [
    "Laptop",
    "Phone",
    "Tablet",
    "Watch",
    "Headphones"
]

CITIES = [
    "Bangalore",
    "Delhi",
    "Mumbai",
    "Chennai",
    "Hyderabad"
]

while True:
    order = {
        "order_id": fake.uuid4(),
        "user_id": random.randint(1000, 9999),
        "product": random.choice(PRODUCTS),
        "amount": round(random.uniform(100, 5000), 2),
        "event_time": datetime.utcnow().isoformat()
    }
    # Introduce bad records intentionally
    if random.randint(1, 5) == 1:
        del order["user_id"] #user_id is required field
    producer.send("orders", value=order)
    print(f"Produced: {order}")
    time.sleep(2)
