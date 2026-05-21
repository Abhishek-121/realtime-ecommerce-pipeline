import json

from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='order-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

required_fields = ["order_id", "user_id", "amount"]

for message in consumer:
    order = message.value
    print(f"Received Event: {order}")
    missing_fields = []
    for field in required_fields:
        if field not in order:
            missing_fields.append(field)

    # If fields missing -> send to DLQ
    if missing_fields:
        print(f"Invalid Event. Missing fields: {missing_fields}")
        dlq_payload = {
            "failed_event": order,
            "missing_fields": missing_fields,
            "reason": "Schema validation failed"
        }
        producer.send("failed_orders",value=dlq_payload)
    else:
        producer.send("processed_orders", value=order)
        print("Event Processed Successfully to processed_order topic...")

