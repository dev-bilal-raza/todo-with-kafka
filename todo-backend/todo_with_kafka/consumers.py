import json
from aiokafka import AIOKafkaConsumer, ConsumerStoppedError # type:ignore
from .settings import topic1

[
    {
        "topics": "add_todo",
            "value": "Dinner"
    }
]

async def consumer_func():
    consumer = AIOKafkaConsumer(str(topic1), bootstrap_servers=["broker:19092"], auto_offset_reset="earliest")
    await consumer.start()
    async for msg in consumer:
        try: 
            
            print("Topic", msg.topic, "Message", json.loads(msg.value).decode("utf-8"))
        except ConsumerStoppedError as e:
            print(f"Error while consuming message from {topic1}: ", e)
        finally:
            await consumer.stop()