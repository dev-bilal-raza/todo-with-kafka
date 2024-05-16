import json
from aiokafka import AIOKafkaProducer # type: ignore
from aiokafka.admin import AIOKafkaAdminClient # type:ignore
from fastapi import FastAPI

app = FastAPI()

@app.get("/", response_model=str)
def home():
    return "Welcome to Todo Project!"

@app.get("/produce", response_model=str)
async def produce(todo: str):
    producer = AIOKafkaProducer(bootstrap_servers=["broker:19092"])
    await producer.start()
    return todo
    try:
        await producer.send(topic="todo", value=json.dumps(todo).encode("utf-8")) # convert into byte code from json
        # await producer.send(topic="todo", value=b"Dinner") # convert into byte code using "b"
    except:
        print("Error While sending data from producer")  
    finally:
        await producer.stop() 
        