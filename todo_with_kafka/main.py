import json
from aiokafka import AIOKafkaProducer # type:ignore
from aiokafka.errors import KafkaTimeoutError # type:ignore
from aiokafka.admin import AIOKafkaAdminClient  # type:ignore
from fastapi import Body, FastAPI
# import requests

app = FastAPI()

@app.get("/", response_model=str)
def home():
    return "Welcome to Todo Project!"

@app.get("/produce", response_model=str)
async def produce(todo: str):
    producer = AIOKafkaProducer(bootstrap_servers=["broker:19092"])
    await producer.start()
    try:
        # convert into byte code from json
        await producer.send(topic="todo", value=json.dumps(todo).encode("utf-8"))
        
        # await producer.send(topic="todo", value=b"Dinner") # convert into byte code using "b"
    except KafkaTimeoutError as e:
        print("Error While sending data from producer", e)
    finally:
        await producer.stop()
        return todo

# @app.post("/orders")
# async def create_order(order: str = Body(...)):
#     # Call user service to get username
#     user_response = requests.get(f"http://user-service:8000/users/{order.user_id}")
#     if user_response.status_code == 200:
#         user_data = user_response.json()
#         username = user_data["name"]
#     else:
#         return {"message": "User not found"}
#     new_order = Order(user_id=order.user_id, item_name=order.item_name, username=username)
#     # Implement logic to store order (replace with database interaction)
#     print(f"Order created for user {username} (ID: {order.user_id}) - Item: {order.item_name}")
#     return new_order