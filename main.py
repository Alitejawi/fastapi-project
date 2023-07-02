from fastapi import FastAPI, Request
import redis
import json
import requests
import asyncio
from pydantic import BaseModel

# Create app instances
app = FastAPI()
redis_client = redis.Redis()

class Event(BaseModel):
    event_id: int
    event_name: str
    # Add more fields as needed

@app.post("/webhook")
async def handle_webhook(request: Request, event: Event):
    # Storing the event in the Redis message queue, push
    redis_client.rpush("task_queue", event.json())
    return {"message": "Event received and buffered"}

async def process_events():
    while True:
        # Retrieve the event from the Redis message queue, pop
        event = redis_client.lpop("task_queue")
        if event:
            # Forward the event to the backend service
            try:
                response = requests.post("http://127.0.0.1:8002/backendservice", json=json.loads(event.decode("utf-8")), timeout=5, verify=False)
                response.raise_for_status()
                print("Event forwarded successfully")
            except requests.exceptions.RequestException as e:
                print(f"Failed to forward event: {str(e)}")
        await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(process_events())