from fastapi import FastAPI, Request
import redis
import json
import requests
import asyncio
from pydantic import BaseModel
from rq import Queue

# Create app instances for FastAPI, Redis and the queue
app = FastAPI()
redis_client = redis.Redis()
queue = Queue(connection=redis_client)

# Defining the BaseModel with id and a string
class Event(BaseModel):
    event_id: int
    event_name: str = 'Event name as default'
    # For optional fields, use below
    #event_datetime: Optional[datetime] = None

# Handling the webhook endpoint
@app.post("/webhook")
async def handle_webhook(request: Request, event: Event):
    # Storing the event in the Redis message queue, push
    redis_client.rpush("task_queue", event.json())
    return {"message": "Event received and buffered"}

# Process the events from the Redis queue
async def process_events():
    while True:
        # Retrieve the event from the Redis message queue, pop
        event = redis_client.lpop("task_queue")
        if event:
            # Enqueue the event from the queue for backend forwarding
            try:
                queue.enqueue(forward_event, json.loads(event.decode("utf-8")))
                print("Event added to the queue")
            except Exception as e:
                print(f"Failed to add event to the queue: {str(e)}")
        await asyncio.sleep(5)

# This is where we actually forward the event to the backend service
def forward_event(event):
    try:
        response = requests.post("http://127.0.0.1:8002/backendservice", json=event, timeout=5, verify=False)
        response.raise_for_status()
        print("Event forwarded successfully")
    except requests.exceptions.RequestException as e:
        print(f"Failed to forward event: {str(e)}")

# Starting up the events at the app startup
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(process_events())