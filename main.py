from fastapi import FastAPI, Request
import redis
import json
import requests
import asyncio

# Create app instances
app = FastAPI()
redis_client = redis.Redis()

@app.post("/webhook")
async def handle_webhook(request: Request):
    event = await request.json()
    # Storing the event in the Redis message queue, push
    redis_client.rpush("task_queue", json.dumps(event))
    return {"message": "Event received and buffered"}

# Not needed here, as this goes into it's own app
# @app.post("/backendservice", status_code=201)
# async def handle_backend(request: Request):
#     event = await request.json()
#     print(event)
#     print("Received")
#     # Forward the event
#     return {"message": "Event forwarded successfully"}

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