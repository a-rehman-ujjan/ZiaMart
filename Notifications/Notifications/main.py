import asyncio
from fastapi import FastAPI
from Notifications.kafka import consume_messages

app = FastAPI(title="Zia Mart Notification Service...",)

loop = asyncio.get_event_loop()
task = loop.create_task(consume_messages())

@app.get('/')
async def root():
    return {"message": "Welcome to the Zia Mart's Notification Service"}