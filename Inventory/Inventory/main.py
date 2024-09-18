from typing import Annotated
import asyncio
from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import OAuth2PasswordBearer
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer
from Inventory import settings
from sqlmodel import Session
from Inventory import inventory_pb2
from typing import Annotated
from Inventory.modules import Products, Inventory_Item, Usertoken
from Inventory.kafka import create_topic, consume_messages, kafka_producer
from Inventory.db import get_session
from Inventory.auth import get_current_user

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Fastapi app started...")
    # print('Creating Tables')
    # create_tables()
    await create_topic()
    # print("Tables Created")
    loop = asyncio.get_event_loop()
    task = loop.create_task(consume_messages())
    yield
    task.cancel()
    await task

app = FastAPI(lifespan=lifespan,
              title="Zia Mart Inventory Service...",
              version='1.0.0'
              )

@app.get('/')
async def root():
    return {"message": "Welcome to the Zia Mart's Inventory Service"}

# Get endpoint to check product quantity
@app.get('/product/inventory/{product_id}', response_model=Inventory_Item)
async def Get_Product_Quantity(id: int, session: Session = Depends(get_session)):
    product = session.get(Products, id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return Inventory_Item(product_id=product.id, quantity=product.quantity)


@app.post('/product/inventory/{product_id}', response_model=Inventory_Item)
async def Update_Quantity(
    Product_id: int,
    Quantity: int,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
    current_user: Usertoken = Depends(get_current_user),
    session: Session = Depends(get_session)
):
    product = session.get(Products, Product_id)
    if product:
        if current_user.get("usertype") == 1:
            new_product = inventory_pb2.product()
            new_product.id=Product_id
            new_product.quantity=Quantity
            serialized_product = new_product.SerializeToString()
            await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)
            return Inventory_Item(product_id=product.id, quantity=Quantity)
        else:
            raise HTTPException(status_code=401, detail="You are not authorized to perform this action")
    else:
        raise HTTPException(status_code=404, detail="Product not found")