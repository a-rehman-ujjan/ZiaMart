from typing import Annotated
import asyncio
from fastapi import Depends, FastAPI, BackgroundTasks, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from Products.kafka import consume_messages, create_topic, kafka_producer
from Products import settings
from sqlmodel import SQLModel, Field, create_engine, Session, select
from Products import product_pb2
from typing import Annotated, List, Optional
from sqlalchemy import create_engine
from Products.db import create_tables, get_session
from Products.functions import get_product
from Products.modules import Add_Product, Products, Update_Product, Usertoken
from Products.auth import get_current_user


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="localhost:8000/token")


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Fastapi app started...")
    print('Creating Tables')
    create_tables()
    print("Tables Created")
    await create_topic()
    loop = asyncio.get_event_loop()
    task = loop.create_task(consume_messages())
    yield
    task.cancel()
    await task

app = FastAPI(lifespan=lifespan,
              title="Zia Mart Product Service...",
              version='1.0.0'
              )

@app.get('/')
async def root():
    return {"message": "Welcome to the Zia Mart's Product Service"}


@app.post('/products', response_model=Add_Product)
async def Add_New_Product(
    New_product:Add_Product,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
    current_user: Annotated[Usertoken, Depends(get_current_user)]
):
    if current_user.get("usertype")==1:
        pb_product = product_pb2.product()
        pb_product.id=New_product.id
        pb_product.name=New_product.name
        pb_product.price=New_product.price
        pb_product.quantity=0
        pb_product.added_by=current_user.get("name")
        pb_product.type=product_pb2.Operation.CREATE
        serialized_product = pb_product.SerializeToString()
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)
        return New_product
    else:
        raise HTTPException(status_code=401, detail="You are not authorized to add the product...")

# GET token to fetch all products
@app.get('/products/{product_id}', response_model=Products)
async def Get_Product( product_id: int, db: Session = Depends(get_session)):
    product_data = get_product(session=db, product_id=product_id)
    if not product_data:
        raise HTTPException(status_code=404, detail="No Product was found With that ID...")
    return product_data

# Post endpoint to fetch all products
@app.get('/products', response_model=List[Products])
async def Get_All_Products(session: Annotated[Session, Depends(get_session)]):
    all_products = session.exec(select(Products)).all()
    return all_products

@app.put('/products/{product_id}', response_model=Update_Product)
async def Update_Product(
    product_id: int,
    product: Update_Product,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
    current_user: Annotated[Usertoken, Depends(get_current_user)],
    db: Session = Depends(get_session)
):
    if get_product(session=db, product_id=product_id) is None:
        raise HTTPException(status_code=404, detail="No Product was found With that ID...")
    elif current_user.get("usertype")==1:
        pb_product = product_pb2.product()
        pb_product.id=product_id
        pb_product.name=product.name
        pb_product.price=product.price
        pb_product.quantity=product.quantity
        pb_product.added_by=current_user.get("name")
        pb_product.type=product_pb2.Operation.UPDATE
        serialized_product = pb_product.SerializeToString()
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)
        return product
    else:
        raise HTTPException(status_code=401, detail="You are not authorized to update the product...")

@app.delete('/products/{product_id}', response_model=Products)
async def Delete_Product(
    product_id: int,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
    current_user: Annotated[Usertoken, Depends(get_current_user)],
    db: Session = Depends(get_session)
):
    if get_product(session=db, product_id=product_id) is None:
        raise HTTPException(status_code=404, detail="No Product was found With that ID...")
    elif current_user.get("usertype")==1:
        pb_product = product_pb2.product()
        pb_product.id=product_id
        pb_product.type=product_pb2.Operation.DELETE
        serialized_product = pb_product.SerializeToString()
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)
        return {"message": "Product Deleted Successfully..."}
    else:
        raise HTTPException(status_code=401, detail="You are not authorized to delete the product...")