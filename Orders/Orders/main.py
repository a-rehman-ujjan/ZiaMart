from typing import Annotated
import asyncio
from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import OAuth2PasswordBearer
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer
from Orders import settings
from sqlmodel import Session, select
from Orders import order_pb2
from typing import Annotated, List
from Orders.kafka import consume_messages, create_topic, kafka_producer
from Orders.db import create_tables, get_session, engine
from Orders.modules import Orders, Order, Usertoken
from Orders.functions import Get_Product_Quantity, get_order
from Orders.auth import get_current_user

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Fastapi app started...")
    print('Creating Tables')
    create_tables()
    await create_topic()
    print("Tables Created")
    loop = asyncio.get_event_loop()
    task = loop.create_task(consume_messages())
    yield
    task.cancel()
    await task

app = FastAPI(lifespan=lifespan, title="Zia Mart Order Service...",)

@app.get('/')
async def root():
    return {"message": "Welcome to the Zia Mart's Order Service"}


@app.post('/orders', response_model=Order)
async def Create_Order(
    New_Order:Order,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
    current_user: Annotated[Usertoken, Depends(get_current_user)]
):
    pb_order = order_pb2.order()
    pb_order.id=New_Order.id
    pb_order.product_id=New_Order.product_id
    pb_order.quantity=New_Order.quantity
    check_Quantity = await Get_Product_Quantity(id=pb_order.product_id, session=Session(engine))
    if check_Quantity is not None:
        if check_Quantity.quantity >= New_Order.quantity and check_Quantity.quantity != 0:
            pb_order.user_name = current_user.get("name")
            pb_order.user_id = current_user.get("id")
            pb_order.product_name = check_Quantity.name
            pb_order.total_price = check_Quantity.price * New_Order.quantity
            serialized_product = pb_order.SerializeToString()
            await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)
            await producer.send_and_wait(settings.KAFKA_NOTIFICATION_TOPIC, serialized_product)
            return New_Order
        else:
            raise HTTPException(status_code=400, detail="Not Enough Quantity Available in Inventory")
    else:
        raise HTTPException(status_code=404, detail="Product Not Found")


@app.get('/orders/{order_id}', response_model=Orders)
async def Get_Order(
    order_id: int,
    current_user: Annotated[Usertoken, Depends(get_current_user)],
    db: Session = Depends(get_session)
):
    product_data = get_order(session=db, order_id=order_id)
    if product_data:
        if current_user.get("id")== product_data.user_id or current_user.get("usertype")==1:
            return product_data
        else:
            raise HTTPException(status_code=401, detail="You are not authorized to view the order...")
    else:
        raise HTTPException(status_code=404, detail="No Order was found With that ID...")

# Post endpoint to fetch all products
@app.get('/orders', response_model=List[Orders])
async def Get_All_Orders(
    session: Annotated[Session, Depends(get_session)],
    current_user: Annotated[Usertoken, Depends(get_current_user)]
):
    if current_user.get("usertype")==1:
        all_products = session.exec(select(Orders)).all()
        return all_products
    else:
        raise HTTPException(status_code=401, detail="You are not authorized to view the orders...")