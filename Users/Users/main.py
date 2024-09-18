from typing import Annotated
import asyncio
from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaProducer
from Users.kafka import consume_messages, create_topic, kafka_producer
from Users import settings
from sqlmodel import Session, select
from Users import user_pb2
from typing import Annotated, List
from Users.functions import authenticate_user
from Users.authentication import create_access_token, get_password_hash, get_current_user
from Users.modules import Users, User, TokenResponse, Update_User, Change_Password, Usertoken
from Users.db import create_tables, get_session
from fastapi.middleware.cors import CORSMiddleware

# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

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

app = FastAPI(lifespan=lifespan,
              title="Zia Mart User Service..."
              )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can restrict this to specific origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods, including OPTIONS
    allow_headers=["*"],  # Allow all headers
)


@app.get('/')
async def root():
    return {"message": "Welcome to the Zia Mart"}

@app.post('/register', response_model=User)
async def Create_User(
    New_user:User,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    hashed_password = get_password_hash(New_user.password)
    pb_product = user_pb2.user()
    pb_product.id = New_user.id
    pb_product.username = New_user.name
    pb_product.email = New_user.email
    pb_product.password = hashed_password
    pb_product.usertype = user_pb2.Type.CUSTOMER
    pb_product.type = user_pb2.Operation.CREATE
    serialized_product = pb_product.SerializeToString()
    await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)
    return New_user

@app.post('/register-as-admin', response_model=User)
async def Create_Admin(
    New_user:User,
    Store_Password: str,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)]
):
    if Store_Password != settings.ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid Admin Password")
    else:
        hashed_password = get_password_hash(New_user.password)
        pb_product = user_pb2.user()
        pb_product.id = New_user.id
        pb_product.username = New_user.name
        pb_product.email = New_user.email
        pb_product.password = hashed_password
        pb_product.usertype = user_pb2.Type.ADMIN
        pb_product.type = user_pb2.Operation.CREATE
        serialized_product = pb_product.SerializeToString()
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)
        return New_user

# GET token to fetch all products
@app.post('/token', response_model=TokenResponse)
async def login_user(form_data: OAuth2PasswordRequestForm = Depends(),
 db: Session = Depends(get_session)):
    user = authenticate_user(session=db, username=form_data.username, password=form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail="Invalid User Credentials...")
    access_token = create_access_token(data={"id": user.id, "name": user.name, "email": user.email, "usertype": user.user_type})
    return  {"access_token": access_token, "token_type": "bearer"}
    
# Post endpoint to fetch all products
@app.get('/users', response_model=List[Users])
async def get_all_users(session: Annotated[Session, Depends(get_session)], current_user: Annotated[Usertoken, Depends(get_current_user)]):
    if current_user.get("usertype") != user_pb2.Type.ADMIN:
        raise HTTPException(status_code=401, detail="Only Admins can access this endpoint")
    else:
        all_users = session.exec(select(Users)).all()
        return all_users

@app.get('/users/{user_id}', response_model=Users)
async def get_user(user_id: int, db: Session = Depends(get_session)):
    user = db.get(Users, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user

@app.put('/users/{user_id}', response_model=Update_User)
async def update_profile(
    user_id: int,
    user: Update_User,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
    current_user: Annotated[Usertoken, Depends(get_current_user)],
    db: Session = Depends(get_session)
):
    db_user = db.get(Users, user_id)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    elif current_user.get("id") != user_id:
        raise HTTPException(status_code=401, detail="You Do Not Have Permissions to access this endpoint")
    else:
        pb_product = user_pb2.user()
        pb_product.id = user_id
        pb_product.username = user.name
        pb_product.email = user.email
        pb_product.type = user_pb2.Operation.UPDATE
        serialized_product = pb_product.SerializeToString()
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)
        return user

@app.put('/users/password/{user_id}', response_model=Change_Password)
async def change_password(
    user_id: int,
    password: Change_Password,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
    current_user: Annotated[Usertoken, Depends(get_current_user)],
    db: Session = Depends(get_session)
):
    db_user = db.get(Users, user_id)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    elif current_user.get("id") != user_id:
        raise HTTPException(status_code=401, detail="You Do Not Have Permissions to access this endpoint")
    else:
        hashed_password = get_password_hash(password.password)
        pb_product = user_pb2.user()
        pb_product.id=user_id
        pb_product.password=hashed_password
        pb_product.type=user_pb2.Operation.UPDATEPASSWORD
        serialized_product = pb_product.SerializeToString()
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)
        return password

@app.delete('/users/{user_id}')
async def delete_user(
    user_id: int,
    producer: Annotated[AIOKafkaProducer, Depends(kafka_producer)],
    current_user: Annotated[Usertoken, Depends(get_current_user)],
    db: Session = Depends(get_session)
):
    db_user = db.get(Users, user_id)
    if db_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    elif current_user.get("id") == user_id:
        pb_product = user_pb2.user()
        pb_product.id=user_id
        pb_product.type=user_pb2.Operation.DELETE
        serialized_product = pb_product.SerializeToString()
        await producer.send_and_wait(settings.KAFKA_ORDER_TOPIC, serialized_product)
        return {"message": "Your Profile is Deleted"}
    else:
        raise HTTPException(status_code=401, detail="You Do Not Have Permissions to Delete Other Users")