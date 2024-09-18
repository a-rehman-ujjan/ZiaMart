from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from sqlmodel import Session
from Users import settings, user_pb2
from Users.modules import Users
from Users.db import engine


async def create_topic():
    admin_client = AIOKafkaAdminClient(
        bootstrap_servers=settings.BOOTSTRAP_SERVER)
    await admin_client.start()
    topic_list = [NewTopic(name=settings.KAFKA_ORDER_TOPIC,
                           num_partitions=2, replication_factor=1)]
    try:
        await admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{settings.KAFKA_ORDER_TOPIC}' created successfully")
    except Exception as e:
        print(f"Failed to create topic '{settings.KAFKA_ORDER_TOPIC}': {e}")
    finally:
        await admin_client.close()

async def consume_messages():
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        settings.KAFKA_ORDER_TOPIC,
        bootstrap_servers=settings.BOOTSTRAP_SERVER,
        group_id=settings.KAFKA_CONSUMER_GROUP_ID,
        auto_offset_reset='earliest',
        # session_timeout_ms=30000,  # Example: Increase to 30 seconds
        # max_poll_records=10,
    )
    # Start the consumer.
    await consumer.start()
    print("consumer started")
    try:
        # Continuously listen for messages.
        async for msg in consumer:
            if msg is not None:
                try:
                    user = user_pb2.user()
                    user.ParseFromString(msg.value)            
                    data_from_producer=Users(id=user.id, name= user.username, email= user.email, password=user.password, user_type=user.usertype) 
                    print(f"Received message from Kafka: {msg.value}")
                    # Here you can add code to process each message.
                    with Session(engine) as session:
                        if user.type == user_pb2.Operation.CREATE:
                            session.add(data_from_producer)
                            session.commit()
                            session.refresh(data_from_producer)
                            print(f'''Stored Protobuf data in database with ID: {user.id}''')
                        elif user.type == user_pb2.Operation.UPDATE:
                            updated_user = session.get(Users, user.id)
                            if updated_user:
                                updated_user.name = user.username
                                updated_user.email = user.email
                                session.commit()
                                session.refresh(updated_user)
                                print(f'''Updated Protobuf data in database with ID: {user.id}''')
                            else:
                                print(f"User with ID: {data_from_producer.id} not found")
                        elif user.type == user_pb2.Operation.UPDATEPASSWORD:
                            updated_password = session.get(Users, user.id)
                            if updated_password:
                                updated_password.password = user.password
                                session.commit()
                                session.refresh(updated_password)
                                print(f'''Updated Protobuf data in database with ID: {user.id}''')
                            else:
                                print(f"User with ID: {data_from_producer.id} not found")
                        elif user.type == user_pb2.Operation.DELETE:
                            deleted_user = session.get(Users, user.id)
                            if deleted_user:
                                session.delete(deleted_user)
                                session.commit()
                                print(f"Deleted Protobuf data in database with ID: {user.id}")
                            else:
                                print(f"User with ID: {data_from_producer.id} not found")
                except Exception as e:  # Catch deserialization errors
                    print(f"Error processing message: {e}")
            else:
                print("No message received from Kafka.")
    finally:
        # Ensure to close the consumer when done.
        await consumer.stop()

async def kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=settings.BOOTSTRAP_SERVER)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()