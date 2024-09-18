from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from Orders import order_pb2, settings
from Orders.modules import Orders, Products
from Orders.db import engine
from sqlmodel import Session
import aiohttp

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
                    product = order_pb2.order()
                    product.ParseFromString(msg.value)
                    data_from_producer=Orders(id=product.id, user_id= product.user_id, user_name=product.user_name, product_id=product.product_id, product_name=product.product_name, quantity=product.quantity, total_price=product.total_price)
                    print(f"Received message from Kafka: {msg.value}")
                    # Here you can add code to process each message.
                    with Session(engine) as session:
                        session.add(data_from_producer)
                        session.commit()
                        session.refresh(data_from_producer)
                        print(f'''Stored Protobuf data in database with ID: {data_from_producer.id}''')

                    db_product = session.get(Products, product.product_id)
                    db_product.quantity += -(product.quantity)
                    db_product.added_by = "Admin01"
                    session.add(db_product)
                    session.commit()
                    session.refresh(db_product)

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