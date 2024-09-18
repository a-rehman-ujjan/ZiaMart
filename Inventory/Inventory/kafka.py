from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from sqlmodel import Session
from Inventory import inventory_pb2, settings
from Inventory.modules import Products
from Inventory.db import engine


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
        with Session(engine) as session:    
        # Continuously listen for messages.
            async for msg in consumer:
                if msg is not None:
                    try:
                        Inventory_Update = inventory_pb2.product()
                        Inventory_Update.ParseFromString(msg.value)
                        db_product = session.get(Products, Inventory_Update.id)
                        print(f"Received message from Kafka: {msg.value}")
                        if db_product:
                            db_product.quantity += Inventory_Update.quantity
                            db_product.added_by = "Admin01"
                            session.add(db_product)
                            session.commit()
                            session.refresh(db_product)
                            # logging.info(f"Updated Product With ID: {Inventory_Update.product_id}")
                        else:
                            print("error")
                            # logging.warning(f"Product with ID: {Inventory_Update.product_id} not Found for update")
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