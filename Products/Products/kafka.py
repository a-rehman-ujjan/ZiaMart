from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from sqlmodel import Session, select
from Products import settings
from Products import product_pb2
from Products.modules import Products
from Products.db import engine


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
                    product = product_pb2.product()
                    product.ParseFromString(msg.value)            
                    data_from_producer=Products(id=product.id, name= product.name, price= product.price, quantity=product.quantity, added_by=product.added_by) 
                    print(f"Received message from Kafka: {msg.value}")
                    # Here you can add code to process each message.
                    with Session(engine) as session:
                        if product.type == product_pb2.Operation.CREATE:
                            session.add(data_from_producer)
                            session.commit()
                            session.refresh(data_from_producer)
                            print(f'''Stored Protobuf data in database with ID: {data_from_producer.id}''')
                        if product.type == product_pb2.Operation.UPDATE:
                            existing_product = session.get(Products, product.id)
                            if product:
                                existing_product.name = product.name
                                existing_product.price = product.price
                                existing_product.quantity = product.quantity
                                existing_product.added_by = product.added_by
                                session.commit()
                                session.refresh(product)
                                print(f'''Updated Protobuf data in database with ID: {product.id}''')
                            else:
                                print(f"Product with ID: {product.id} not found in database")   
                        if product.type == product_pb2.Operation.DELETE:
                            existing_product = session.exec(select(Products).where(Products.id == product.id)).one()
                            session.delete(product)
                            session.commit()
                            print(f'''Deleted Protobuf data in database with ID: {product.id}''')
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