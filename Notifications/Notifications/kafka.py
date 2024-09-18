from twilio.rest import Client
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from Notifications import settings, notification_pb2
from Notifications.modules import Notification, Orders
from Notifications.db import engine
from Notifications.functions import Order_Notification

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
    await create_topic()
    # Create a consumer instance.
    consumer = AIOKafkaConsumer(
        settings.KAFKA_ORDER_TOPIC,
        bootstrap_servers=settings.BOOTSTRAP_SERVER,
        group_id=settings.KAFKA_CONSUMER_GROUP_ID,
        auto_offset_reset='earliest',
        session_timeout_ms=45000,
        max_poll_interval_ms=600000,  # Increase poll interval
        max_poll_records=10  # Reduce batch size
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
                    product = notification_pb2.order()
                    product.ParseFromString(msg.value)
                    print(f"Received message from Kafka: {msg.value}")
                    # Here you can add code to process each message.
                    print(f"Processing message: {product}")                  
                    message = f"New Order was placed successfully! /n Order ID: {product.id} /n User ID: {product.user_id} /n User Name: {product.user_name} /n Product ID: {product.product_id} /n Product Name: {product.product_name} /n Quantity: {product.quantity} /n Total Price: {product.total_price}"
                    print(f"Sending Notification for Order ID: {product.id}")
                    await Order_Notification(message)
                    print(f'''Notification Sent for Order ID: {product.id}''')
                    
                except Exception as e:  # Catch deserialization errors
                    print(f"Error processing message: {e}")
            else:
                print("No message received from Kafka.")
    finally:
        # Ensure to close the consumer when done.
        print("Stopping consumer...")
        await consumer.stop()