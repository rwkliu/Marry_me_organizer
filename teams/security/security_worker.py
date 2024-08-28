import pika
import time
import os

# Read rabbitmq connection url from environment variable
amqp_url = os.environ["AMQP_URL"]
url_params = pika.URLParameters(amqp_url)

# Connect to rabbitmq
connection = pika.BlockingConnection(url_params)
channel = connection.channel()

# Declare the coordinator exchange
channel.exchange_declare(exchange="coordinator", exchange_type="topic")

# Declare a new queue
# Durable flag is set so that messages are retained between restarts
security_queue_name = "security.high.accident"
channel.queue_declare(queue=security_queue_name, durable=True)

# Bind the queue to the exchange
channel.queue_bind(
    exchange="coordinator", queue=security_queue_name, routing_key="high.accident"
)


def receive_msg(ch, method, properties, body):
    print("received msg : ", body.decode("utf-8"))
    print("acking it")
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Make sure the consumer receives only one message at a time
# next message is received only after acking the previous one
channel.basic_qos(prefetch_count=1)

# Define the queue consumption
channel.basic_consume(queue=security_queue_name, on_message_callback=receive_msg)

print("Waiting to consume")
# Start consuming
channel.start_consuming()
