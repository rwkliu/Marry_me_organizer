import pika
import time
import os

# Read rabbitmq connection url from environment variable
# amqp_url = os.environ["AMQP_URL"]
# url_params = pika.URLParameters(amqp_url)
params = pika.ConnectionParameters("localhost")
exchange = "high"
security_queue_name = "catering"

# Connect to rabbitmq
connection = pika.BlockingConnection(params)
channel = connection.channel()

# Declare the coordinator exchange
channel.exchange_declare(exchange=exchange, exchange_type="topic")

# Declare a new queue
# Durable flag is set so that messages are retained between restarts
channel.queue_declare(queue=security_queue_name, durable=True)

# Bind the queue to the exchange
channel.queue_bind(
    exchange=exchange, queue=security_queue_name, routing_key="bad_food.catering"
)
channel.queue_bind(
    exchange=exchange, queue=security_queue_name, routing_key="music.catering"
)
channel.queue_bind(
    exchange=exchange, queue=security_queue_name, routing_key="feeling_ill.catering"
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
