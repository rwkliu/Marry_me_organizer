import pika
import os


class Worker:
    def __init__(self, queue_name, queue_routing_key):
        # Create the connection to rabbitmq
        amqp_url = os.environ["AMQP_URL"]
        url_params = pika.URLParameters(amqp_url)
        connection = pika.BlockingConnection(url_params)

        # Worker attributes
        self.channel = connection.channel()
        self.queue_name = queue_name
        self.queue_routing_key = queue_routing_key

        # Declare the exchange, queue, and binding
        exchange = "coordinator"
        self.channel.exchange_declare(exchange=exchange, exchange_type="topic")
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.queue_bind(
            exchange=exchange, queue=queue_name, routing_key=queue_routing_key
        )

    def consume(self, msg_handler):
        def handle_msg_default(ch, method, properties, body):
            print("received message: ", body.decode("utf-8"))
            print("sending ack")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        # Make sure the consumer receives only one message at a time
        # next message is received only after acking the previous one
        self.channel.basic_qos(prefetch_count=1)

        if msg_handler is None:
            msg_handler = handle_msg_default

        # Define the queue consumption
        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=msg_handler
        )

        print("Waiting to consume")
        # Start consuming
        self.channel.start_consuming()
