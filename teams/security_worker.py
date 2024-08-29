import pika
import time
import os
from worker import Worker


def handle_event(ch, method, properties, body):
    print("This message was received: ", body.decode("utf-8"))
    ch.basic_ack(delivery_tag=method.delivery_tag)


security_worker = Worker("security.high.accident", "high.accident")
security_worker.consume(handle_event)
