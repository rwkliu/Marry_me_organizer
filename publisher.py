import pika

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

route_key = "high.accident"
message = "high accident event"

channel.basic_publish(
    exchange="coordinator",
    routing_key=route_key,
    body=message,
    properties=pika.BasicProperties(delivery_mode=2),
)
print("message sent!")
channel.close()
connection.close()
