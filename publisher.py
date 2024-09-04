import pika
from json_parser import read_file, sort_contents

connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()

file_name = "dataset_1.json"
file_path = "./data/" + file_name
events = sort_contents(read_file(file_path))

event_routing_key_table = {
    "brawl": "brawl.security",
    "accident": "accident.security",
    "not_on_list": "not_on_list.security",
    "dirty_table": "dirty_table.cleanup",
    "broken_items": "broken_items.cleanup",
    "dirty_floor": "dirty_floor.cleanup",
    "bad_food": "bad_food.catering",
    "music": "music.catering",
    "feeling_ill": "feeling_ill.catering",
    "bride": "bride.officiant",
    "groom": "groom.officiant",
}

channel.exchange_declare(exchange="high", exchange_type="topic")
channel.exchange_declare(exchange="medium", exchange_type="topic")
channel.exchange_declare(exchange="low", exchange_type="topic")

for event in events:
    exchange = event["priority"].lower()
    routing_key = event_routing_key_table[event["event_type"]]
    description = event["description"]
    if exchange == "high":
        channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=description,
            properties=pika.BasicProperties(delivery_mode=2),
        )
        print("event sent: ", exchange, routing_key, description)

channel.close()
connection.close()
