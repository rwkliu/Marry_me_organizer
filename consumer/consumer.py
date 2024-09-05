import asyncio
import aio_pika
import os
import argparse
import json


amqp_url = os.environ["AMQP_URL"]


async def consume(queue_name, channel, channel_number, exchange, bindings):
    print("setting up a channel")
    await channel.declare_exchange(exchange, "topic")
    queue = await channel.declare_queue(queue_name, durable=True)

    for binding in bindings:
        await queue.bind(binding["priority"], binding["routing_key"])

    async def on_message(message: aio_pika.IncomingMessage):
        print("processing message")
        async with message.process():
            print(f"Channel {channel_number}: Received {message.body.decode()}")
            processing_time = 3
            await asyncio.sleep(processing_time)
            print("event handled")

    await queue.consume(on_message)
    await asyncio.Future()


async def main(queue_name, num_channels, exchange, bindings):
    # Establish a connection
    print("Set up a connection")
    connection = await aio_pika.connect_robust(amqp_url)

    # Create multiple channels from the same connection
    channels = [await connection.channel() for _ in range(num_channels)]

    # Set QoS for each channel
    for channel in channels:
        await channel.set_qos(prefetch_count=1)

    # Start a consumer for each channel
    tasks = []
    for i, channel in enumerate(channels):
        task = asyncio.create_task(
            consume(queue_name, channel, i + 1, exchange, bindings)
        )
        tasks.append(task)

    await asyncio.gather(*tasks)


parser = argparse.ArgumentParser(description="Set up the consumer")
parser.add_argument(
    "--queue_name",
    help="The name of the queue that the consumer will consume messages from.",
)
parser.add_argument(
    "--channels", help="The number of channels (workers) over the connection", type=int
)
parser.add_argument(
    "--exchange",
    help="The name of the exchange. The exchange type will be a topic exchange",
)
parser.add_argument(
    "--queue_bindings",
    help="A list of queue bindings. The format of this argument shall be a list of dictionaries, \
      where each dictionary contains the key-value pairs for the priority (low, medium, high) and the routing key",
    type=str,
)

if __name__ == "__main__":
    args = parser.parse_args()
    queue_name = args.queue_name
    num_channels = args.channels
    exchange = args.exchange
    bindings = json.loads(args.queue_bindings)
    asyncio.run(main(queue_name, num_channels, exchange, bindings))
