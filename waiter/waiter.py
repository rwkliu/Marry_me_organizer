import asyncio
import aio_pika
import os
import json


amqp_url = os.environ["AMQP_URL"]
queue_names = json.loads(os.environ["QUEUE_NAMES"])
num_channels = int(os.environ["NUM_CHANNELS"])
exchange = os.environ["EXCHANGE"]
bindings = json.loads(os.environ["QUEUE_BINDINGS"])


async def on_message(message: aio_pika.IncomingMessage):
    print("processing message")
    async with message.process():
        print(f"Received {message.body.decode()}")
        processing_time = 3
        await asyncio.sleep(processing_time)
        print("event handled")


async def consume(channel, channel_number):
    print("setting up a channel")
    await channel.declare_exchange(exchange, "topic")

    queues = []
    for i, binding in enumerate(bindings):
        queues.append(await channel.declare_queue(queue_names[i], durable=True))
        await queues[i].bind(binding["priority"], binding["routing_key"])
        await queues[i].consume(on_message)

    await asyncio.Future()


async def main():
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
        task = asyncio.create_task(consume(channel, i + 1))
        tasks.append(task)

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
