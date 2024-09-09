import asyncio
import aio_pika
import os
import json


# Environment variables
amqp_url = os.environ["AMQP_URL"]
queue_name = os.environ["QUEUE_NAME"]
num_channels = int(os.environ["NUM_CHANNELS"])
exchange = os.environ["EXCHANGE"]
bindings = json.loads(os.environ["QUEUE_BINDINGS"])

# Constants
PROCESSING_TIME = 3
ROUTINE_START_MESSAGE = "Start_standard_routine"

# Flags
routine_active = False


async def start_routine():
    global routine_active

    while True:
        print("Pausing message consumption for 20 seconds")
        routine_active = True
        await asyncio.sleep(20)

        print("Resuming message consumption for 5 seconds")
        routine_active = False
        await asyncio.sleep(5)


async def on_message(message: aio_pika.IncomingMessage):
    global routine_active

    if not routine_active:
        print("processing message")
        async with message.process():
            message_content = message.body.decode()
            print(f"Received {message_content}")
            await asyncio.sleep(PROCESSING_TIME)
            print("event handled")

            if message_content == ROUTINE_START_MESSAGE:
                print("Starting routine")
                await start_routine()


async def consume(channel, channel_number):
    print("setting up a channel")
    await channel.declare_exchange(exchange, "topic")

    for binding in bindings:
        queue = await channel.declare_queue(queue_name, durable=True)
        await queue.bind(binding["priority"], binding["routing_key"])

    await queue.consume(on_message)
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
