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
routine = json.loads(os.environ["ROUTINE"])

# Constants
PROCESSING_TIME = 3
ROUTINE_START_MESSAGE = "Start_routine"

# Flags
routine_active = False
pause_event = asyncio.Event()

# Set the event (True) so messages can be consumed immediately
pause_event.set()


async def start_routine():
    global pause_event, routine_active

    routine_active = True
    time_unavailable = routine["unavailable"]
    time_available = routine["available"]

    while True:
        print(f"Pausing message consumption for {time_unavailable} seconds")
        pause_event.clear()
        await asyncio.sleep(time_unavailable)

        print(f"Resuming message consumption for {time_available} seconds")
        pause_event.set()
        await asyncio.sleep(time_available)


async def on_message(message: aio_pika.IncomingMessage):
    global routine_active

    print("processing message")
    message_content = message.body.decode()
    print(f"Received {message_content}")

    if message_content == ROUTINE_START_MESSAGE and routine_active == False:
        print("Starting routine")
        asyncio.create_task(start_routine())

    # Process the message
    await asyncio.sleep(PROCESSING_TIME)
    print("event handled")
    await message.ack()


async def consume(channel, channel_number):
    print("setting up a channel")
    await channel.declare_exchange(exchange, "topic")

    for binding in bindings:
        queue = await channel.declare_queue(queue_name, durable=True)
        await queue.bind(binding["priority"], binding["routing_key"])

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            await pause_event.wait()  # Wait for the available window
            await on_message(message)


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
