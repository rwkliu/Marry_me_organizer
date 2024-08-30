import asyncio
import aio_pika
import os


amqp_url = os.environ["AMQP_URL"]
security_queue_name = "security.high.accident"


async def consume(security_queue_name, channel_number):
    print("Set up a connection")
    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        print("setting up a channel")
        channel = await connection.channel()
        await channel.declare_exchange("coordinator", "topic")
        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue(security_queue_name, durable=True)
        await queue.bind("coordinator", "high.accident")

        async def on_message(message: aio_pika.IncomingMessage):
            print("processing message")
            async with message.process():
                print(f"Channel {channel_number}: Received {message.body.decode()}")

        await queue.consume(on_message)
        await asyncio.Future()


async def main():
    tasks = []
    for i in range(3):
        task = asyncio.create_task(consume(security_queue_name, i + 1))
        tasks.append(task)

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
