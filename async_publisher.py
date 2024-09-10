import aio_pika
import uuid
import asyncio


async def on_response(message: aio_pika.IncomingMessage):
    print(f"Received message: {message.body.decode()}")
    await message.ack()


async def publish():
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost")

    channel = await connection.channel()

    exchange = await channel.declare_exchange("high", aio_pika.ExchangeType.TOPIC)

    correlation_id = str(uuid.uuid4())

    callback_queue = await channel.declare_queue("callback", durable=True)

    await callback_queue.bind(exchange, "callback")

    await callback_queue.consume(on_response)

    message_body = f"event: high priority cleanup {correlation_id}"
    print(f"sending event: {message_body}")
    await exchange.publish(
        aio_pika.Message(
            body=message_body.encode(),
            correlation_id=correlation_id,
            reply_to="callback",
        ),
        routing_key="dirty_table.cleanup",
    )

    print("waiting for response...")
    await asyncio.Future()


async def main():
    await publish()


if __name__ == "__main__":
    asyncio.run(main())
