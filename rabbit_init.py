import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()

channel.exchange_declare(exchange="coordinator", exchange_type="direct")

Security_queue = channel.queue_declare(queue="Security", durable=True)
Waiters_queue = channel.queue_declare(queue="Waiters", durable=True)
Clean_up_queue = channel.queue_declare(queue="Clean_up", durable=True)
Catering_queue = channel.queue_declare(queue="Catering", durable=True)
Officiant_queue = channel.queue_declare(queue="Officiant", durable=True)

Security_queue_name = Security_queue.method.queue
Waiters_queue_name = Waiters_queue.method.queue
Clean_up_queue_name = Clean_up_queue.method.queue
Catering_queue_name = Catering_queue.method.queue
Officiant_queue_name = Officiant_queue.method.queue

channel.queue_bind(exchange="coordinator", queue=Security_queue_name)
channel.queue_bind(exchange="coordinator", queue=Waiters_queue_name)
channel.queue_bind(exchange="coordinator", queue=Clean_up_queue_name)
channel.queue_bind(exchange="coordinator", queue=Catering_queue_name)
channel.queue_bind(exchange="coordinator", queue=Officiant_queue_name)
