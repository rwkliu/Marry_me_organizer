import pika
import time
import os
from worker import Worker

security_worker = Worker("security.high.accident", "high.accident")
security_worker.consume(None)
