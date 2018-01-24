import redis
import random
import threading
import time
from datetime import datetime

from ratelimit.ratelimit import RateLimit


rds = redis.Redis(db=2)
network = 123456789
rate = 2        # 2/sec
limit = 2


class Worker(threading.Thread):

    def run(self):
        ratelimit = RateLimit(rds, network, rate, limit)
        task_id = 0
        while task_id < 100:
            if ratelimit.acquire(1):
                now = datetime.now()
                print('[{}] Worker {}, run task {}'.format(now.strftime('%H:%M:%S'), threading.get_ident(), task_id))
                task_id += 1
                time.sleep(random.randint(0, 1000) / 1000)
            else:
                # In actual celery tasks, use:
                # self.retry(exc=exc, countdown=1)
                time.sleep(1)
            

for i in range(4):
    worker = Worker()
    time.sleep(random.randint(0, 100) / 1000)
    worker.start()
