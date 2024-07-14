import os
import redis
import threading
from rq import Worker, Queue, Connection
from tasks import process_query_task

listen = ['default']
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
conn = redis.Redis.from_url(redis_url)

class ThreadedWorker(Worker):
    def __init__(self, queues, connection=None, thread_count=4, **kwargs):
        super(ThreadedWorker, self).__init__(queues, connection=connection, **kwargs)
        self.thread_count = thread_count

    def work(self, burst=False):
        """Starts the worker in the specified mode."""
        def worker_job():
            super(ThreadedWorker, self).work(burst=burst)

        threads = [threading.Thread(target=worker_job) for _ in range(self.thread_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

if __name__ == '__main__':
    with Connection(conn):
        worker = ThreadedWorker(list(map(Queue, listen)), connection=conn, thread_count=4)
        worker.work()
