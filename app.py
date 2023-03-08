import time

import redis
from fastapi import FastAPI

app = FastAPI()
cache = redis.Redis(host='redis', port=6379)


def get_hit_count():
    """

    :return:
    """
    retries = 5
    while True:
        try:
            return cache.incr('hits')
        except redis.exceptions.ConnectionError as exc:
            if retries == 0:
                raise exc
            retries -= 1
            time.sleep(0.5)

@app.get('/')
def hello():
    count = get_hit_count()
    return f'Hello from Docker! I have been seen {count} times.'