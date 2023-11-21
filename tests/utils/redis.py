import os

from redis import Redis

__redis = None


def new_test_redis():
    global __redis

    if not __redis:
        __redis = Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=int(os.getenv("REDIS_DB", "0")),
            password=os.getenv("REDIS_PASSWORD", None),
        )

    return __redis
