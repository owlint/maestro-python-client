from redis import Redis

from maestro_python_client.Cache.Cache import Cache


class RedisCache(Cache):
    def __init__(self, redis: Redis, ttl: int = 300) -> None:
        super().__init__()
        self.__redis = redis
        self.__ttl = ttl

    def get(self, key: str) -> str:
        payload = self.__redis.get(key)

        if not payload:
            raise ValueError(f"Key {key} is not cached")

        return payload.decode("utf-8")

    def put(self, key: str, value: str, extra_ttl: int = 0):
        if not self.__redis.set(key, value.encode("utf-8"), ex=self.__ttl + extra_ttl):
            raise ValueError(f"Could not add {key} to cache")

    def delete(self, key: str):
        self.__redis.delete(key)

    def set_ttl(self, key: str, ttl: int):
        self.__redis.expire(key, ttl)
