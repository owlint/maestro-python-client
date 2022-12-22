from redis import Redis

from maestro_python_client.Cache.Cache import Cache


class RedisCache(Cache):
    def __init__(self, redis: Redis) -> None:
        super().__init__()
        self.__redis = redis

    def get(self, key: str) -> str:
        payload = self.__redis.get(key)

        if not payload:
            raise ValueError(f"Key {key} is not cached")

        return payload.decode("utf-8")

    def put(self, key: str, value: str, ttl: int | None = None):
        if not self.__redis.set(key, value.encode("utf-8"), ex=ttl):
            raise ValueError(f"Could not add {key} to cache")

    def delete(self, key: str):
        self.__redis.delete(key)

    def set_ttl(self, key: str, ttl: int):
        self.__redis.expire(key, ttl)
