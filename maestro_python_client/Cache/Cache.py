from abc import ABC, abstractmethod


class Cache(ABC):
    @abstractmethod
    def get(self, key: str) -> str:
        ...

    @abstractmethod
    def put(self, key: str, value: str, ttl: int):
        ...
