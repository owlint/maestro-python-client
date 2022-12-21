from abc import ABC, abstractmethod


class Cache(ABC):
    @abstractmethod
    def get(self, key: str) -> str:
        ...

    @abstractmethod
    def put(self, key: str, value: str, ttl: int):
        ...

    @abstractmethod
    def delete(self, key: str):
        ...

    @abstractmethod
    def set_ttl(self, key: str, ttl: int):
        ...
