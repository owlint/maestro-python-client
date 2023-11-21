from abc import ABC, abstractmethod


class AbstractCache(ABC):
    @abstractmethod
    def get(self, key: str) -> str:
        """Retrieve an item from the cache."""

    @abstractmethod
    def put(self, key: str, value: str, ttl: int):
        """Put an item in the cache."""

    @abstractmethod
    def delete(self, key: str):
        """Delete an item from the cache."""

    @abstractmethod
    def set_ttl(self, key: str, ttl: int):
        """Sets the Time To Live on a particular item in the cache given its key."""
