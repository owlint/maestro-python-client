from typing import List, Tuple
from uuid import uuid4

from pyestro.abc.client import AbstractClient
from pyestro.dtos.tasks import Task
from pyestro.abc.cache import AbstractCache


class CachedClient(AbstractClient):
    """
    A Maestro client that caches data/responses.

    This implementation is more of a "Decorator" (the pattern), than a reimplementation of the
    interface.

    The decoratored behavior is the caching layer.
    """

    def __init__(
        self,
        client: AbstractClient,
        cache: AbstractCache,
        cached_queues: list[str] = None,
        completed_task_ttl: int = 900,
    ) -> None:
        self._client = client
        cached_queues = cached_queues or []
        self.__cached_queues: set[str] = set(cached_queues)
        self.__cache = cache
        self.__completed_task_ttl = completed_task_ttl

    def launch_task(
        self,
        owner: str,
        queue: str,
        task_payload: str,
        retries: int = 0,
        timeout: int = 900,
        executes_in: int = 0,
        start_timeout: int = 0,
        callback_url: str = "",
        parent_task_id: str = "",
    ) -> str:
        """Launches a task.

        Launches a task to be executed by Maestro.

        Args:
            owner: owner of the task. Used for fair scheduling.
            queue: name of the queue to use.
            task_payload: encoded payload for the task.
            retries: Number of allowed retries in case of fail or timeouts.
            timeout: Allowed time span for the task to execute.
            executes_in: Number of seconds to wait before executing the task
            start_timeout: Allowed time span in seconds for the task to start. Must be > 0 if the task is in a cached queue.
            callback_url: URL called after task execution is completed.
            parent_task_id: Task ID of the parent, if any.

        Returns:
            A string representing the Maestro task id

        Raises:
            ValueError: Problem in the communication with maestro or invalid start_time.
        """

        if queue in self.__cached_queues and start_timeout <= 0:
            raise ValueError("Start timeout must be > 0 for cached task")

        ttl = (
            executes_in
            + self.__completed_task_ttl
            + (start_timeout + timeout) * (retries + 1)
        )
        task_payload = self.__cache_payload(
            queue,
            task_payload,
            ttl,
        )
        return self._client.launch_task(
            owner,
            queue,
            task_payload,
            retries,
            timeout,
            executes_in,
            start_timeout,
            callback_url,
            parent_task_id,
        )

    def get_next_task(self, queue: str) -> Task | None:
        task = self._client.get_next_task(queue)
        return self.__task_from_cache(task)

    def consume_task(self, queue: str) -> Task | None:
        task = self._client.consume_task(queue)
        return self.__task_from_cache(task)

    def get_task_state(self, task_id: str) -> Task:
        task = self._client.get_task_state(task_id)
        return self.__task_from_cache(task)

    def complete_task(self, task_id: str, result: str) -> None:
        task = self._client.get_task_state(task_id)

        self.__set_ttl(task.task_queue, task.payload, self.__completed_task_ttl)
        result = self.__cache_payload(
            task.task_queue, result, self.__completed_task_ttl
        )

        self._client.complete_task(task_id, result)

    def cancel_task(self, task_id: str) -> None:
        task = self._client.get_task_state(task_id)
        self.__set_ttl(task.task_queue, task.payload, self.__completed_task_ttl)

        self._client.cancel_task(task_id)

    def fail_task(self, task_id: str) -> None:
        task = self._client.get_task_state(task_id)
        self.__set_ttl(task.task_queue, task.payload, self.__completed_task_ttl)
        self._client.fail_task(task_id)

    def delete_task(self, task_id: str, consume: bool = False) -> None:
        task = self._client.get_task_state(task_id)
        self.__delete(task.task_queue, task.payload)
        if task.result:
            self.__delete(task.task_queue, task.result)

        self._client.delete_task(task_id, consume=consume)

    def launch_task_list(
        self,
        tasks: List[Tuple[str, str, str]],
        retries: int = 0,
        timeout: int = 900,
        executes_in: int = 0,
        start_timeout: int = 0,
        callback_url: str = "",
        parent_task_id: str = "",
    ) -> List[str]:
        """Launches a list a task.

        Each task is represented a 3-uple that will be added to maestro.

        Args:
            tasks:
                The list of tasks to be added. Each task is a 3-uple containing
                1. The owner of the task (as str)
                2. The queue
                3. The payload
            retries: Number of allowed retries in case of fail or timeouts.
            timeout: Allowed time span for the task to execute.
            executes_in: Number of seconds to wait before executing the task
            start_timeout: Allowed time span in seconds for the task to start. Must be > 0 if the task is in a cached queue.
            callback_url: URL called after task execution is completed.
            parent_task_id: Task ID of the parent, if any.

        Returns:
            A list of string representing the identifiers of the tasks


        Raises:
            ValueError: Error in communication with maestro or invalid start_time
        """

        payload_ttl = (
            executes_in
            + self.__completed_task_ttl
            + (start_timeout + timeout) * (retries + 1)
        )

        for i, (owner, queue, payload) in enumerate(tasks):
            if queue in self.__cached_queues and start_timeout <= 0:
                raise ValueError("Start timeout must be > 0 for cached task")

            tasks[i] = (
                owner,
                queue,
                self.__cache_payload(
                    queue,
                    payload,
                    payload_ttl,
                ),
            )

        return self._client.launch_task_list(
            tasks,
            retries,
            timeout,
            executes_in,
            start_timeout,
            callback_url,
            parent_task_id,
        )

    def __task_from_cache(self, task: Task | None) -> Task | None:
        if not task:
            return task

        task.payload = self.__payload_from_cache(task.task_queue, task.payload)

        if task.result:
            task.result = self.__payload_from_cache(task.task_queue, task.result)

        return task

    def __cache_payload(self, queue: str, payload: str, timeout: int = 0) -> str:
        if queue not in self.__cached_queues:
            return payload

        cache_key = self.__unique_key(queue)
        self.__cache.put(cache_key, payload, timeout)
        return cache_key

    def __payload_from_cache(self, queue: str, payload: str) -> str:
        if queue not in self.__cached_queues:
            return payload

        return self.__cache.get(payload)

    def __set_ttl(self, queue: str, key: str, ttl: int):
        if queue not in self.__cached_queues:
            return

        self.__cache.set_ttl(key, ttl)

    def __delete(self, queue: str, key: str):
        if queue not in self.__cached_queues:
            return

        self.__cache.delete(key)

    @staticmethod
    def __unique_key(queue_name: str) -> str:
        return f"maestro-cache-{queue_name}-{str(uuid4())}"
