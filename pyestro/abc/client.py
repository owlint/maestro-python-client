from abc import ABC, abstractmethod
from pyestro.dtos.tasks import Task, TaskHistory
from typing import Any


class AbstractClient(ABC):

    @abstractmethod
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
        """
        Launch a task.

        Launches a task to be executed by Maestro.

        Args:
            owner: owner of the task. Used for fair scheduling.
            queue: name of the queue to use.
            task_payload: encoded payload for the task.
            retries: Number of allowed retries in case of fail or timeouts.
            timeout: Allowed time span for the task to execute.
            executes_in: Number of seconds to wait before executing the task
            start_timeout: Allowed time span in seconds for the task to start.
            callback_url: URL called after task execution is completed.
            parent_task_id: Task ID of the parent, if any.

        Returns:
            A string representing the Maestro task id

        Raises:
            ValueError: Problem in the communication with maestro.
        """
        raise NotImplementedError

    @abstractmethod
    def get_task_state(self, task_id: str) -> Task:
        """Return the state of a task given its id.

        Args:
            task_id: the maestro id for this task

        Returns:
            A Task Object

        Raises:
            FileNotFoundError: This task does not exist
            ValueError: Error in communication with maestro

        """
        raise NotImplementedError

    @abstractmethod
    def get_next_task(self, queue: str) -> Task | None:
        """Get the following pending task.

        Get the following task to execute.

        Args:
            queue: the queue name

        Returns:
            A Task Object or None is no task is available

        Raises:
            ValueError: Error in communication with maestro
        """
        raise NotImplementedError

    @abstractmethod
    def consume_task(self, queue: str) -> Task | None:
        """Consumes a task result from the queue.

        Consuming is getting the next result and directly removing it from
        maestro.

        Args:
            queue: the queue name

        Returns:
            A Task Object or None is no result is available

        Raises:
            ValueError: Error in communication with maestro

        """
        raise NotImplementedError

    @abstractmethod
    def delete_task(self, task_id: str, consume: bool = False) -> None:
        """Delete a task from maestro.

        Given its id the task is removed from maestro.

        Args:
            task_id: The identifier of the task
            consume: A boolean whether the task should be consumed before

        Raises:
            FileNotFoundError: This task does not exist
            ValueError: Error in communication with maestro
        """
        raise NotImplementedError

    @abstractmethod
    def cancel_task(self, task_id: str) -> None:
        """Cancel a task from maestro.

        Given its id the task is cancelled from maestro.

        Args:
            task_id: the identifier of the task

        Raises:
            FileNotFoundError: This task does not exist
            ValueError: Error in communication with maestro
        """
        raise NotImplementedError

    @abstractmethod
    def complete_task(self, task_id: str, result: str) -> None:
        """Complete a task in maestro.

        Given its id and its result the task is marked completed from maestro.

        Args:
            task_id: the identifier of the task
            result: the result for the task

        Raises:
            FileNotFoundError: This task does not exist
            ValueError: Error in communication with maestro
        """
        raise NotImplementedError

    @abstractmethod
    def fail_task(self, task_id: str) -> None:
        """Fail a task in maestro.

        Given its id the task is marked failed in maestro.

        Args:
            task_id: the identifier of the task

        Raises:
            FileNotFoundError: This task does not exist
            ValueError: Error in communication with maestro
        """
        raise NotImplementedError

    @abstractmethod
    def get_owners_history(self, owner_ids: list[str]) -> list[TaskHistory]:
        """Retrieve a list of tasks with child's list of owner IDs.
        Args:
            owner_ids: a list of owner ids

        Raises:
            ValueError: Error in communication with maestro
        """
        raise NotImplementedError

    @abstractmethod
    def delete_owners_history(self, owner_ids: list[str]) -> dict[str, Any]:
        """Delete history for all tasks associated to a list of owners.
        Args:
            owner_ids: a list of owner ids

        Raises:
            ValueError: Error in communication with maestro
        """
        raise NotImplementedError

    @abstractmethod
    def launch_task_list(
            self,
            tasks: list[tuple[str, str, str]],
            retries: int = 0,
            timeout: int = 900,
            executes_in: int = 0,
            start_timeout: int = 0,
            callback_url: str = "",
            parent_task_id: str = "",
    ) -> list[str]:
        """Launches a list of tasks.

        Each task is represented as a tuple of elements as such: (owner, queue, payload)

        Args:
            tasks: The list of tasks to be added.
            retries: Number of allowed retries in case of fail or timeouts.
            timeout: Allowed time span for the task to execute.
            executes_in: Number of seconds to wait before executing the task
            start_timeout: Allowed time span in seconds for the task to start.
            callback_url: URL called after task execution is completed.
            parent_task_id: Task ID of the parent, if any.

        Returns:
            A list of string representing the identifiers of the tasks


        Raises:
            ValueError: Error in communication with maestro
        """
        raise NotImplementedError
