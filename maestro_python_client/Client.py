import datetime
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Union
from urllib.parse import urljoin

import requests
from typing_extensions import deprecated


class Task:
    def __init__(self):
        self.task_id: str = ""
        self.owner: str = ""
        self.task_queue: str = ""
        self.payload: str = ""
        self.state: str = ""
        self.timeout: int = 0
        self.retries: int = 0
        self.max_retries: int = 0
        self.created_at: datetime.datetime = datetime.datetime.now()
        self.updated_at: datetime.datetime = datetime.datetime.now()
        self.result: Union[None, str] = None
        self.not_before: int = 0
        self.consumed = False
        self.parent_task_id = ""

    @classmethod
    @deprecated("Task.from_dict() should be used instead")
    def from_json(cls, payload: Dict[str, Any]) -> "Task":
        return cls.from_dict(payload)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "Task":
        task = cls()

        task.task_id = payload["task_id"]
        task.owner = payload["owner"]
        task.task_queue = payload["task_queue"]
        task.payload = payload["payload"]
        task.state = payload["state"]
        task.timeout = payload["timeout"]
        task.retries = payload["retries"]
        task.max_retries = payload["max_retries"]
        task.created_at = datetime.datetime.fromtimestamp(payload["created_at"])
        task.updated_at = datetime.datetime.fromtimestamp(payload["updated_at"])
        task.consumed = payload.get("consumed", False)
        task.parent_task_id = payload.get("parent_task_id", "")

        if "result" in payload:
            task.result = payload["result"]

        task.not_before = payload["not_before"]

        return task


@dataclass(frozen=True)
class TaskHistory:
    task_id: str = ""
    parent_task_id: str = ""
    owner: str = ""
    task_queue: str = ""
    state: str = ""
    timeout: int = 0
    retries: int = 0
    start_timeout: int = 0
    max_retries: int = 0
    not_before: int = 0
    version: int = 0
    consumed: bool = False

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TaskHistory":
        return cls(
            task_id=payload["task_id"],
            parent_task_id=payload["parent_task_id"],
            owner=payload["owner_id"],
            task_queue=payload["task_queue"],
            state=payload["state"],
            timeout=payload["timeout"],
            retries=payload["retries"],
            max_retries=payload["max_retries"],
            version=payload["version"],
            not_before=payload["not_before"],
            start_timeout=payload["start_timeout"],
            consumed=payload["consumed"],
        )


class Client:
    def __init__(self, maestro_endpoint: str):
        self.__maestro_endpoint = maestro_endpoint

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
            start_timeout: Allowed time span in seconds for the task to start.
            callback_url: URL called after task execution is completed.
            parent_task_id: Task ID of the parent, if any.

        Returns:
            A string representing the Maestro task id

        Raises:
            ValueError: Problem in the communication with maestro.
        """

        resp = requests.post(
            urljoin(self.__maestro_endpoint, "/api/task/create"),
            json=self.__serialize_task(
                owner,
                queue,
                task_payload,
                retries,
                timeout,
                executes_in,
                start_timeout,
                callback_url,
                parent_task_id,
            ),
        )
        if resp.status_code > 400 or "error" in resp.json():
            raise ValueError(
                f"Could not communicate with maestro. Status code is {resp.status_code}, "
                f"response is {resp.content}"
            )
        return resp.json()["task_id"]

    def task_state(self, task_id: str) -> Task:
        """Return the state of a task given its id.

        Args:
            task_id: the maestro id for this task

        Returns:
            A Task Object

        Raises:
            FileNotFoundError: This task does not exists
            ValueError: Error in communication with maestro

        """
        resp = requests.post(
            urljoin(self.__maestro_endpoint, "/api/task/get"),
            json={"task_id": task_id},
        )
        if resp.status_code == 404:
            raise FileNotFoundError("Could not find this task")
        elif resp.status_code > 400 or "error" in resp.json():
            raise ValueError(
                f"Could not communicate with maestro. Status code is {resp.status_code}, "
                f"response is {resp.content}"
            )

        return Task.from_dict(resp.json()["task"])

    def next(self, queue: str) -> Union[Task, None]:
        """Get the following pending task.

        Get the following task to execute.

        Args:
            queue: the queue name

        Returns:
            A Task Object or None is no task is available

        Raises:
            ValueError: Error in communication with maestro
        """
        resp = requests.post(
            urljoin(self.__maestro_endpoint, "/api/queue/next"),
            json={"queue": queue},
        )

        if resp.status_code > 400 or "error" in resp.json():
            raise ValueError(
                f"Could not communicate with maestro. Status code is {resp.status_code}, "
                f"response is {resp.content}"
            )

        return Task.from_dict(resp.json()["task"]) if resp.json() != {} else None

    def consume(self, queue: str) -> Union[Task, None]:
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
        resp = requests.post(
            urljoin(self.__maestro_endpoint, "/api/queue/results/consume"),
            json={"queue": queue},
        )

        if resp.status_code > 400 or "error" in resp.json():
            raise ValueError(
                f"Could not communicate with maestro. Status code is {resp.status_code}, "
                f"response is {resp.content}"
            )

        return Task.from_dict(resp.json()["task"]) if resp.json() != {} else None

    def delete_task(self, task_id: str, consume: bool = False) -> None:
        """Delete a task from maestro.

        Given its id the task is removed from maestro.

        Args:
            task_id: the identifier of the task

        Raises:
            FileNotFoundError: This task does not exists
            ValueError: Error in communication with maestro
        """
        resp = requests.post(
            urljoin(self.__maestro_endpoint, "/api/task/delete"),
            json={
                "task_id": task_id,
                "consume": consume,
            },
        )
        if resp.status_code == 404:
            raise FileNotFoundError("Could not find this task")
        elif resp.status_code > 400 or "error" in resp.json():
            raise ValueError(
                f"Could not communicate with maestro. Status code is {resp.status_code}, "
                f"response is {resp.content}"
            )

        return resp.json()

    def cancel_task(self, task_id: str) -> None:
        """Cancel a task from maestro.

        Given its id the task is cancelled from maestro.

        Args:
            task_id: the identifier of the task

        Raises:
            FileNotFoundError: This task does not exists
            ValueError: Error in communication with maestro
        """
        resp = requests.post(
            urljoin(self.__maestro_endpoint, "/api/task/cancel"),
            json={"task_id": task_id},
        )
        if resp.status_code == 404:
            raise FileNotFoundError("Could not find this task")
        elif resp.status_code > 400 or "error" in resp.json():
            raise ValueError(
                f"Could not communicate with maestro. Status code is {resp.status_code}, "
                f"response is {resp.content}"
            )

        return resp.json()

    def complete_task(self, task_id: str, result: str) -> None:
        """Complete a task in maestro.

        Given its id and its result the task is marked completed from maestro.

        Args:
            task_id: the identifier of the task
            result: the result for the task

        Raises:
            FileNotFoundError: This task does not exists
            ValueError: Error in communication with maestro
        """
        resp = requests.post(
            urljoin(self.__maestro_endpoint, "/api/task/complete"),
            json={"task_id": task_id, "result": result},
        )
        if resp.status_code == 404:
            raise FileNotFoundError("Could not find this task")
        elif resp.status_code > 400 or "error" in resp.json():
            raise ValueError(
                f"Could not communicate with maestro. Status code is {resp.status_code}, "
                f"response is {resp.content}"
            )

        return resp.json()

    def fail_task(self, task_id: str) -> None:
        """Fail a task in maestro.

        Given its id the task is marked failed in maestro.

        Args:
            task_id: the identifier of the task

        Raises:
            FileNotFoundError: This task does not exists
            ValueError: Error in communication with maestro
        """
        resp = requests.post(
            urljoin(self.__maestro_endpoint, "/api/task/fail"),
            json={"task_id": task_id},
        )
        if resp.status_code == 404:
            raise FileNotFoundError("Could not find this task")
        elif resp.status_code > 400 or "error" in resp.json():
            raise ValueError(
                f"Could not communicate with maestro. Status code is {resp.status_code}, "
                f"response is {resp.content}"
            )

        return resp.json()

    def consume_task(self, task_id: str) -> Dict[str, Any]:
        """Consume a task from maestro.

        Given its id the task is marked consumed in maestro.

        Args:
            task_id: the identifier of the task

        Raises:
            FileNotFoundError: This task does not exists
            ValueError: Error in communication with maestro
        """
        resp = requests.post(
            urljoin(self.__maestro_endpoint, "/api/task/consume"),
            json={"task_id": task_id},
        )
        if resp.status_code == 404:
            raise FileNotFoundError("Could not find this task")
        elif resp.status_code > 400 or "error" in resp.json():
            raise ValueError(
                f"Could not communicate with maestro. Status code is {resp.status_code}, "
                f"response is {resp.content}"
            )

        return resp.json()

    def get_owners_history(self, owner_ids: List[str]) -> list[TaskHistory]:
        """Retrieve a list of tasks with childs a list of owner IDs.
        Args:
            owner_ids: a list of owner ids

        Raises:
            ValueError: Error in communication with maestro
        """
        resp = requests.post(
            urljoin(self.__maestro_endpoint, "/api/owners/history/get"),
            json={"owner_ids": owner_ids},
        )

        if resp.status_code > 400 or "error" in resp.json():
            raise ValueError(
                f"Could not communicate with maestro. Status code is {resp.status_code}, "
                f"response is {resp.content}"
            )

        tasks = [TaskHistory.from_dict(task) for task in resp.json()["tasks"]]

        return tasks

    def delete_owners_history(self, owner_ids: List[str]) -> Dict[str, Any]:
        """Delete history for all tasks associated to a list of owners.
        Args:
            owner_ids: a list of owner ids

        Raises:
            ValueError: Error in communication with maestro
        """
        resp = requests.post(
            urljoin(self.__maestro_endpoint, "/api/owners/history/delete"),
            json={"owner_ids": owner_ids},
        )

        if resp.status_code > 400 or "error" in resp.json():
            raise ValueError(
                f"Could not communicate with maestro. Status code is {resp.status_code}, "
                f"response is {resp.content}"
            )

        return resp.json()

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
            start_timeout: Allowed time span in seconds for the task to start.
            callback_url: URL called after task execution is completed.
            parent_task_id: Task ID of the parent, if any.

        Returns:
            A list of string representing the identifiers of the tasks


        Raises:
            ValueError: Error in communication with maestro
        """
        payload = []
        for (owner, task_name, task_payload) in tasks:
            payload.append(
                self.__serialize_task(
                    owner,
                    task_name,
                    task_payload,
                    retries,
                    timeout,
                    executes_in,
                    start_timeout,
                    callback_url,
                    parent_task_id,
                )
            )

        resp = requests.post(
            urljoin(self.__maestro_endpoint, "/api/task/create/list"),
            json={"tasks": payload},
        )
        if resp.status_code > 400 or "error" in resp.json():
            raise ValueError(
                f"Could not communicate with maestro. Status code is {resp.status_code}, "
                f"response is {resp.content}"
            )
        return resp.json()["task_ids"]

    @staticmethod
    def __serialize_task(
        owner: str,
        queue: str,
        task_payload: str,
        retries: int,
        timeout: int,
        executes_in: int,
        start_timeout: int,
        callback_url: str,
        parent_task_id: str,
    ) -> dict[str, Any]:
        task = {
            "owner": owner,
            "queue": queue,
            "retries": retries,
            "timeout": timeout,
            "payload": task_payload,
            "callback_url": callback_url,
            "parent_task_id": parent_task_id,
        }

        if executes_in > 0:
            task["not_before"] = datetime.datetime.now().timestamp() + executes_in

        if start_timeout > 0:
            task["startTimeout"] = datetime.datetime.now().timestamp() + executes_in

        return task
