from typing import List, Tuple, Dict, Any, Union
import datetime
from urllib.parse import urljoin

import requests


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

    @classmethod
    def from_json(cls, payload: Dict[str, Any]) -> "Task":
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

        if "result" in payload:
            task.result = payload["result"]

        task.not_before = payload["not_before"]

        return task


class Client:
    def __init__(self, maestro_endpoint):
        self.__maestro_endpoint = maestro_endpoint

    def launch_task(
        self,
        owner: str,
        queue: str,
        task_payload: str,
        retries: int = 0,
        timeout: int = 900,
        executes_in: int = 0,
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

        Returns:
            A string representing the Maestro task id

        Raises:
            ValueError: Problem in the communication with maestro.
        """
        payload = {
            "owner": owner,
            "queue": queue,
            "retries": retries,
            "timeout": timeout,
            "payload": task_payload,
        }
        if executes_in > 0:
            payload["not_before"] = (
                datetime.datetime.now().timestamp() + executes_in
            )

        resp = requests.post(
            urljoin(self.__maestro_endpoint, "/api/task/create"),
            json=payload,
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

        return Task.from_json(resp.json())

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

        return Task.from_json(resp.json()) if resp.json() != {} else None

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

        return Task.from_json(resp.json()) if resp.json() != {} else None

    def delete_task(self, task_id: str) -> None:
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

    def launch_task_list(
        self,
        tasks: List[Tuple[str, str, str]],
        retries: int = 0,
        timeout: int = 900,
    ) -> List[str]:
        """Launches a list a task.

        Each task is represented a 3-uple that will be added to maestro.

        Args:
            tasks:
                The list of tasks to be added. Each task is a 3-uple containing
                1. The owner of the task (as str)
                2. The queue
                3. The payload

        Returns:
            A list of string representing the identifiers of the tasks


        Raises:
            ValueError: Error in communication with maestro
        """
        payload = []
        for (owner, task_name, task_payload) in tasks:
            payload.append(
                {
                    "owner": owner,
                    "queue": task_name,
                    "retries": retries,
                    "timeout": timeout,
                    "payload": task_payload,
                }
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
