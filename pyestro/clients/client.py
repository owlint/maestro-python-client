from pyestro.abc.client import AbstractClient
from pyestro.dtos.tasks import Task, TaskHistory
from datetime import datetime
from typing import Any
from httpx import Client


class Client(AbstractClient):
    """A regular Maestro http client."""

    def __init__(self, base_url: str):
        self._client = Client(base_url=base_url)

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
        payload = self.__serialize_task(
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

        response = self._client.post("/api/task/create", json=payload)
        response_content = response.json()

        if response.status_code > 400 or "error" in response_content:
            raise ValueError(
                f"Could not communicate with maestro. Status code is {response.status_code}, "
                f"response is {response.content}"
            )
        return response_content["task_id"]

    def get_task_state(self, task_id: str) -> Task:
        response = self._client.post(
            "/api/task/get",
            json={"task_id": task_id},
        )

        if response.status_code == 404:
            raise FileNotFoundError("Could not find this task")
        elif response.status_code > 400 or "error" in response.json():
            raise ValueError(
                f"Could not communicate with maestro. Status code is {response.status_code}, "
                f"response is {response.content}"
            )

        return Task.from_dict(response.json()["task"])

    def get_next_task(self, queue: str) -> Task | None:
        response = self._client.post("/api/queue/next", json={"queue": queue})
        response_json = response.json()

        if response.status_code > 400 or "error" in response_json:
            raise ValueError(
                f"Could not communicate with maestro. Status code is {response.status_code}, "
                f"response is {response.content}"
            )

        return Task.model_validate(response_json["task"]) if response_json else None

    def consume_task(self, queue: str) -> Task | None:
        response = self._client.post("/api/queue/results/consume", json={"queue": queue})
        response_json = response.json()

        if response.status_code > 400 or "error" in response_json:
            raise ValueError(
                f"Could not communicate with maestro. Status code is {response.status_code}, "
                f"response is {response.content}"
            )

        return Task.model_validate(response_json["task"]) if response_json else None

    def delete_task(self, task_id: str, consume: bool = False) -> None:
        response = self._client.post("/api/task/delete", json={"task_id": task_id, "consume": consume})
        response_json = response.json()

        if response.status_code == 404:
            raise FileNotFoundError("Could not find this task")

        if response.status_code > 400 or "error" in response_json:
            raise ValueError(
                f"Could not communicate with maestro. Status code is {response.status_code}, "
                f"response is {response.content}"
            )

        return response_json

    def cancel_task(self, task_id: str) -> None:
        response = self._client.post("/api/task/cancel", json={"task_id": task_id})
        response_json = response.json()

        if response.status_code == 404:
            raise FileNotFoundError("Could not find this task")
        if response.status_code > 400 or "error" in response_json:
            raise ValueError(
                f"Could not communicate with maestro. Status code is {response.status_code}, "
                f"response is {response.content}"
            )

        return response_json

    def complete_task(self, task_id: str, result: str) -> None:
        response = self._client.post("/api/task/complete", json={"task_id": task_id, "result": result})
        response_json = response.json()

        if response.status_code == 404:
            raise FileNotFoundError("Could not find this task")

        if response.status_code > 400 or "error" in response_json:
            raise ValueError(
                f"Could not communicate with maestro. Status code is {response.status_code}, "
                f"response is {response.content}"
            )

        return response_json

    def fail_task(self, task_id: str) -> None:
        response = self._client.post("/api/task/fail", json={"task_id": task_id})
        response_json = response.json()

        if response.status_code == 404:
            raise FileNotFoundError("Could not find this task")

        if response.status_code > 400 or "error" in response_json:
            raise ValueError(
                f"Could not communicate with maestro. Status code is {response.status_code}, "
                f"response is {response.content}"
            )

        return response_json

    def get_owners_history(self, owner_ids: list[str]) -> list[TaskHistory]:
        """Retrieve a list of tasks with childs a list of owner IDs.
        Args:
            owner_ids: a list of owner ids

        Raises:
            ValueError: Error in communication with maestro
        """
        response = self._client.post("/api/owners/history/get", json={"owner_ids": owner_ids})
        response_json = response.json()

        if response.status_code > 400 or "error" in response_json:
            raise ValueError(
                f"Could not communicate with maestro. Status code is {response.status_code}, "
                f"response is {response.content}"
            )

        tasks = [TaskHistory.model_validate(task) for task in response.json()["tasks"]]

        return tasks

    def delete_owners_history(self, owner_ids: list[str]) -> dict[str, Any]:

        response = self._client.post("/api/owners/history/delete", json={"owner_ids": owner_ids})
        response_json = response.json()

        if response.status_code > 400 or "error" in response_json:
            raise ValueError(
                f"Could not communicate with maestro. Status code is {response.status_code}, "
                f"response is {response.content}"
            )

        return response_json

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
        payload = []
        for owner, task_name, task_payload in tasks:
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

        response = self._client.post("/api/task/create/list", json={"tasks": payload})
        response_json = response.json()

        if response.status_code > 400 or "error" in response_json:
            raise ValueError(
                f"Could not communicate with maestro. Status code is {response.status_code}, "
                f"response is {response.content}"
            )
        return response_json["task_ids"]

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
            task["not_before"] = datetime.now().timestamp() + executes_in

        if start_timeout > 0:
            task["startTimeout"] = datetime.now().timestamp() + executes_in

        return task
