import abc
import threading
import time
from logging import Logger

from pyestro.Client import Client, Task


class TaskWorker(abc.ABC):
    @abc.abstractmethod
    def on_task(self, task_payload: str) -> str:
        pass


class MaestroTaskHandler(threading.Thread):
    def __init__(
        self, client: Client, queue_name: str, worker: TaskWorker, logger: Logger
    ):
        assert isinstance(worker, TaskWorker)

        super().__init__()
        self.__client = client
        self.__worker = worker
        self.__queue = queue_name
        self.__logger = logger

    def run(self) -> None:
        while True:
            try:
                self.__run()
            except Exception as e:
                self.__logger.exception(
                    {
                        "infrastructure": "task_handler",
                        "msg": f"Could not manage task for queue {self.__queue}",
                        "err": str(e),
                    },
                )

    def __run(self) -> None:
        task = self.__try_get_task()
        if not task:
            time.sleep(1)
            return
        assert task is not None

        result, success = self.__execute_task(task)
        if success:
            self.__client.complete_task(task.task_id, result)
        else:
            self.__client.fail_task(task.task_id)

    def __execute_task(self, task: Task) -> tuple[str, bool]:
        try:
            return self.__worker.on_task(task.payload), True

        except Exception as e:
            self.__logger.exception(
                {
                    "infrastructure": "task_handler",
                    "queue": task.task_queue,
                    "task_id": task.task_id,
                    "msg": f"Cannot run task {task.task_id} from queue {task.task_queue}",
                    "err": str(e),
                },
            )
            return "", False

    def __try_get_task(self) -> Task | None:
        task = self.__client.next(self.__queue)
        if task is None or task.task_id == "":
            return None
        return task
