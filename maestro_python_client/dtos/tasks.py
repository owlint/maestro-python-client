from pydantic import BaseModel, ConfigDict
from datetime import datetime


class Task(BaseModel):
    """A DTO representing a Task in Maestro."""

    task_id: str
    owner: str
    task_queue: str
    state: str
    timeout: int
    retries: int
    max_retries: int
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()
    result: str | None = None
    not_before: int = 0
    consumed: bool
    parent_task_id: str


class TaskHistory(BaseModel):
    """A DTO that represents the history of a particular task."""

    model_config = ConfigDict(frozen=True)

    task_id: str
    parent_task_id: str
    owner: str
    task_queue: str
    state: str
    timeout: int
    retries: int
    start_timeout: int
    max_retries: int
    not_before: int
    version: int
    consumed: bool = False
