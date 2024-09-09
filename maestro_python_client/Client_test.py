from maestro_python_client.Client import QueueStats, TaskHistory


def test_task_history_from_dict():
    task = TaskHistory.from_dict(
        {
            "task_id": "task_id",
            "parent_task_id": "parent_task_id",
            "owner_id": "owner_id",
            "state": "state",
            "version": 1,
            "task_queue": "task_queue",
            "timeout": 2,
            "retries": 3,
            "max_retries": 4,
            "not_before": 5,
            "start_timeout": 6,
            "consumed": True,
        }
    )

    assert task.task_id == "task_id"
    assert task.parent_task_id == "parent_task_id"
    assert task.owner == "owner_id"
    assert task.state == "state"
    assert task.version == 1
    assert task.task_queue == "task_queue"
    assert task.timeout == 2
    assert task.retries == 3
    assert task.max_retries == 4
    assert task.not_before == 5
    assert task.start_timeout == 6
    assert task.consumed is True


def test_queue_stats_from_dict():
    queue_stats = QueueStats.from_dict(
        {
            "canceled": ["Task-0"],
            "completed": ["Task-6"],
            "failed": [],
            "pending": ["Task-1", "Task-4", "Task-3", "Task-5"],
            "planned": [],
            "running": ["Task-2"],
            "timedout": ["Task-7"],
        }
    )
    assert queue_stats.canceled == ["Task-0"]
    assert queue_stats.completed == ["Task-6"]
    assert queue_stats.failed == []
    assert queue_stats.planned == []
    assert queue_stats.pending == ["Task-1", "Task-4", "Task-3", "Task-5"]
    assert queue_stats.running == ["Task-2"]
    assert queue_stats.timedout == ["Task-7"]
