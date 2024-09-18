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
            "canceled": 1,
            "completed": 1,
            "failed": 0,
            "pending": 4,
            "planned": 0,
            "running": 1,
            "timedout": 1,
        }
    )
    assert queue_stats.canceled == 1
    assert queue_stats.completed == 1
    assert queue_stats.failed == 0
    assert queue_stats.planned == 0
    assert queue_stats.pending == 4
    assert queue_stats.running == 1
    assert queue_stats.timedout == 1
