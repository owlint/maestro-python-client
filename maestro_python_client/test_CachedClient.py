from collections import defaultdict
from copy import deepcopy
from unittest.mock import MagicMock, patch

from pytest import fixture, raises

from maestro_python_client.Cache.Cache import Cache
from maestro_python_client.CachedClient import CachedClient
from test_utils.String import unique_str


@fixture
def json_task():
    return {
        "task_id": unique_str(),
        "owner": unique_str(),
        "task_queue": unique_str(),
        "payload": "",
        "result": "",
        "state": "pending",
        "timeout": 300,
        "retries": 0,
        "max_retries": 0,
        "created_at": 1,
        "updated_at": 1,
        "not_before": 0,
    }


class TestCache(Cache):
    def __init__(self) -> None:
        super().__init__()
        self.__cache = {}

    @property
    def cache(self) -> dict[str, str]:
        return deepcopy(self.__cache)

    def put(self, key: str, value: str, ttl: int | None = None):
        self.__cache[key] = value

    def get(self, key: str) -> str:
        return self.__cache[key]

    def set_ttl(self, key: str, ttl: int):
        assert key in self.__cache

    def delete(self, key: str):
        self.__cache.pop(key)

    def key_for_value(self, value: str) -> str | None:
        for key, v in self.__cache.items():
            if v == value:
                return key

        return None


@patch("requests.post")
def test_launch_task(requests, subtests):
    response_mock = MagicMock()
    requests.return_value = response_mock
    response_mock.status_code = 200

    with subtests.test("Queue not cached"):
        mock = MagicMock()
        client = CachedClient("", mock, ["cached"])
        client.launch_task(unique_str(), "not_cached", "payload", start_timeout=100)
        assert not mock.put.called

    with subtests.test("Queue cached"):
        mock = MagicMock()
        client = CachedClient("", mock, ["cached"])
        client.launch_task(unique_str(), "cached", "payload", start_timeout=100)
        assert mock.put.called

    with subtests.test("Start timeout 0 cached should fail"):
        mock = MagicMock()
        client = CachedClient("", mock, ["cached"])
        with raises(ValueError):
            client.launch_task(unique_str(), "cached", "payload", start_timeout=0)

    with subtests.test("Start timeout 0 not cached should not fail"):
        mock = MagicMock()
        client = CachedClient("", mock, ["cached"])
        client.launch_task(unique_str(), "not_cached", "payload", start_timeout=0)


@patch("requests.post")
def test_launch_task_list(requests, subtests):
    response_mock = MagicMock()
    requests.return_value = response_mock
    response_mock.status_code = 200

    with subtests.test("Queue not cached"):
        mock = MagicMock()
        client = CachedClient("", mock, ["cached"])
        client.launch_task_list(
            [(unique_str(), "not_cached", "payload")], start_timeout=100
        )
        assert not mock.put.called

    with subtests.test("Queue cached"):
        mock = MagicMock()
        client = CachedClient("", mock, ["cached"])
        client.launch_task_list(
            [(unique_str(), "cached", "payload")], start_timeout=100
        )
        assert mock.put.called

    with subtests.test("Start timeout 0 cached should fail"):
        mock = MagicMock()
        client = CachedClient("", mock, ["cached"])
        with raises(ValueError):
            client.launch_task_list(
                [(unique_str(), "cached", "payload")], start_timeout=0
            )

    with subtests.test("Start timeout 0 not cached should not fail"):
        mock = MagicMock()
        client = CachedClient("", mock, ["cached"])
        client.launch_task_list(
            [(unique_str(), "not_cached", "payload")], start_timeout=0
        )


@patch("requests.post")
def test_task_getters(requests, subtests, json_task):
    response_mock = MagicMock()
    requests.return_value = response_mock
    response_mock.status_code = 200

    methods = ["task_state", "next", "consume"]
    for method in methods:
        with subtests.test("With cache"):
            cache = TestCache()
            client = CachedClient("", cache, ["cached"])

            response_mock.json.return_value = defaultdict(lambda: "")
            client.launch_task(unique_str(), "cached", "payload", start_timeout=100)

            response_mock.json.return_value = {
                "task": {
                    **json_task,
                    "task_queue": "cached",
                    "payload": cache.key_for_value("payload"),
                }
            }
            client.complete_task("task", "result")
            assert len(cache.cache) == 2

            response_mock.json.return_value = {
                "task": {
                    **json_task,
                    "task_queue": "cached",
                    "result": cache.key_for_value("result"),
                    "payload": cache.key_for_value("payload"),
                }
            }
            test_method = getattr(client, method)
            res = test_method("cached")
            assert res
            assert res.payload == "payload"
            assert res.result == "result"

        with subtests.test("Without cache"):
            cache = TestCache()
            client = CachedClient("", cache, ["cached"])

            response_mock.json.return_value = defaultdict(lambda: "")
            client.launch_task(unique_str(), "not_cached", "payload", start_timeout=100)

            response_mock.json.return_value = {
                "task": {
                    **json_task,
                    "task_queue": "not_cached",
                    "payload": "payload",
                }
            }
            client.complete_task("task", "result")
            assert not cache.cache

            response_mock.json.return_value = {
                "task": {
                    **json_task,
                    "task_queue": "not_cached",
                    "result": "result",
                    "payload": "payload",
                }
            }

            test_method = getattr(client, method)
            res = test_method("not_cached")
            assert res
            assert res.payload == "payload"
            assert res.result == "result"


@patch("requests.post")
def test_complete_task(requests, subtests, json_task):
    response_mock = MagicMock()
    requests.return_value = response_mock
    response_mock.status_code = 200

    with subtests.test("With cache"):
        cache = TestCache()
        client = CachedClient("", cache, ["cached"])

        response_mock.json.return_value = defaultdict(lambda: "")
        client.launch_task(unique_str(), "cached", "payload", start_timeout=100)

        response_mock.json.return_value = {
            "task": {
                **json_task,
                "task_queue": "cached",
                "payload": cache.key_for_value("payload"),
            }
        }
        client.complete_task("task", "result")
        assert cache.key_for_value("result")

    with subtests.test("Without cache"):
        cache = TestCache()
        client = CachedClient("", cache, ["cached"])

        response_mock.json.return_value = defaultdict(lambda: "")
        client.launch_task(unique_str(), "not_cached", "payload", start_timeout=100)

        response_mock.json.return_value = {
            "task": {
                **json_task,
                "task_queue": "not_cached",
                "payload": cache.key_for_value("payload"),
            }
        }
        client.complete_task("task", "result")
        assert not cache.key_for_value("result")


@patch("requests.post")
def test_delete_task(requests, subtests, json_task):
    response_mock = MagicMock()
    requests.return_value = response_mock
    response_mock.status_code = 200

    with subtests.test("With cache and result"):
        cache = TestCache()
        client = CachedClient("", cache, ["cached"])

        response_mock.json.return_value = defaultdict(lambda: "")
        client.launch_task(unique_str(), "cached", "payload", start_timeout=100)
        response_mock.json.return_value = {
            "task": {
                **json_task,
                "task_queue": "cached",
                "payload": cache.key_for_value("payload"),
            }
        }
        client.complete_task("task", "result")
        assert cache.key_for_value("result")
        assert cache.key_for_value("payload")

        response_mock.json.return_value = {
            "task": {
                **json_task,
                "task_queue": "cached",
                "payload": cache.key_for_value("payload"),
                "result": cache.key_for_value("result"),
            }
        }
        client.delete_task("task")
        assert not cache.key_for_value("result")
        assert not cache.key_for_value("payload")

    with subtests.test("With cache no result"):
        cache = TestCache()
        client = CachedClient("", cache, ["cached"])

        response_mock.json.return_value = defaultdict(lambda: "")
        client.launch_task(unique_str(), "cached", "payload", start_timeout=100)

        response_mock.json.return_value = {
            "task": {
                **json_task,
                "task_queue": "cached",
                "payload": cache.key_for_value("payload"),
            }
        }
        assert not cache.key_for_value("result")
        assert cache.key_for_value("payload")

        client.delete_task("task")
        assert not cache.key_for_value("result")
        assert not cache.key_for_value("payload")

    with subtests.test("Without cache"):
        cache = TestCache()
        client = CachedClient("", cache, ["cached"])

        response_mock.json.return_value = defaultdict(lambda: "")
        client.launch_task(unique_str(), "not_cached", "payload", start_timeout=100)

        response_mock.json.return_value = {
            "task": {
                **json_task,
                "task_queue": "not_cached",
                "payload": cache.key_for_value("payload"),
            }
        }
        client.complete_task("task", "result")
        assert not cache.key_for_value("result")
        assert not cache.key_for_value("payload")

        client.delete_task("task")
        assert not cache.key_for_value("result")
        assert not cache.key_for_value("payload")


@patch("requests.post")
def test_task_stoppers(requests, subtests, json_task):
    response_mock = MagicMock()
    requests.return_value = response_mock
    response_mock.status_code = 200

    methods = ["fail_task", "cancel_task"]
    for method in methods:
        with subtests.test("With cache"):
            cache = MagicMock()
            client = CachedClient("", cache, ["cached"])

            response_mock.json.return_value = defaultdict(lambda: "")
            client.launch_task(unique_str(), "cached", "payload", start_timeout=100)
            response_mock.json.return_value = {
                "task": {
                    **json_task,
                    "task_queue": "cached",
                    "result": cache.key_for_value("result"),
                    "payload": cache.key_for_value("payload"),
                }
            }
            test_method = getattr(client, method)
            test_method("cached")
            assert cache.set_ttl.called

        with subtests.test("Without cache"):
            cache = MagicMock()
            client = CachedClient("", cache, ["cached"])

            response_mock.json.return_value = defaultdict(lambda: "")
            client.launch_task(unique_str(), "not_cached", "payload", start_timeout=100)

            response_mock.json.return_value = {
                "task": {
                    **json_task,
                    "task_queue": "not_cached",
                    "result": "result",
                    "payload": "payload",
                }
            }

            test_method = getattr(client, method)
            test_method("not_cached")
            assert not cache.set_ttl.called
