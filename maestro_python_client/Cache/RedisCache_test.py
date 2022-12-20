from pytest import raises

from maestro_python_client.Cache.RedisCache import RedisCache
from test_utils.Redis import new_test_redis
from test_utils.String import unique_str


def test_get(subtests):
    cache = RedisCache(new_test_redis())

    with subtests.test("key exists"):
        key = unique_str()
        cache.put(key, "value")

        assert cache.get(key) == "value"

    with subtests.test("get twice"):
        key = unique_str()
        cache.put(key, "value")

        assert cache.get(key) == "value"

    with subtests.test("value doesn't exists"):
        with raises(ValueError):
            cache.get(unique_str())


def test_put(subtests):
    cache = RedisCache(new_test_redis())

    with subtests.test("put new"):
        key = unique_str()
        cache.put(key, "value")

        assert cache.get(key) == "value"

    with subtests.test("put existing"):
        key = unique_str()
        cache.put(key, "value")
        cache.put(key, "new")

        assert cache.get(key) == "new"
