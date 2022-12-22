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


def test_delete(subtests):
    cache = RedisCache(new_test_redis())

    with subtests.test("put new"):
        key = unique_str()
        cache.put(key, "value")
        cache.delete(key)

        with raises(ValueError):
            cache.get(key)


def test_set_ttl(subtests):
    redis = new_test_redis()
    cache = RedisCache(redis)

    with subtests.test("put new"):
        key = unique_str()
        cache.put(key, "value")
        cache.set_ttl(key, 42)

        assert redis.ttl(key) == 42
