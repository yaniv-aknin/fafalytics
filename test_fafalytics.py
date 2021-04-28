import os
import shutil
import tempfile

import pytest
from unittest import TestCase

from fafalytics.pyutils import negate, Query, restructure_dict, Literal
from fafalytics import storage

def test_negate():
    true = lambda: True
    assert negate(true)() is False

def test_query():
    def missing(obj, component):
        assert component == 'bar'
        return 5
    assert Query('foo')({'foo': 1}) == 1
    assert Query('foo/bar')({'foo': {'bar': 1}}) == 1
    assert Query('foo/bar', missing=missing)({'foo': {}}) == 5
    assert Query(Literal(None))({}) is None
    with TestCase().assertRaises(RuntimeError):
        Query('foo', reraise=RuntimeError)({})

def test_restructure_dict():
    src = {'foo': 'bar', 'qux': {'baz': 5}}
    queries = {
        'shlaq': Query('foo', cast=lambda s: s[0]),
        'shliq/shlaq': Query('qux/baz'),
        'shloq': Query('qux/quux', missing=lambda x,y: 7),
    }
    result = restructure_dict(src, queries)
    expected = {'shlaq': 'b', 'shliq': {'shlaq': 5}, 'shloq': 7}
    TestCase().assertDictEqual(expected, result)

def test_storage(redis):
    storage.is_alive()

@pytest.fixture
def redis():
    assert shutil.which(storage.REDIS_BINARY) is not None, \
        "can't find %S in PATH; apt install redis-server?" % REDIS_BINARY
    with tempfile.TemporaryDirectory(prefix='fafalytics') as tmpdir:
        storage.configure(tmpdir)
        os.chdir(tmpdir)
        storage.start_store()
        yield
        storage.get_client.cache_clear()
        storage.stop_store()
