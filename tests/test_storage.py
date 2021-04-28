import os
import shutil
import tempfile

import pytest

from fafalytics import storage

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
