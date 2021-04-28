import os
import subprocess
import shutil
import tempfile

import pytest
import psutil

from fafalytics import storage

def test_storage(redis):
    storage.is_alive()

@pytest.fixture
def redis(tmpdir):
    expected_children = psutil.Process().children()
    assert shutil.which(storage.REDIS_BINARY) is not None, \
        "can't find %S in PATH; apt install redis-server?" % REDIS_BINARY
    print(tmpdir)
    storage.configure(tmpdir)
    os.chdir(tmpdir)
    storage.start_store()
    yield
    storage.get_client.cache_clear()
    storage.stop_store()
    assert psutil.Process().children() == expected_children, "unexpected child processes; leaking redis instances?"
