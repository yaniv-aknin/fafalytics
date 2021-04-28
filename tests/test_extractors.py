import testutils

import pytest

from fafalytics.extractors import first, run_extractors
from fafalytics.pyutils import Query as Q
from fafalytics import parsing

def test_extract_time_to_first(replay_14395949):
    obj = run_extractors(replay_14395949['commands'], first.TimeToFirst())
    assert Q('player1/first/t1_land')(obj) == 9200
    assert Q('player1/first/t1_air')(obj) == None

@pytest.fixture
def replay_14395949():
    replay = testutils.testdata / '14395949.fafreplay'
    return parsing.get_parsed(str(replay))
