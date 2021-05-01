import testutils
from unittest import TestCase

import pytest

from fafalytics.extractors import first, apm, run_extractors
from fafalytics.pyutils import Query as Q
from fafalytics import parsing

def test_extract_time_to_first(replay_14395949):
    obj = run_extractors(replay_14395949['commands'], first.TimeToFirst())
    assert Q('player1/first/t1_land')(obj) == 9200
    assert Q('player1/first/t1_air')(obj) == None

def test_extract_actions_per_minute(replay_14395949):
    obj = run_extractors(replay_14395949['commands'], apm.APM({apm.Minute(3): 'first_3m', apm.Minute(5): 'first_5m'}))
    TestCase().assertAlmostEqual(Q('player1.mean_apm.overall')(obj), 16.6859, places=2)
    TestCase().assertAlmostEqual(Q('player2.mean_apm.overall')(obj), 23.5266, places=2)

@pytest.fixture
def replay_14395949():
    replay = testutils.testdata / '14395949.fafreplay'
    return parsing.get_parsed(str(replay))
