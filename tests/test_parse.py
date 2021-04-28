import testutils

from fafalytics import parsing
from fafalytics.pyutils import Query as Q

def test_parse():
    replay = testutils.testdata / '14395949.fafreplay'
    obj = parsing.get_parsed(str(replay))
    expected_keys = set(('json', 'binary', 'commands', 'remaining'))
    assert set(obj) == expected_keys
    assert Q('json/uid')(obj) == 14395949

