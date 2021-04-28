import testutils

from fafalytics import parsing
from fafalytics.pyutils import Query as Q

EXPECTED_KEYS = frozenset(('json', 'binary', 'commands', 'remaining'))

def test_parse_v2():
    replay = testutils.testdata / '14395949.fafreplay'
    obj = parsing.get_parsed(str(replay))
    assert set(obj) == EXPECTED_KEYS
    assert Q('json/uid')(obj) == 14395949
    assert Q('commands/[3]/arg2')(obj) == 'SkyKeeper'
    assert Q('binary/armies/[1]/PlayerName')(obj) == 'SkyKeeper'

def test_parse_v1():
    replay = testutils.testdata / '14011691.fafreplay'
    obj = parsing.get_parsed(str(replay))
    assert set(obj) == EXPECTED_KEYS
    assert Q('json/uid')(obj) == 14011691

def test_parse_pickle():
    replay = testutils.testdata / '14011691.pickle'
    obj = parsing.get_parsed(str(replay))
    assert set(obj) == EXPECTED_KEYS
    assert Q('json/uid')(obj) == 14011691
