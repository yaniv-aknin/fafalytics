import testutils

from fafalytics import loader
from fafalytics.pyutils import Query as Q

def test_load():
    dump = testutils.testdata / 'dump.json'
    with open(dump) as handle:
        resolver = loader.GameJsonResolver.from_handle(handle)
        games = {game['id']: game for game in resolver}
    game = games['14395861']
    assert Q('playerStats/0/id')(game) == '28030229'
