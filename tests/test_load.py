import testutils

from fafalytics import loader
from fafalytics.pyutils import Query as Q

def load(path):
    with open(path, 'rb') as handle:
        resolver = loader.GameJsonResolver.from_handle(handle)
        return {game['id']: game for game in resolver}

def test_load():
    games = load(testutils.testdata / 'dump.json')
    game = games['14395861']
    assert Q('playerStats/0/id')(game) == '28030229'

def test_load_zstd():
    games = load(testutils.testdata / 'dump.json.zst')
    game = games['14395861']
    assert Q('playerStats/0/id')(game) == '28030229'
