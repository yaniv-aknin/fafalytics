import json

import zstd
import click

from .storage import get_client
from .manyfiles import yields_outputs
from .logs import log_invocation

class GameJsonResolver:
    inline_relationships = (
        ('playerStats',),
        ('playerStats', 'player'),
        ('mapVersion',),
        ('mapVersion', 'map'),
    )
    keep_relationships = (
        ('featuredMod',),
    )
    def __init__(self, doc: dict):
        self.doc = doc
        self.objects = {}
        self.known_types = set()
        if set(self.doc) != {'data', 'meta', 'included'}:
            raise ValueError('expected an api.faforever.com/data/game dump')
        if self.doc['data'][0]['type'] != 'game':
            raise ValueError('expected game models from api.faforever.com/data/game')
    @classmethod
    def from_handle(cls, handle):
        buf = handle.read()
        if handle.name.endswith('.zst'):
            buf = zstd.decompress(buf)
        instance = cls(json.loads(buf))
        instance.populate()
        return instance
    def populate(self):
        for included in self.doc['included']:
            self.known_types.add(included['type'])
            self.objects[self.key(included)] = included
    def find(self, type, id):
        return self.objects[type, id]
    @staticmethod
    def key(obj):
        return obj['type'], obj['id']
    def resolve(self, obj, path):
        result = {'id': obj['id'], 'type': obj['type']}
        result.update(obj['attributes'])
        for rel_type, rel_data in obj.get('relationships', {}).items():
            rel_data = rel_data['data']
            new_path = path + (rel_type,)
            if new_path in self.keep_relationships:
                result[rel_type] = rel_data['id']
            if new_path not in self.inline_relationships:
                continue
            if isinstance(rel_data, list):
                result[rel_type] = {str(index): self.resolve(self.find(**datum), new_path) for index, datum in enumerate(rel_data)}
            elif isinstance(rel_data, dict):
                result[rel_type] = self.resolve(self.find(**rel_data), new_path)
            elif rel_data is None:
                continue
            else:
                raise ValueError('unexpected relationship data %r' % rel_data)
        return result
    def __iter__(self):
        for game in self.doc['data']:
            yield self.resolve(game, ())

def permit_game(game, only_valid=True, only_1v1=True, featured_mod=6):
    if only_valid and game['validity'] != 'VALID':
        return False
    if only_1v1 and len(game['playerStats']) != 2:
        return False
    if featured_mod != -1 and game['featuredMod'] != featured_mod:
        return False
    return True

@click.command()
@log_invocation
@click.option('--only-valid/--all-games', default=True)
@click.option('--only-1v1/--any-number-of-players', default=True)
@click.option('--featured-mod', type=int, default=6)
@click.argument('jsons', nargs=-1, type=click.File('rb'))
@yields_outputs
def load(output, only_valid, only_1v1, featured_mod, jsons):
    "Load Game model JSONs into datastore"
    with click.progressbar(jsons, label='Loading') as bar:
        for json in bar:
            for game in GameJsonResolver.from_handle(json):
                if not permit_game(game, only_valid, only_1v1, featured_mod):
                    continue
                yield game
