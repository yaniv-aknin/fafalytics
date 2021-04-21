import json

import click

from .datastore import get_client
from .output import OUTPUT_CALLBACKS, yields_outputs
from .pyutils import query_dict

class GameJsonResolver:
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
        instance = cls(json.load(handle))
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
    def resolve(self, obj, path=frozenset()):
        key = self.key(obj)
        if key in path:
            raise LookupError('%r in %r' % (key, path))
        result = {'id': obj['id'], 'type': obj['type']}
        result.update(obj['attributes'])
        for rel_type, rel_data in obj.get('relationships', {}).items():
            rel_data = rel_data['data']
            if rel_type not in self.known_types or not rel_data:
                continue
            if isinstance(rel_data, list):
                result[rel_type] = [self.resolve(self.find(**datum), path.union({key})) for datum in rel_data]
            elif isinstance(rel_data, dict):
                result[rel_type] = self.resolve(self.find(**rel_data), path.union({key}))
            else:
                raise ValueError('unexpected relationship data %r' % rel_data)
        return result
    def __iter__(self):
        for game in self.doc['data']:
            yield self.resolve(game)

@click.command()
@click.argument('jsons', nargs=-1, type=click.File('r'))
@yields_outputs
def load(output, jsons):
    for json in jsons:
        yield from GameJsonResolver.from_handle(json)
