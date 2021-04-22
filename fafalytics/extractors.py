import functools
import json

import click

from .datastore import get_client
from .parsing import read_header_and_body, yield_command_at_offsets
from .output import yields_outputs, OUTPUT_CALLBACKS
from .units import units

class ExtractorDone(StopIteration):
    pass
class TimeToFirstFactory:
    T1_FACTORIES = frozenset((unit.id for unit, score in units.search.categories("++FACTORY ++TECH1")))
    def __init__(self):
        self.first_factory = {}
    def feed(self, command):
        if command['type'] != 'issue':
            return
        if command['cmd_data']['blueprint_id'] in self.T1_FACTORIES:
            if command['player'] in self.first_factory:
                return
            self.first_factory[command['player']] = command['offset_ms']
        if len(self.first_factory) == 2:
            raise ExtractorDone()
    def emit(self):
        assert not(set(self.first_factory) - {0,1})
        return {
            'player1_first_factory_ms': self.first_factory.get(0, None),
            'player2_first_factory_ms': self.first_factory.get(1, None),
        }

def run_extractors(commands, *extractors):
    active_extractors = set(extractors)
    for command in commands:
        for extractor in tuple(active_extractors):
            try:
                extractor.feed(command)
            except ExtractorDone:
                active_extractors.remove(extractor)
        if not active_extractors:
            break
    result = {}
    for extractor in extractors:
        result.update(extractor.emit())
    return result

@click.command()
@click.option('--skip-desynced/--no-skip-desynced', default=True)
@click.argument('replays', nargs=-1, type=click.Path(exists=True, dir_okay=False))
@yields_outputs
def extract(ctx, skip_desynced, replays):
    for replay in replays:
        json_header, body = read_header_and_body(replay)
        binary_header = body['header']
        if skip_desynced and body['desync_ticks']:
            continue
        extracted = run_extractors(
            yield_command_at_offsets(body['body']),
            TimeToFirstFactory(),
        )
        yield {'id': json_header['uid'], 'headers': {'json': json_header, 'binary': binary_header}, 'extracted': extracted}
