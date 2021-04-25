import logging
import functools
import json

import click

from .storage import get_client
from .parsing import get_parsed
from .output import yields_outputs, OUTPUT_CALLBACKS
from .units import units
from .manyfiles import file_processor, yield_processed_files

class ExtractorDone(StopIteration):
    pass
class Extractor:
    def __str__(self):
        return self.__class__.__name__
class TimeToFirstFactory(Extractor):
    T1_FACTORIES = frozenset((unit.id for unit, score in units.search.categories("++FACTORY ++TECH1")))
    def __init__(self):
        self.first_factory = {}
    def feed(self, command):
        if command['type'] != 'issue':
            return
        if command['cmd_data']['blueprint_id'] in self.T1_FACTORIES:
            if command['player'] in self.first_factory:
                return
            # casting offset_ms to int because the parser multiplied it by the
            # 'advance' arg of the 'Advance' command, and that was parsed as an
            # uncasted '1.0' float; I would change it only in parsing.py, but
            # then I'd have to re-parse all 500k game dataset, which sucks
            self.first_factory[command['player']] = int(command['offset_ms'])
        if len(self.first_factory) == 2:
            raise ExtractorDone()
    def emit(self):
        assert not(set(self.first_factory) - {0,1})
        return {
            'player1.first_factory_ms': self.first_factory.get(0, None),
            'player2.first_factory_ms': self.first_factory.get(1, None),
        }

class APM(Extractor):
    ACTIONS = frozenset(('issue', 'command_count_increase', 'command_count_decrease', 'factory_issue'))
    THREE_MINUTES_IN_MS = 3*60*1000
    FIVE_MINUTES_IN_MS = 5*60*1000
    def __init__(self):
        self.actions = {0: 0, 1: 0}
        self.actions_3m = None
        self.actions_5m = None
        self.last_offset = None
    @property
    def last_offset_in_minutes(self):
        if self.last_offset is None:
            raise ValueError('no command seen')
        return self.last_offset / 1000 / 60
    def feed(self, command):
        if command['type'] not in self.ACTIONS:
            return
        self.last_offset = int(command['offset_ms'])
        self.actions[command['player']] += 1
        if command['offset_ms'] > self.THREE_MINUTES_IN_MS and self.actions_3m is None:
            self.actions_3m = dict(self.actions)
        if command['offset_ms'] > self.FIVE_MINUTES_IN_MS and self.actions_5m is None:
            self.actions_5m = dict(self.actions)
    def emit(self):
        result = {
            'player1.mean_apm': None,
            'player2.mean_apm': None,
            'player1.mean_apm_first_3m': None,
            'player2.mean_apm_first_3m': None,
            'player1.mean_apm_first_5m': None,
            'player2.mean_apm_first_5m': None,
        }
        if self.last_offset is not None:
            result.update({
                'player1.mean_apm': self.actions[0] / self.last_offset_in_minutes,
                'player2.mean_apm': self.actions[1] / self.last_offset_in_minutes,
            })
        if self.actions_3m is not None:
            result.update({
                'player1.mean_apm_first_3m': self.actions_3m[0] / 3,
                'player2.mean_apm_first_3m': self.actions_3m[1] / 3,
            })
        if self.actions_5m is not None:
            result.update({
                'player1.mean_apm_first_5m': self.actions_5m[0] / 5,
                'player2.mean_apm_first_5m': self.actions_5m[1] / 5,
            })
        return result

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

def extract_replay(filename):
    replay = get_parsed(filename)
    replay['binary']['last_tick'] = replay['remaining']['last_tick']
    replay['binary'].pop('players')
    replay['binary'].pop('scenario')
    desyncs = replay['remaining']['desync_ticks']
    replay['binary']['desync'] = {'count': len(desyncs),
                                  'ticks': ','.join(str(t) for t in desyncs)}
    extracted = run_extractors(
        replay['commands'],
        TimeToFirstFactory(),
        APM(),
    )
    return {'id': replay['json']['uid'], 'headers': {'json': replay['json'], 'binary': replay['binary']}, 'extracted': extracted}

@click.command()
@file_processor
@yields_outputs
def extract(ctx, max_errors, infiles):
    "Read replay file and populate the datastore with features extracted from it."
    with click.progressbar(infiles, label='Extracting') as bar:
        yield from yield_processed_files(bar, extract_replay, max_errors)
