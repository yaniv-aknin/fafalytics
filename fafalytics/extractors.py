import logging
import functools
import json

import click

from .storage import get_client
from .parsing import get_parsed
from .output import yields_outputs, OUTPUT_CALLBACKS
from .units import id_by_categories as C
from .manyfiles import file_processor, yield_processed_files
from .logs import log_invocation

class Extractor:
    def __str__(self):
        return self.__class__.__name__
class TimeToFirst(Extractor):
    "A feature extractor focused on game timeline. 'Time to first T2 mexer', etc"
    FEATURES = {
        't1_factory': C('factory tech1'),
        't1_land': C('factory tech1 land'),
        't1_air': C('factory tech1 air'),
        't1_naval': C('factory tech1 naval'),
        't2_factory': C('factory tech2 -supportfactory'),
        't2_support': C('tech2 supportfactory'),
        't1_mexer': C('tech1 massextraction'),
        't2_mexer': C('tech2 massextraction'),
        't3_mexer': C('tech3 massextraction'),
    }
    def __init__(self):
        self.features = self.FEATURES.copy()
        self.result = {0: {k: None for k in self.FEATURES},
                       1: {k: None for k in self.FEATURES}}
    def feed(self, command):
        if not self.features:
            return
        if command['type'] != 'issue':
            return
        assert command['player'] in (0, 1)
        # casting offset_ms to int because the parser multiplied it by the
        # 'advance' arg of the 'Advance' command, and that was parsed as an
        # uncasted '1.0' float; I would change it only in parsing.py, but
        # then I'd have to re-parse all 500k game dataset, which sucks
        self.update(command['cmd_data']['blueprint_id'],
                    command['player'], int(command['offset_ms']))
    def update(self, unit_id, player, offset):
        for feature, unit_ids in tuple(self.features.items()):
            if unit_id not in unit_ids:
                continue
            if self.result[player][feature]:
                continue
            self.result[player][feature] = offset
            other_player = (player + 1) % 2
            if self.result[other_player][feature]:
                self.features.pop(feature)
    def emit(self):
        return {'player%d' % player+1: {'first': features}
                for player, features in self.result.items()}

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
    for command in commands:
        for extractor in extractors:
            extractor.feed(command)
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
        TimeToFirst(),
        APM(),
    )
    return {'id': replay['json']['uid'], 'headers': {'json': replay['json'], 'binary': replay['binary']}, 'extracted': extracted}

@click.command()
@log_invocation
@file_processor
@yields_outputs
def extract(ctx, max_errors, infiles):
    "Read replay file and populate the datastore with features extracted from it."
    with click.progressbar(infiles, label='Extracting') as bar:
        yield from yield_processed_files(bar, extract_replay, max_errors)
