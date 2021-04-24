import logging
import click
import pandas as pd
import pickle
from os import path
import base64
import datetime
import json
import zlib

from typing import Iterable

from replay_parser import replay
import zstd

from .pyutils import Timer

ALL_COMMANDS = tuple(range(24))
FACTIONS = lambda x: {k:v for k,v in enumerate('uef aeon cybran seraphim nomads unknown'.split())}[x-1]

def extract_v1(buf):
    decoded = base64.decodebytes(buf)
    decoded = decoded[4:] # skip 4 bytes of zlib stream length
    return zlib.decompress(decoded)

def extract_v2(buf):
    return zstd.decompress(buf)

def read_header_and_body(filename: str, store_body: bool=True, parse_commands: Iterable=ALL_COMMANDS):
    with open(filename, 'rb') as handle:
        header = json.loads(handle.readline().decode())
        buf = handle.read()
        version = header.get('version', 1)
        if version == 1:
            extracted = extract_v1(buf)
        elif version == 2:
            extracted = extract_v2(buf)
        else:
            raise ValueError("unknown version %s" % version)
    with Timer() as timer:
        body = replay.parse(extracted, store_body=store_body, parse_commands=parse_commands)
        logging.debug('parsed in %.2f seconds', timer.elapsed)
    return header, body

def yield_command_at_offsets(body):
    TICK_MILLISECONDS = 100
    offset_ms = 0
    for atom in body:
        for player, commands in atom.items():
            for command, args in commands.items():
                if command == 'Advance':
                    offset_ms += TICK_MILLISECONDS * args['advance']
                elif command in ('VerifyChecksum', 'SetCommandSource'):
                    continue
                else:
                    assert 'offset_ms' not in args
                    assert 'player' not in args
                    args['offset_ms'] = offset_ms
                    args['player'] = player
                    yield args

def get_parsed(filename):
    with open(filename, 'rb') as handle:
        raw = handle.read()
    if filename.endswith('pickle'):
        return pickle.loads(zstd.decompress(raw))
    header, body = read_header_and_body(filename)
    return {
        'json': header,
        'binary': body.pop('header'),
        'commands': list(yield_command_at_offsets(body.pop('body'))),
        'remaining': body,
    }

@click.command()
@click.option('--max-errors', type=int)
@click.argument('outdir', type=click.Path(exists=True, dir_okay=True, file_okay=False))
@click.argument('replays', nargs=-1, type=click.Path(exists=True, dir_okay=False))
def unpack(max_errors, outdir, replays):
    "Unpack and pre-parse replay files, making them much faster to read on subsequent reads."
    if max_errors is None:
        max_errors = float('inf')
    durations = []
    with click.progressbar(replays, label='Unpacking') as bar:
        for replay in bar:
            try:
                with Timer() as timer:
                    json_header, body = read_header_and_body(replay)
                    binary_header = body.pop('header')
                    commands = list(yield_command_at_offsets(body.pop('body')))
                    blob = pickle.dumps({'json': json_header, 'binary': binary_header, 'remaining': body, 'commands': commands})
                    compressed = zstd.compress(blob)
                    base, ext = path.splitext(path.basename(replay))
                    with open(path.join(outdir, base+'.pickle'), 'wb') as handle:
                        handle.write(compressed)
                    durations.append(timer.elapsed)
            except Exception as error:
                logging.error('extract: replay %s raised %s:%s', replay, error.__class__.__name__, error)
    stats = dict(pd.Series(durations).describe())
    stats['sum'] = sum(durations)
    logging.info('processed: %s', ','.join('%s=%.1f' % (k,v) for k,v in stats.items()))
