from os import path
import base64
import functools
import json
import logging
import pickle
import zlib

from typing import Iterable

import click
from replay_parser import replay as replay_parser
import zstd

from .pyutils import Timer
from .manyfiles import file_processor, process_all_files

ALL_COMMANDS = tuple(range(24))

# See Zulip discussion on the faction order, mapping factions isn't trivial:
#  https://faforever.zulipchat.com/#narrow/stream/203478-general/topic/Faction.20Order/near/235006069
# I went with Sheikah's approach:
#  > You can continue this discussion but for the client I will just use the
#  > four basic ones and everything else will be considered random as that is
#  > the only consistent thing
FACTION_LOOKUP = {1: 'uef', 2: 'aeon', 3: 'cybran', 4: 'seraphim'}
def map_faction(faction_id):
    return FACTION_LOOKUP.get(faction_id, 'UNDEFINED')

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
        body = replay_parser.parse(extracted, store_body=store_body, parse_commands=parse_commands)
        logging.debug('parsed in %.2f seconds', timer.elapsed)
    return header, body

def get_command_timeseries(body):
    "Given an iterable of raw replay commands, return higher level timestamped stream."
    TICK_MILLISECONDS = 100
    offset_ms = 0
    result = []
    for atom in body:
        for player, commands in atom.items():
            for command, args in commands.items():
                if command == 'Advance':
                    offset_ms += TICK_MILLISECONDS * int(args['advance'])
                    continue
                if command in ('VerifyChecksum', 'SetCommandSource'):
                    continue
                assert 'offset_ms' not in args
                assert 'player' not in args
                args['offset_ms'] = offset_ms
                args['player'] = player
                result.append(args)
    return result

def get_parsed(filename):
    with open(filename, 'rb') as handle:
        raw = handle.read()
    if filename.endswith('pickle'):
        return pickle.loads(zstd.decompress(raw))
    header, body = read_header_and_body(filename)
    return {
        'json': header,
        'binary': body.pop('header'),
        'commands': get_command_timeseries(body.pop('body')),
        'remaining': body,
    }

def unpack_replay(outdir, replay):
    json_header, body = read_header_and_body(replay)
    binary_header = body.pop('header')
    commands = get_command_timeseries(body.pop('body'))
    blob = pickle.dumps({'json': json_header, 'binary': binary_header, 'remaining': body, 'commands': commands})
    compressed = zstd.compress(blob)
    base, ext = path.splitext(path.basename(replay))
    with open(path.join(outdir, base+'.pickle'), 'wb') as handle:
        handle.write(compressed)

@click.command()
@click.option('--outdir', type=click.Path(exists=True, dir_okay=True, file_okay=False), default='.')
@file_processor
def unpack(outdir, max_errors, infiles):
    "Unpack and pre-parse replay files, making them much faster to read on subsequent reads."
    with click.progressbar(infiles, label='Unpacking') as bar:
        process_all_files(bar, functools.partial(unpack_replay, outdir), max_errors)
