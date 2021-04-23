import logging
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
