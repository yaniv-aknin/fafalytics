import base64
import json
import zlib

from typing import Iterable

import fafreplay
import zstd

ALL_COMMANDS = tuple(getattr(fafreplay.commands, c) for c in fafreplay.commands.__all__ if c != '__doc__')

def extract_v1(buf):
    decoded = base64.decodebytes(buf)
    decoded = decoded[4:] # skip 4 bytes of zlib stream length
    return zlib.decompress(decoded)

def extract_v2(buf):
    return zstd.decompress(buf)

def read_header_and_body(filename: str, save_commands: bool=True, commands: Iterable=ALL_COMMANDS):
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
    parser = fafreplay.Parser(save_commands=save_commands, commands=commands)
    body = parser.parse(extracted)
    return header, body

def read_headers(filename: str) -> (dict, dict):
    header, body = read_header_and_body(filename, save_commands=False, commands=[])
    return header, body['header']
