#!/usr/bin/env python

import sys
import argparse
import json
import subprocess

import fafreplay
import zstd
import click

ALL_COMMANDS = tuple(getattr(fafreplay.commands, c) for c in fafreplay.commands.__all__ if c != '__doc__')

def read_header_and_body(filename, save_commands=True, commands=ALL_COMMANDS):
    with open(filename, 'rb') as handle:
        header = json.loads(handle.readline().decode())
        decompressed = zstd.decompress(handle.read())
    parser = fafreplay.Parser(save_commands=save_commands, commands=commands)
    body = parser.parse(decompressed)
    return header, body

@click.command()
@click.argument('replay')
def main(replay):
    header, body = read_header_and_body(replay)
    import code
    code.interact(local=locals())
