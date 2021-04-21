import functools
import json

import click

from .datastore import get_client
from .parsing import read_headers, FACTIONS
from .pyutils import query_dict
from .output import yields_outputs, OUTPUT_CALLBACKS

@click.command()
@click.argument('replays', nargs=-1, type=click.Path(exists=True, dir_okay=False))
@yields_outputs
def extract(ctx, replays):
    for replay in replays:
        json_header, binary_header = read_headers(replay)
        yield {'id': json_header['uid'], 'headers': {'json': json_header, 'binary': binary_header}}
