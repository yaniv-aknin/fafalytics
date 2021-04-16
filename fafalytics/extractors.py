import functools
import json

import click

from .datastore import get_client
from .parsing import read_headers, FACTIONS
from .pyutils import query_dict
from .output import yields_outputs, OUTPUT_CALLBACKS

DECODE = lambda b: b.decode()
HEADER_EXTRACT = {
    'scfa_version': 'scfa_version',
    'replay_version': 'replay_version',
    'map_name': ('scenario/name', DECODE),
    'player1_name': ('armies/0/PlayerName', DECODE),
    'player1_faction': ('armies/0/Faction', FACTIONS),
    'player1_start_spot': 'armies/0/StartSpot',
    'player2_name': ('armies/1/PlayerName', DECODE),
    'player2_faction': ('armies/1/Faction', FACTIONS),
    'player2_start_spot': 'armies/1/StartSpot',
}

@click.command()
@click.option('--output', type=click.Choice(tuple(OUTPUT_CALLBACKS)), default='datastore')
@click.argument('replays', nargs=-1, type=click.Path(exists=True, dir_okay=False))
@yields_outputs
def extract(ctx, replays):
    for replay in replays:
        json_header, binary_header = read_headers(replay)
        output = {'id': json_header['uid'], 'launched_at': json_header['launched_at'], 'game_end': json_header['game_end']}
        for key, query in HEADER_EXTRACT.items():
            output[key] = query_dict(binary_header, query)
        yield output
