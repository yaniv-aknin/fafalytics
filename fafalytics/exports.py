import csv
import json

import click

from .datastore import get_client

@click.command()
@click.argument('output', type=click.File(mode='w'))
def export(output):
    client = get_client()
    game_ids = client.keys('ex.*')
    keys = ['id'] + list(sorted(x.decode() for x in client.hgetall(game_ids[0]).keys() if x != b'id'))
    with output:
        writer = csv.DictWriter(output, keys)
        writer.writeheader()
        for game_id in game_ids:
            obj = client.hgetall(game_id)
            unjsonified = {k.decode(): json.loads(v) for k, v in obj.items()}
            writer.writerow(unjsonified)
