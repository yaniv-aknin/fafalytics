import json

import click
import pandas as pd

from .storage import get_client

@click.command()
@click.option('--format', type=click.Choice(['parquet', 'csv']))
@click.argument('output', type=click.Path(dir_okay=False, writable=True))
def export(format, output):
    client = get_client()
    objects = {}
    for key in ('load', 'extract'):
        for game_id, json_blob in client.hgetall(key).items():
            objects.setdefault(game_id, {'id': game_id})[key] = json.loads(json_blob)
    df = pd.json_normalize(objects.values()).set_index('id')
    format = format or 'csv' if output.endswith('csv') else 'parquet'
    if format == 'csv':
        df.to_csv(output)
    else:
        df.to_parquet(output)
