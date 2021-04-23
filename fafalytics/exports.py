import json

import click
import pandas as pd

from .storage import get_client

@click.command()
@click.option('--format', type=click.Choice(['parquet', 'csv']))
@click.argument('output', type=click.Path(dir_okay=False, writable=True))
def export(format, output):
    client = get_client()
    game_ids = client.keys('game.*')
    df = pd.json_normalize([json.loads(game) for game in client.mget(game_ids)]).set_index('id')
    format = format or 'csv' if output.endswith('csv') else 'parquet'
    if format == 'csv':
        df.to_csv(output)
    else:
        df.to_parquet(output)
