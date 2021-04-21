import json

import click
import pandas as pd

from .datastore import get_client

@click.command()
@click.argument('output', type=click.File(mode='w'))
def export(output):
    client = get_client()
    game_ids = client.keys('game.*')
    df = pd.DataFrame([json.loads(game) for game in client.mget(game_ids)])
    df.set_index('id').to_csv(output)
