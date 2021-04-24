import logging
import json
from datetime import datetime

import click
import pandas as pd

from .storage import get_client

def parse_iso8601(datestr):
    assert datestr[-1] == 'Z'
    return datetime.fromisoformat(datestr[:-1])

def get_all_objects():
    client = get_client()
    objects = {}
    for key in ('load', 'extract'):
        for game_id, json_blob in client.hgetall(key).items():
            objects.setdefault(game_id, {'id': game_id})[key] = json.loads(json_blob)
    return objects

def to_format(df, filename, format=None):
    format = format or 'csv' if filename.endswith('csv') else 'parquet'
    if format == 'csv':
        df.to_csv(filename)
    else:
        df.to_parquet(filename)

@click.group()
def export():
    pass

def export_decorator(func):
    return (
        click.option('--format', type=click.Choice(['parquet', 'csv']))(
            click.argument('output', type=click.Path(dir_okay=False, writable=True))(func)
        )
    )

@export.command()
@export_decorator
def flattened(format, output):
    objects = get_all_objects()
    df = pd.json_normalize(objects.values()).set_index('id')
    to_format(df, output, format)

@export.command()
@export_decorator
def curated(format, output):
    objects = get_all_objects()
    results = []
    for obj in objects.values():
        if 'load' not in obj or 'extract' not in obj:
            assert 'id' in obj
            logging.warning('skipping partial game %s', obj['id'])
            continue
        human_armies = {k: v for k, v in obj['extract']['headers']['binary']['armies'].items() if v['Human']}
        if len(human_armies) != 2:
            logging.warning("skipping game %s which has %d human armies (expected 2)", obj['id'], len(human_armies))
            continue
        result = {
            'id': obj['id'].decode(),
            'meta': {
                'title': obj['extract']['headers']['json']['title'],
                'replay': obj['load']['replayUrl'],
            },
            'map': {
                'name': obj['load']['mapVersion']['map']['displayName'],
                'version': obj['load']['mapVersion']['id'],
                'width': obj['load']['mapVersion']['width'],
                'height': obj['load']['mapVersion']['height'],
            },
            'durations': {
                'database.start': parse_iso8601(obj['load']['endTime']),
                'database.end': parse_iso8601(obj['load']['startTime']),
                'header.start': datetime.fromtimestamp(obj['extract']['headers']['json']['launched_at']),
                'header.end': datetime.fromtimestamp(obj['extract']['headers']['json']['game_end']),
                'ticks': obj['extract']['headers']['binary']['last_tick'],
            },
            'features': obj['extract']['extracted'],
            'players': {},
            'armies': {},
        }
        all_stats = obj['load']['playerStats'].values()
        stats_by_owner_id = {stats['player']['id']: stats for stats in all_stats}
        for player_index in (0, 1):
            key = 'player%d' % (player_index + 1)
            army = human_armies[str(player_index)]
            stats = stats_by_owner_id[army['OwnerID']]
            login = stats['player']['login']
            rating_matched = True
            if abs(army['MEAN'] - stats['beforeMean']) < 1:
                logging.debug('game %s has db vs replay beforeMean mismatch - db=%s, game=%s',
                              obj['id'], stats['beforeMean'], army['MEAN'])
                rating_matched = False
            assert army['Faction'] == stats['faction']
            result['players'][key] = {
                'id': stats['player']['id'],
                'login': stats['player']['login'],
                'playing_since': parse_iso8601(stats['player']['createTime']),
                'num_games': army['NG'],
                'trueskill_mean_before': stats['beforeMean'],
                'trueskill_deviation_before': stats['beforeDeviation'],
                'trueskill_mean_after': stats['afterMean'],
                'trueskill_deviation_after': stats['afterDeviation'],
                'trueskill_db_matches_game': rating_matched,
                'faf_rating': army['PL'],
            }
            result['armies'][key] = {
                'name': army['PlayerName'],
                'faction': army['Faction'],
                'start_spot': army['StartSpot'],
                'color': army['ArmyColor'],
                'result': stats['result'],
                'score': stats['score'],
            }
        results.append(result)
    df = pd.json_normalize(results).set_index('id')
    to_format(df, output, format)
