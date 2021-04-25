import logging
import json
from datetime import datetime

import click
import pandas as pd

from .storage import get_client
from .pyutils import EchoTimer
from .parsing import FACTIONS

def parse_iso8601(datestr):
    assert datestr[-1] == 'Z'
    return datetime.fromisoformat(datestr[:-1])

def get_valid_objects_keys(client):
    load_keys = set(client.hkeys('load'))
    extract_keys = set(client.hkeys('extract'))
    for subset, key in ((load_keys-extract_keys, 'extract'), (extract_keys-load_keys, 'load')):
        for item in subset:
            logging.warning('skipping %s, no %r data', item, key)
    return load_keys & extract_keys

def yield_deserilized_values(client, keys):
    for key in keys:
        yield {
            'id': key,
            'load': json.loads(client.hget('load', key)),
            'extract': json.loads(client.hget('extract', key)),
        }

def write_dataframe_in_format(objects, filename, fmt=None):
    df = pd.json_normalize(objects).set_index('id')
    fmt = fmt or 'csv' if filename.endswith('csv') else 'parquet'
    if fmt == 'csv':
        df.to_csv(filename)
    else:
        df.to_parquet(filename)

@click.group()
def export():
    "Dump datastore into a CSV/Parquet file"

def export_decorator(func):
    return (
        click.option('--format', type=click.Choice(['parquet', 'csv']))(
            click.argument('output', type=click.Path(dir_okay=False, writable=True))(func)
        )
    )

class InvalidObject(ValueError):
    pass
def build_curated_dict(obj):
    human_armies = {k: v for k, v in obj['extract']['headers']['binary']['armies'].items() if v['Human']}
    if len(human_armies) != 2:
        raise InvalidObject("%d human armies (expected 2)" % len(human_armies))
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
            'trueskill_mean_before': stats['beforeMean'],
            'trueskill_deviation_before': stats['beforeDeviation'],
            'trueskill_mean_after': stats['afterMean'],
            'trueskill_deviation_after': stats['afterDeviation'],
            'trueskill_db_matches_game': rating_matched,
            'army_num_games': int(army['NG']),
            'army_faf_rating': int(army['PL']),
        }
        result['armies'][key] = {
            'name': army['PlayerName'],
            'faction': FACTIONS.get(army['Faction'], 'NOTFOUND'),
            'start_spot': str(army['StartSpot']),
            'color': str(army['ArmyColor']),
            'result': stats['result'],
            'score': stats['score'],
        }
    return result

@export.command()
@export_decorator
def flattened(format, output):
    "Dump everything in the datastore using flattened JSONs (not recommended, messy)"
    client = get_client()
    keys = get_valid_objects_keys(client)
    with click.progressbar(keys, label='Reading datastore') as bar:
        objects = list(yield_deserilized_values(client, bar))
    with EchoTimer('Writing %d objects to dataframe' % len(objects)):
        write_dataframe_in_format(objects, output, format)

@export.command()
@export_decorator
def curated(format, output):
    "Dump specific fields from the datastore to a nice CSV/Parquet file (recommended)"
    client = get_client()
    keys = get_valid_objects_keys(client)
    objects = []
    invalid = 0
    with click.progressbar(keys, label='Reading datastore') as bar:
        for obj in yield_deserilized_values(client, bar):
            try:
                objects.append(build_curated_dict(obj))
            except InvalidObject as error:
                invalid += 1
                logging.warning('skipping %s: %s' % (obj['id'], error))
                continue
    with EchoTimer('Writing %d objects to dataframe (%d invalid/skipped)' % (len(objects), invalid)):
        write_dataframe_in_format(objects, output, format)
