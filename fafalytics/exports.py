import logging
import json
from datetime import datetime
from codecs import decode
from functools import partial

import click
import pandas as pd

from .storage import get_client
from .pyutils import EchoTimer, Query, restructure_dict, Literal
from .parsing import FACTIONS
from .logs import log_invocation

def parse_iso8601(datestr):
    assert datestr[-1] == 'Z'
    return datetime.fromisoformat(datestr[:-1])

def int_or_none(obj):
    try:
        return int(obj)
    except (TypeError, ValueError):
        return None

def get_valid_game_ids(client):
    logging.info('loading game ids')
    load_keys = set(client.hkeys('load'))
    extract_keys = set(client.hkeys('extract'))
    missing_extracts = load_keys - extract_keys
    missing_loads = extract_keys - load_keys
    for keys, counterpart in ((missing_extracts, 'load'), (missing_loads, 'extract')):
        if not keys:
            continue
        logging.warn('skipping %d games with no %r data', len(keys), counterpart)
        for key in keys:
            logging.debug('game %s has no %r', key, counterpart)
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

class InvalidObject(ValueError):
    pass
Q = partial(Query, reraise=InvalidObject)
def null(obj, component):
    return None
BASE_STRUCTURE = {
    'id':                         Q('id', cast=decode),
    'meta/title':                 Q('extract/headers/json/title'),
    'meta/replay':                Q('load/replayUrl'),
    'map/name':                   Q('load/mapVersion/map/displayName'),
    'map/version':                Q('load/mapVersion/id'),
    'map/width':                  Q('load/mapVersion/width'),
    'map/height':                 Q('load/mapVersion/height'),
    'durations/database.start':   Q('load/endTime', cast=parse_iso8601),
    'durations/database.end':     Q('load/endTime', cast=parse_iso8601),
    'durations/header.start':     Q('extract/headers/json/launched_at', cast=datetime.fromtimestamp, missing=null),
    'durations/header.end':       Q('extract/headers/json/game_end', cast=datetime.fromtimestamp, missing=null),
    'durations/ticks':            Q('extract/headers/binary/last_tick'),
    'features':                   Q('extract/extracted'),
    'players':                    Q(Literal({})),
    'armies':                     Q(Literal({})),
}
PLAYER_STRUCTURE = {
    'id':                         Q('player/id'),
    'login':                      Q('player/login'),
    'playing_since':              Q('player/createTime', cast=parse_iso8601),
    'trueskill_mean_before':      Q('beforeMean'),
    'trueskill_deviation_before': Q('beforeDeviation'),
    'trueskill_mean_after':       Q('afterMean'),
    'trueskill_deviation_after':  Q('afterDeviation'),
}

def build_curated_dict(obj):
    human_armies = {k: v for k, v in obj['extract']['headers']['binary']['armies'].items() if v['Human']}
    if len(human_armies) != 2:
        raise InvalidObject("%d human armies (expected 2)" % len(human_armies))
    result = restructure_dict(obj, BASE_STRUCTURE)
    all_stats = obj['load']['playerStats'].values()
    stats_by_owner_id = {stats['player']['id']: stats for stats in all_stats}
    for player_index in (0, 1):
        key = 'player%d' % (player_index + 1)
        army = human_armies[str(player_index)]
        try:
            stats = stats_by_owner_id[army['OwnerID']]
        except KeyError:
            raise InvalidObject(
                'army with owner %s has no stats (found %s)' % (army['OwnerID'], ','.join(str(k) for k in stats_by_owner_id)))
        login = stats['player']['login']
        rating_matched = True
        if 'MEAN' in army and abs(army['MEAN'] - stats['beforeMean']) < 1:
            logging.debug('game %s has db vs replay beforeMean mismatch - db=%s, game=%s',
                          obj['id'], stats['beforeMean'], army['MEAN'])
            rating_matched = False
        if army['Faction'] != stats['faction']:
            raise InvalidObject("replay/db disagree on %r faction (%s vs %s)" % (login, army['Faction'], stats['faction']))

        result['players'][key] = restructure_dict(stats, PLAYER_STRUCTURE)
        result['players'][key]['trueskill_db_matches_game'] = rating_matched
        # these two variables are odd; they're set on 'army', but they belong
        # with 'player'; they're sometimes missing altogether, and sometimes
        # are '' (empty string), which can't be casted.
        result['players'][key]['army_num_games'] = int_or_none(army.get('NG'))
        result['players'][key]['army_faf_rating'] = int_or_none(army.get('PL'))

        result['armies'][key] = {
            'name': army['PlayerName'],
            'faction': FACTIONS.get(army['Faction'], 'NOTFOUND'),
            'start_spot': str(army['StartSpot']),
            'color': str(army['ArmyColor']),
            'result': stats['result'],
            'score': stats['score'],
        }
    return result

@click.group()
@log_invocation
@click.option('--format', type=click.Choice(['parquet', 'csv']))
@click.option('--game-ids', multiple=True, type=int)
@click.argument('output', type=click.Path(dir_okay=False, writable=True))
@click.pass_context
def export(ctx, format, game_ids, output):
    "Dump datastore into a CSV/Parquet file"
    client = get_client()
    game_ids = [str(game_id).encode() for game_id in game_ids]
    if not game_ids:
        game_ids = get_valid_game_ids(client)
    ctx.obj = (client, game_ids, format, output)

@export.resultcallback()
def export_callback(retval, format, game_ids, output):
    objects, invalid = retval
    if not objects:
        click.secho('(nothing to write)')
        return
    with EchoTimer('Writing %d objects to dataframe (%d invalid/skipped)' % (len(objects), invalid)):
        write_dataframe_in_format(objects, ctx.parent.args['output'], ctx.parent.params['format'])

@export.command()
@click.pass_context
def flattened(ctx):
    "Dump everything in the datastore using flattened JSONs (not recommended, messy)"
    client, game_ids, format, output = ctx.obj
    with click.progressbar(game_ids, label='Reading datastore') as bar:
        objects = list(yield_deserilized_values(client, bar))
    return objects, invalid

@export.command()
@click.pass_context
def curated(ctx):
    "Dump specific fields from the datastore to a nice CSV/Parquet file (recommended)"
    client, game_ids, format, output = ctx.obj
    objects = []
    invalid = 0
    with click.progressbar(game_ids, label='Reading datastore') as bar:
        for obj in yield_deserilized_values(client, bar):
            try:
                objects.append(build_curated_dict(obj))
            except InvalidObject as error:
                invalid += 1
                logging.warning('skipping %s: %s' % (obj['id'], error))
                continue
    return objects, invalid
