import logging
import json
from datetime import datetime
from codecs import decode
from functools import partial

import click
import pandas as pd

from .storage import get_client
from .pyutils import EchoTimer, Query, restructure_dict, Literal, null
from .parsing import map_faction
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
    for keys, counterpart in ((missing_extracts, 'extract'), (missing_loads, 'load')):
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

class InvalidObject(ValueError):
    pass
Q = partial(Query, reraise=InvalidObject)
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
}
PLAYER_STRUCTURE = {
    'player/id':                         Q('stats/player/id'),
    'player/login':                      Q('stats/player/login'),
    'player/playing_since':              Q('stats/player/createTime', cast=parse_iso8601),
    'player/trueskill_mean_before':      Q('stats/beforeMean'),
    'player/trueskill_deviation_before': Q('stats/beforeDeviation'),
    'player/trueskill_mean_after':       Q('stats/afterMean'),
    'player/trueskill_deviation_after':  Q('stats/afterDeviation'),
    'player/result':                     Q('stats/result'),
    'player/score':                      Q('stats/score'),
    'army/name':                         Q('army/PlayerName'),
    'army/faction':                      Q('army/Faction', cast=map_faction),
    'army/start_spot':                   Q('army/StartSpot', cast=str),
    'army/color':                        Q('army/ArmyColor', cast=str),
    # these two variables are odd; they're set on 'army', but they belong
    # with 'player'; they're sometimes missing altogether, and sometimes
    # are '' (empty string), which cant easily be casted with int()
    'player/army_num_games':             Q('army/NG', cast=int_or_none, missing=null),
    'player/army_faf_rating':            Q('army/PL', cast=int_or_none, missing=null),
}

def match_player_stats_to_armies(obj):
    human_armies = {k: v for k, v in obj['extract']['headers']['binary']['armies'].items() if v['Human']}
    if len(human_armies) != 2:
        raise InvalidObject("%d human armies (expected 2)" % len(human_armies))
    all_stats = obj['load']['playerStats'].values()
    stats_by_owner_id = {stats['player']['id']: stats for stats in all_stats}
    for player_index in (0, 1):
        player_key = 'player%d' % (player_index + 1)
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
        if army['Faction'] != stats['faction']:
            raise InvalidObject("replay/db disagree on %r faction (%s vs %s)" % (login, army['Faction'], stats['faction']))
        obj.setdefault('sides', {})
        obj['sides'][player_key] = {'army': army, 'stats': stats}

def build_curated_dict(obj):
    match_player_stats_to_armies(obj)
    result = restructure_dict(obj, BASE_STRUCTURE)
    for player_key, player_data in obj['sides'].items():
        result[player_key] = restructure_dict(player_data, PLAYER_STRUCTURE)
    return result

@click.group()
@log_invocation
@click.option('--format', type=click.Choice(['parquet', 'csv', 'pickle']), default='pickle')
@click.option('--game-ids', multiple=True, type=int)
@click.argument('outfile', type=click.Path(dir_okay=False, writable=True))
@click.pass_context
def export(ctx, format, game_ids, outfile):
    "Dump datastore into a CSV/Parquet file"
    client = get_client()
    game_ids = [str(game_id).encode() for game_id in game_ids]
    if not game_ids:
        game_ids = get_valid_game_ids(client)
    ctx.obj = (client, game_ids)

@export.resultcallback()
def export_callback(retval, format, game_ids, outfile):
    objects, invalid = retval
    if not objects:
        click.secho('(nothing to write)')
        return
    with EchoTimer('Adding %d objects to dataframe (%d invalid/skipped)' % (len(objects), invalid)):
        df = pd.json_normalize(objects).set_index('id')
    with EchoTimer('Writing %dkb dataframe to %s' % (df.memory_usage(index=True).sum()/1024, format)):
        if format == 'csv':
            df.to_csv(outfile)
        elif format == 'parquet':
            df.to_parquet(outfile)
        else:
            df.to_pickle(outfile)

@export.command()
@click.pass_context
def flattened(ctx):
    "Dump everything in the datastore using flattened JSONs (not recommended, messy)"
    client, game_ids = ctx.obj
    with click.progressbar(game_ids, label='Reading datastore') as bar:
        objects = list(yield_deserilized_values(client, bar))
    return objects, invalid

@export.command()
@click.pass_context
def curated(ctx):
    "Dump specific fields from the datastore to a nice CSV/Parquet file (recommended)"
    client, game_ids = ctx.obj
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
