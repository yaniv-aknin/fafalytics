import click
import json

from .datastore import get_client

def load_game_json(handle):
    """Load a bunch of Game models from a file handle.

    This will also inline any relationships under the 'inline' dictionary of each Game."""
    includes = {}
    resolve = lambda obj: includes[obj['type']][obj['id']]
    doc = json.load(handle)
    for include in doc['included']:
        doctype = includes.setdefault(include['type'], {})
        doctype[include['id']] = include
    for game in doc['data']:
        assert game['type'] == 'game'
        game['inlined'] = {}
        for relation_name, related_data in game['relationships'].items():
            assert set(related_data.keys()) == {'data'}
            related_data = related_data['data']
            if isinstance(related_data, list):
                inlined = game['inlined'][relation_name] = []
                for datum in related_data:
                    inlined.append(resolve(datum))
            elif isinstance(related_data, dict):
                game['inlined'][relation_name] = resolve(related_data)
            elif related_data is None:
                pass
            else:
                raise ValueError('%s: unexpected relationship type %s' % (game['id'], type(related_data)))
    return doc['data']

def query_dict(d, query):
    if isinstance(query, tuple):
        query, postprocess = query
    else:
        postprocess = lambda x: x
    components = query.split('/')
    for component in components:
        if component.isdigit():
            component = int(component)
        d = d[component]
    return postprocess(d)

FACTIONS = lambda x: {k:v for k,v in enumerate('uef aeon cybran seraphim nomads unknown'.split())}[x-1]
EXTRACT = {
    "id": "id",
    "end_time": "attributes/endTime",
    "start_time": "attributes/startTime",
    "replay_url": "attributes/replayUrl",
    "victory_condition": "attributes/victoryCondition",
    "map_version_id": "inlined/mapVersion/id",
    "map_height": "inlined/mapVersion/attributes/height",
    "map_width": "inlined/mapVersion/attributes/width",
    "map_id": "inlined/mapVersion/relationships/map/data/id",
    "map_name_heuristic": ("inlined/mapVersion/attributes/folderName", lambda x: x.rpartition('.')[0]),
    "player1_faction": ("inlined/playerStats/0/attributes/faction", FACTIONS),
    "player1_result": "inlined/playerStats/0/attributes/result",
    "player1_start_spot": "inlined/playerStats/0/attributes/startSpot",
    "player1_after_mean": "inlined/playerStats/0/attributes/afterMean",
    "player1_after_deviation": "inlined/playerStats/0/attributes/afterDeviation",
    "player1_before_mean": "inlined/playerStats/0/attributes/beforeMean",
    "player1_before_deviation": "inlined/playerStats/0/attributes/beforeDeviation",
    "player2_faction": ("inlined/playerStats/1/attributes/faction", FACTIONS),
    "player2_result": "inlined/playerStats/1/attributes/result",
    "player2_start_spot": "inlined/playerStats/1/attributes/startSpot",
    "player2_after_mean": "inlined/playerStats/1/attributes/afterMean",
    "player2_after_deviation": "inlined/playerStats/1/attributes/afterDeviation",
    "player2_before_mean": "inlined/playerStats/1/attributes/beforeMean",
    "player2_before_deviation": "inlined/playerStats/1/attributes/beforeDeviation",
}

OUTPUT_CALLBACKS = {'print': print}
def output(func):
    OUTPUT_CALLBACKS[func.__name__] = func
    return func

@output
def datastore(extractor, objects):
    client = get_client()
    for obj in objects:
        jsonified = {k: json.dumps(v) for k, v in obj.items()}
        client.hmset('ex.%s.%s' % (extractor, obj['id']), jsonified)

@output
def console(extractor, objects):
    try:
        import IPython
        IPython.embed()
    except ImportError:
        import code
        code.interact(local=locals())

@click.group()
@click.option('--output', type=click.Choice(tuple(OUTPUT_CALLBACKS)), default='datastore')
@click.pass_context
def extract(ctx, output):
    ctx.obj = {'output_func': OUTPUT_CALLBACKS[output]}

@extract.command()
@click.argument('jsons', nargs=-1, type=click.File('r'))
@click.pass_context
def base(ctx, jsons):
    games = []
    for input_handle in jsons:
        games.extend(load_game_json(input_handle))
    outputs = []
    for game in games:
        assert game['inlined']['featuredMod']['attributes']['technicalName'] == 'ladder1v1'
        output = {}
        for key, query in EXTRACT.items():
            output[key] = query_dict(game, query)
        outputs.append(output)
    ctx.obj['output_func']('base', outputs)
