import functools
import json

import click

from .datastore import get_client

def yields_outputs(func):
    @click.option('--output', type=click.Choice(tuple(OUTPUT_CALLBACKS)), default='datastore')
    @functools.wraps(func)
    def wrapper(output, *args, **kwargs):
        objects = [obj for obj in func(output, *args, **kwargs)]
        OUTPUT_CALLBACKS[output](objects)
    return wrapper


OUTPUT_CALLBACKS = {'print': print}
def output(func):
    OUTPUT_CALLBACKS[func.__name__] = func
    return func

@output
def datastore(objects):
    client = get_client()
    for obj in objects:
        key = 'game.%s' % obj['id']
        current = json.loads(client.get(key) or '{}')
        current.update(obj)
        client.set(key, json.dumps(current, default=lambda b: b.decode()))

@output
def console(objects):
    try:
        import IPython
        IPython.embed()
    except ImportError:
        import code
        code.interact(local=locals())
