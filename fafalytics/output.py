import functools
import json

from .datastore import get_client

def yields_outputs(func):
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
        client.set(key, json.dumps(current))

@output
def console(objects):
    try:
        import IPython
        IPython.embed()
    except ImportError:
        import code
        code.interact(local=locals())
