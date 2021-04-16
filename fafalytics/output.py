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
        jsonified = {k: json.dumps(v) for k, v in obj.items()}
        client.hmset('ex.%s' % (obj['id']), jsonified)

@output
def console(objects):
    try:
        import IPython
        IPython.embed()
    except ImportError:
        import code
        code.interact(local=locals())
