import functools
import json

import click

from .storage import get_client
from .pyutils import shell as python_shell

def yields_outputs(func):
    @click.option('--output', type=click.Choice(tuple(OUTPUT_CALLBACKS)), default='datastore')
    @functools.wraps(func)
    def wrapper(output, *args, **kwargs):
        objects = [obj for obj in func(output, *args, **kwargs)]
        OUTPUT_CALLBACKS[output](func.__name__, objects)
    return wrapper


OUTPUT_CALLBACKS = {'print': print}
def output(func):
    OUTPUT_CALLBACKS[func.__name__] = func
    return func

@output
def datastore(prefix, objects):
    client = get_client()
    with click.progressbar(objects, label="Outputting") as bar:
        for obj in bar:
            client.hsetnx(prefix, obj['id'], json.dumps(obj))

@output
def console(prefix, objects):
    python_shell({'objects': objects})
