import functools
import json

import click

from .storage import get_client

def yields_outputs(func):
    @click.option('--output', type=click.Choice(tuple(OUTPUT_CALLBACKS)), default='datastore')
    @functools.wraps(func)
    def wrapper(output, *args, **kwargs):
        for obj in func(output, *args, **kwargs):
            OUTPUT_CALLBACKS[output](func.__name__, obj)
    return wrapper


OUTPUT_CALLBACKS = {'print': print}
def output(func):
    OUTPUT_CALLBACKS[func.__name__] = func
    return func

@output
def datastore(prefix, obj):
    get_client().hsetnx(prefix, obj['id'], json.dumps(obj))
