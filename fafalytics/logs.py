import time
import functools
import click
import sys
import logging
import os
import contextlib

from .storage import get_client

LOG_STREAM_KEY = b'log'

def log_invocation(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logging.info('fafalytics starting (%d args)', len(sys.argv))
        logging.debug('first 20 args: %s', sys.argv[:20])
        return func(*args, **kwargs)
    return wrapper

class DatastoreHandler(logging.Handler):
    def __init__(self, maxlen=10**6):
        super().__init__()
        self.client = get_client()
        self.maxlen = maxlen
    def emit(self, record):
        line = self.format(record)
        self.client.xadd(LOG_STREAM_KEY, {
            'line': line,
            'module': record.module,
            'funcName': record.funcName,
            'created': record.created,
            'lineno': record.lineno,
            'levelno': record.levelno,
            'process': record.process,
        }, maxlen=self.maxlen)

handlers = {
    'datastore': (DatastoreHandler, '%(message)s'),
    'console': (logging.StreamHandler, '%07d %%(asctime)-15s %%(levelname)s %%(module)s:%%(lineno)d %%(message)s' % os.getpid()),
}

def setup(level, *handler_names):
    root = logging.getLogger()
    root.setLevel(level)
    for handler_name in handler_names:
        try:
            cls, fmt = handlers[handler_name]
            handler = cls()
            handler.setLevel(level)
            handler.setFormatter(logging.Formatter(fmt))
            root.addHandler(handler)
        except Exception as error:
            logging.warning('failed initializing logger %s: %s' % (handler_name, error))

def decode_level(level):
    if level > 40:
        return 'CRIT', 'bright_magenta'
    if level > 30:
        return 'ERRR', 'bright_red'
    if level > 20:
        return 'WARN', 'bright_yellow'
    if level > 10:
        return 'INFO', 'bright_green'
    return 'DEBG', 'bright_white'
def format_log_message(obj, relative_time=None):
    timestamp = float(obj[b'created'])
    if relative_time:
        timestamp = relative_time - timestamp
    level, color = decode_level(int(obj[b'levelno']))
    fmt = '%(process)07d %(timestamp)08.2f %(level)s %(module)s:%(lineno)d %(line)s'
    msg = {
        'process': int(obj[b'process']),
        'timestamp': timestamp,
        'level': level,
        'module': obj[b'module'].decode(),
        'lineno': int(obj[b'lineno']),
        'line': obj[b'line'].decode(),
    }
    return (fmt % msg), color

@click.group()
def log():
    "Datastore based logging"

def get_messages(client, identifier=0, block=None):
    response = get_client().xread({LOG_STREAM_KEY: identifier}, block=block)
    if not response:
        return
    stream, messages = response[0]
    assert stream == LOG_STREAM_KEY, stream
    return messages

@log.command()
def view():
    "Load/print full log stream"
    client = get_client()
    start = time.time()
    messages = get_messages(client)
    if not messages:
        click.echo('(empty)')
        return
    for identifier, obj in messages:
        formatted, color = format_log_message(obj, relative_time=start)
        click.secho(formatted, fg=color)

@log.command()
def tail():
    "Print log lines as they are emitted"
    client = get_client()
    start = time.time()
    identifier = "$"
    with contextlib.suppress(KeyboardInterrupt):
        while True:
            messages = get_messages(client, identifier, block=0)
            for identifier, obj in messages:
                formatted, color = format_log_message(obj, relative_time=start)
                click.secho(formatted, fg=color)

@log.command()
def flush():
    "Discard the log"
    get_client().delete(LOG_STREAM_KEY)
