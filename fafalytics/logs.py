import time
import click
import logging
import os

from .storage import get_client

LOG_STREAM_KEY = b'log'

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
def echo_log_messages(messages):
    relative_timestamp = time.time()
    fmt = '%(process)07d %(timestamp).2f %(level)s %(module)s:%(lineno)d %(line)s'
    for identifier, obj in messages:
        level, color = decode_level(int(obj[b'levelno']))
        msg = {
            'process': int(obj[b'process']),
            'timestamp': relative_timestamp-float(obj[b'created']),
            'level': level,
            'module': obj[b'module'].decode(),
            'lineno': int(obj[b'lineno']),
            'line': obj[b'line'].decode(),
        }
        printable = fmt % msg
        click.secho(printable, fg=color)

@click.command()
def log():
    "Print the log stored in the datastore"
    response = get_client().xread({LOG_STREAM_KEY: 0})
    if not response:
        return
    stream, messages = response[0]
    assert stream == LOG_STREAM_KEY, stream
    echo_log_messages(messages)
