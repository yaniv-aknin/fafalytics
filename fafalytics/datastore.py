from contextlib import suppress
from os import path
from subprocess import PIPE
import os
import signal
import subprocess
import time

import click
import redis

from .pyutils import block_wait, negate

TMPDIR = '/tmp/.fafalytics.d'
PIDFILE = TMPDIR + '/redis.pid'
SOCKPATH = TMPDIR + '/redis.sock'
REDIS_CONF = """
port 0
unixsocket %s
unixsocketperm 755
""" % SOCKPATH

class DatastoreError(Exception):
    pass
class NotRunning(DatastoreError):
    pass
class UnexpectedRunning(DatastoreError):
    pass

def get_client():
    client = redis.Redis(unix_socket_path=SOCKPATH)
    client.ping()
    return client

def read_pid() -> int:
    try:
        with open(PIDFILE) as handle:
            return int(handle.read())
    except FileNotFoundError:
        raise NotRunning("unable to read pidfile %s" % PIDFILE)
    except ValueError:
        raise NotRunning("invalid pidfile %s" % PIDFILE)

def write_pid(pid: int) -> None:
    with open(PIDFILE, 'w') as handle:
        handle.write(str(pid))

def is_alive() -> bool:
    try:
        client = get_client()
        return True
    except redis.exceptions.ConnectionError:
        return False

def is_running() -> bool:
    try:
        get_pid()
        return True
    except NotRunning:
        return False

def get_pid() -> int:
    pid = read_pid()
    if path.exists('/proc/' + str(pid)):
        return pid
    raise NotRunning("missing /proc/%d" % pid)

def start_store():
    if not path.exists(TMPDIR):
        os.mkdir(TMPDIR)
    with suppress(NotRunning):
        pid = get_pid()
        raise UnexpectedRunning('already running at pid %d' % pid)
    process = subprocess.Popen(['redis-server', '-'], stdin=PIPE, stdout=subprocess.DEVNULL)
    process.stdin.write(REDIS_CONF.encode())
    process.stdin.close()
    write_pid(process.pid)
    block_wait(10, 0.1, predicate=is_alive, error=NotRunning('pid %d failed ping' % process.pid))

def stop_store():
    pid = get_pid()
    os.kill(pid, signal.SIGTERM)
    block_wait(10, 0.1, error=UnexpectedRunning('pid %d failed to exit' % pid), predicate=negate(is_running))

@click.group()
def datastore():
    pass

@datastore.command()
def start():
    "Starts the datastore and wait for it to ping healthy."
    try:
        start_store()
    except (NotRunning, UnexpectedRunning) as error:
        click.echo("start failed: %s" % error, err=True)

@datastore.command()
def stop():
    "SIGTERM the datastore and wait for it to exit."
    try:
        stop_store()
    except (NotRunning, UnexpectedRunning) as error:
        click.echo('stop failed: %s' % error, err=True)

@datastore.command()
@click.pass_context
def restart(ctx):
    "Stop the datastore if running, then start it (flushing memory)."
    if is_running():
        ctx.invoke(stop)
    ctx.invoke(start)
