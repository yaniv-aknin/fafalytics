import click
import redis
import subprocess
from subprocess import PIPE
import os
from os import path
import signal
import time

from .pyutils import block_wait, negate

TMPDIR = '/tmp/.fafalytics.d'
PIDFILE = TMPDIR + '/redis.pid'
SOCKPATH = TMPDIR + '/redis.sock'
REDIS_CONF = """
port 0
unixsocket %s
unixsocketperm 755
""" % SOCKPATH

def get_client():
    client = redis.Redis(unix_socket_path=SOCKPATH)
    client.ping()
    return client

def read_pid() -> int:
    try:
        with open(PIDFILE) as handle:
            pid = handle.read()
    except FileNotFoundError:
        return 0
    return int(pid)

def write_pid(pid: int):
    with open(PIDFILE, 'w') as handle:
        handle.write(str(pid))

def is_running() -> int:
    pid = read_pid()
    if not pid:
        return pid
    if path.exists('/proc/' + str(pid)):
        return pid
    return 0

def is_alive() -> bool:
    try:
        client = get_client()
        return True
    except redis.exceptions.ConnectionError:
        return False

@click.group()
def datastore(): pass

@datastore.command()
def start():
    if not path.exists(TMPDIR):
        os.mkdir(TMPDIR)
    pid = is_running()
    if pid:
        print('already running at pid %d' % pid)
        return
    process = subprocess.Popen(['redis-server', '-'], stdin=PIPE, stdout=subprocess.DEVNULL)
    process.stdin.write(REDIS_CONF.encode())
    process.stdin.close()
    write_pid(process.pid)
    block_wait(10, 0.1, predicate=is_alive)

@datastore.command()
def stop():
    pid = is_running()
    if not pid:
        print('not running')
        return
    os.kill(pid, signal.SIGTERM)
    try:
        block_wait(10, 0.1, predicate=negate(is_running))
    except TimeoutError:
        print('pid %d failed to exit' % pid)

@datastore.command()
@click.pass_context
def restart(ctx):
    if is_running():
        ctx.invoke(stop)
    ctx.invoke(start)
