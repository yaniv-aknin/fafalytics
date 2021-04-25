#!/usr/bin/env python

import logging
import os
import sys

import click

from .storage import datastore
from .extractors import extract
from .loader import load
from .exports import export
from .fetching import fetch
from .pyutils import first
from .logs import DatastoreHandler, handlers, setup, log
from .parsing import unpack
from .interact import interactive


@click.group(context_settings={'auto_envvar_prefix': 'FAFAL'})
@click.option('--loggers', type=click.Choice(tuple(logs.handlers)), multiple=True, default=[first(handlers)])
@click.option('--loglevel', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], case_sensitive=False), default='WARNING')
def main(loggers, loglevel):
    setup(loglevel, *loggers)

main.add_command(datastore)
main.add_command(load)
main.add_command(extract)
main.add_command(export)
main.add_command(fetch)
main.add_command(log)
main.add_command(unpack)
main.add_command(interactive)
