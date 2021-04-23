#!/usr/bin/env python

import logging
import os

import click

from .storage import datastore
from .extractors import extract
from .loader import load
from .exports import export
from .fetching import fetch


@click.group()
@click.option('--loglevel', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], case_sensitive=False), default='WARNING')
def main(loglevel):
    FORMAT = '%07d %%(asctime)-15s %%(levelname)s %%(module)s:%%(lineno)d %%(message)s' % os.getpid()
    logging.basicConfig(format=FORMAT, level=loglevel)

main.add_command(datastore)
main.add_command(load)
main.add_command(extract)
main.add_command(export)
main.add_command(fetch)
