#!/usr/bin/env python

import click

from .storage import datastore
from .extractors import extract
from .loader import load
from .exports import export
from .fetching import fetch

@click.group()
def main():
    pass

main.add_command(datastore)
main.add_command(load)
main.add_command(extract)
main.add_command(export)
main.add_command(fetch)
