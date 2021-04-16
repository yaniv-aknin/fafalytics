#!/usr/bin/env python

import click

from .datastore import datastore
from .extractors import extract
from .loader import load
from .exports import export

@click.group()
def main():
    pass

main.add_command(datastore)
main.add_command(load)
main.add_command(extract)
main.add_command(export)
