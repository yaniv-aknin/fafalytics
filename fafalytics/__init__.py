#!/usr/bin/env python

import click

from .datastore import datastore
from .extractors import extract

@click.group()
def main():
    pass

main.add_command(datastore)
main.add_command(extract)
