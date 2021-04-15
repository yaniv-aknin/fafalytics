#!/usr/bin/env python

import click

from .datastore import datastore

@click.group()
def main():
    pass

main.add_command(datastore)
