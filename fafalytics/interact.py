import click
import pandas as pd

from .parsing import get_parsed
from .manyfiles import process_all_files
from .pyutils import shell as python_shell

@click.group()
def interactive():
    "Explore datastructures with a Python shell"

@interactive.command()
@click.argument('infiles', nargs=-1, type=click.Path(exists=True, dir_okay=False))
def replay(infiles):
    replays = process_all_files(infiles, get_parsed)
    if not replays:
        click.echo('(nothing loaded)')
    python_shell({'replays': replays})

@interactive.command()
@click.argument('parquet', type=click.Path(exists=True, dir_okay=False))
def dataframe(parquet):
    df = pd.read_parquet(parquet)
    python_shell({'df': df, 'pd': pd})
