import click
import pandas as pd

from .parsing import get_parsed
from .manyfiles import process_all_files
from .pyutils import shell as python_shell
from .pdutils import patch_dataframe_with_query_columns
from .clean import clean_dataframe

@click.group()
def manual():
    "Explore datastructures with a Python shell or perform manual operations"

@manual.command()
@click.argument('infiles', nargs=-1, type=click.Path(exists=True, dir_okay=False))
def replay(infiles):
    replays = process_all_files(infiles, get_parsed)
    if not replays:
        click.echo('(nothing loaded)')
    python_shell({'replays': replays})

@manual.command()
@click.argument('infile', type=click.Path(exists=True, dir_okay=False))
def dataframe(infile):
    patch_dataframe_with_query_columns()
    df = pd.read_pickle(infile)
    python_shell({'df': df, 'pd': pd})

@manual.command()
@click.argument('infile', type=click.Path(exists=True, dir_okay=False))
@click.argument('outfile', type=click.Path(exists=False, dir_okay=False))
def clean(infile, outfile):
    df = pd.read_pickle(infile)
    old_shape = df.shape
    df = clean_dataframe(df)
    df.to_pickle(outfile)
    print('Cleaned df %s to %s' % (old_shape, df.shape))
