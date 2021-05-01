import click
import pandas as pd

from .parsing import get_parsed
from .manyfiles import process_all_files
from .pyutils import shell as python_shell

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
@click.argument('parquet', type=click.Path(exists=True, dir_okay=False))
def dataframe(parquet):
    df = pd.read_parquet(parquet)
    python_shell({'df': df, 'pd': pd})

@manual.command()
@click.argument('infile', type=click.Path(exists=True, dir_okay=False))
@click.argument('outfile', type=click.Path(exists=False, dir_okay=False))
def clean(infile, outfile):
    df = pd.read_parquet(infile)
    old_shape = df.shape
    for prefix in ('player1.player.', 'player2.player.'):
        df[prefix + 'faf_rating.before'] = df[prefix + 'trueskill_mean_before'] - 3 * df[prefix + 'trueskill_deviation_before']
    df['meta.rating_delta'] = (df['player1.player.faf_rating.before']-df['player2.player.faf_rating.before']).abs().astype('int32')
    df['meta.rating_mean'] = ((df['player1.player.faf_rating.before']+df['player2.player.faf_rating.before'])/2).astype('int32')
    df['map.area'] = df['map.width'] * df['map.height']
    df = df[df['durations.ticks']>10*60*3]    # 10*60*3 ticks == 3 minutes
    df = df[df['durations.ticks']<10*60*60*6] # 10*60*60*6 ticks == 6 hours
    df = df[df['player1.player.faf_rating.before']>-250]
    df = df[df['player1.player.faf_rating.before']<3000]
    df = df[df['player2.player.faf_rating.before']>-250]
    df = df[df['player2.player.faf_rating.before']<3000]
    df.to_parquet(outfile)
    print('Cleaned df %s to %s' % (old_shape, df.shape))
