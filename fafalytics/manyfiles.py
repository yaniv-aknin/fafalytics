"Utilities related to processing many files given as CLI arguments."
import logging

import click
import pandas as pd

from .pyutils import Timer

def file_processor(func):
    return (
        click.option('--max-errors', type=int)(
            click.argument('infiles', nargs=-1, type=click.Path(exists=True, dir_okay=False))(func)
        )
    )

def yield_processed_files(infiles, callback, max_errors=None, catch=(Exception,)):
    if max_errors is None:
        max_errors = float('inf')
    durations = []
    for infile in infiles:
        try:
            logging.debug('processing %s', infile)
            with Timer() as timer:
                yield callback(infile)
            durations.append(timer.elapsed)
            logging.debug('processed %s in %.2f seconds', infile, timer.elapsed)
        except catch as error:
            if max_errors == 0:
                raise
            max_errors -= 1
            logging.error('processing %s raised %s:%s', infile, error.__class__.__name__, error)
    stats = dict(pd.Series(durations).describe())
    stats['sum'] = sum(durations)
    logging.info('processed: %s', ','.join('%s=%.1f' % (k,v) for k,v in stats.items()))

def process_all_files(infiles, callback, max_errors=None, catch=(Exception,)):
    return tuple(yield_processed_files(infiles, callback, max_errors, catch))
