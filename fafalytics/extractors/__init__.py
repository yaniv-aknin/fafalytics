import click

from ..parsing import get_parsed
from ..output import yields_outputs, OUTPUT_CALLBACKS
from ..manyfiles import file_processor, yield_processed_files
from ..logs import log_invocation

from .apm import APM
from .first import TimeToFirst
from .commandmix import CommandMix

def run_extractors(commands, *extractors):
    for command in commands:
        for extractor in extractors:
            extractor.feed(command)
    result = {}
    for extractor in extractors:
        result.update(extractor.emit())
    return result

def extract_replay(filename):
    replay = get_parsed(filename)
    replay['binary']['last_tick'] = replay['remaining']['last_tick']
    replay['binary'].pop('players')
    replay['binary'].pop('scenario')
    desyncs = replay['remaining']['desync_ticks']
    replay['binary']['desync'] = {'count': len(desyncs),
                                  'ticks': ','.join(str(t) for t in desyncs)}
    extracted = run_extractors(
        replay['commands'],
        TimeToFirst(),
        APM(),
        CommandMix(),
    )
    return {'id': replay['json']['uid'], 'headers': {'json': replay['json'], 'binary': replay['binary']}, 'extracted': extracted}

@click.command()
@log_invocation
@file_processor
@yields_outputs
def extract(ctx, max_errors, infiles):
    "Read replay file and populate the datastore with features extracted from it."
    with click.progressbar(infiles, label='Extracting') as bar:
        yield from yield_processed_files(bar, extract_replay, max_errors)
