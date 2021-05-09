from os import path
import os
import time
import json
import datetime
import urlobject

import requests
import click

from .storage import get_client

API_BASE = urlobject.URLObject('https://api.faforever.com')

def isoformat(date):
    return datetime.datetime.combine(date, datetime.time()).isoformat(timespec='seconds') + 'Z'

def build_url(api_base, page_size, max_pages, page_number, start_date, end_date, sort='ASC'):
    url = api_base.add_path('/data/game')
    url = url.add_query_param('page[size]', page_size)
    url = url.add_query_param('page[number]', page_number)
    url = url.add_query_param('page[totals]', '')
    filter_param = ['((playerStats.ratingChanges.leaderboard.id=in=("2");validity=in=("VALID"))']
    if start_date:
        filter_param.append(';endTime=ge="%s"' % isoformat(start_date))
    if end_date:
        filter_param.append(';endTime=le="%s"' % isoformat(end_date))
    filter_param.append(');endTime=isnull=false')
    url = url.add_query_param('filter', "".join(filter_param))
    url = url.add_query_param('include', 'playerStats,playerStats.player,mapVersion,mapVersion.map')
    url = url.add_query_param('sort', '-startTime' if sort == 'DESC' else 'startTime')
    return url

def write_response(directory, index, obj):
    with open(path.join(directory, 'dump%04d.json' % index), 'w') as handle:
        json.dump(obj, handle, indent=4)

@click.group()
def fetch():
    "Fetching game data from faforever.com"

@fetch.command()
@click.option('--api-base', type=urlobject.URLObject, default=API_BASE)
@click.option('--page-size', type=int, default=10)
@click.option('--max-pages', type=int)
@click.option('--start-date', type=click.DateTime(['%Y-%m-%d']), default=None)
@click.option('--end-date', type=click.DateTime(['%Y-%m-%d']), default=None)
@click.option('--sleep-interval', type=float, default=10)
@click.option('--run-type', type=click.Choice(['dry', 'damp', 'wet']), default='dry',
              help='Dry run prints the first URL and exists. Damp run fetches first URL and exists. Wet run fetches everything.')
@click.option('--sort', type=click.Choice(['ASC', 'DESC']), default='ASC')
@click.argument('output_directory', type=click.Path())
def games(api_base, page_size, max_pages, start_date, end_date, sleep_interval, run_type, sort, output_directory):
    "Get JSON dumps of Game model objects from api.faforever.com"
    first_url = build_url(api_base, page_size, max_pages, 1, start_date, end_date, sort)
    if run_type == 'dry':
        print(first_url)
        return
    print('Fetching 1st page...')
    first_response = requests.get(first_url).json()
    if first_response['meta']['page']['totalRecords'] == 0:
        print('(empty response)')
        return
    write_response(output_directory, 1, first_response)
    total_pages = first_response['meta']['page']['totalPages']
    will_fetch = total_pages if max_pages is None else min(total_pages, max_pages)
    with click.progressbar(range(1, will_fetch+1), label='Fetching %d of %d pages' % (will_fetch, total_pages)) as bar:
        bar.next() # account for first page already fetched
        for page_number in bar:
            if max_pages and page_number > max_pages:
                break
            time.sleep(sleep_interval)
            url = build_url(api_base, page_size, max_pages, page_number, start_date, end_date, sort)
            if run_type == 'damp':
                print(url)
                continue
            write_response(output_directory, page_number, requests.get(url).json())

@fetch.command()
@click.option('--symlink-directory', type=click.Path())
@click.argument('output_directory', type=click.Path())
def replay_urls(symlink_directory, output_directory):
    "Print list of replay URLs known in the datastore but not in output_directory"
    client = get_client()
    for game_id, json_blob in client.hgetall('load').items():
        obj = json.loads(json_blob)
        url = urlobject.URLObject(obj['replayUrl'])
        basename = url.path.segments[-1]
        output_path = path.join(output_directory, basename)
        if not path.exists(output_path):
            print(url)
        if not symlink_directory:
            continue
        symlink_path = path.join(symlink_directory, basename)
        if not path.exists(symlink_path):
            os.symlink(output_path, symlink_path)
