from os import path
import time
import json
import datetime
import urlobject

import requests
import click

API_BASE = urlobject.URLObject('https://api.faforever.com')

def isoformat(date):
    return datetime.datetime.combine(date, datetime.time()).isoformat(timespec='seconds') + 'Z'

def build_url(api_base, page_size, max_pages, page_number, start_date, duration_weeks):
    url = api_base.add_path('/data/game')
    url = url.add_query_param('page[size]', page_size)
    url = url.add_query_param('page[number]', page_number)
    url = url.add_query_param('page[totals]', '')
    start_formatted = isoformat(start_date)
    end_formatted = isoformat(start_date + duration_weeks)
    filter_param = ('((playerStats.ratingChanges.leaderboard.id=in=("2");endTime=ge="%s";'
                    'endTime=le="%s";validity=in=("VALID")));endTime=isnull=false' % (start_formatted, end_formatted))
    url = url.add_query_param('filter', filter_param)
    url = url.add_query_param('include', 'playerStats,playerStats.player,mapVersion,mapVersion.map')
    url = url.add_query_param('sort', '-startTime')
    return url

def write_response(directory, index, obj):
    with open(path.join(directory, 'dump%04d.json' % index), 'w') as handle:
        json.dump(obj, handle, indent=4)

@click.group()
def fetch(): pass

@fetch.command()
@click.option('--api-base', type=urlobject.URLObject, default=API_BASE)
@click.option('--page-size', type=int, default=10)
@click.option('--max-pages', type=int)
@click.option('--start-date', type=click.DateTime(['%Y-%m-%d']), default='2021-01-01')
@click.option('--duration-weeks', type=lambda x: datetime.timedelta(weeks=int(x)), default=12)
@click.option('--sleep-interval', type=float, default=10)
@click.option('--dry-run/--no-dry-run', default=False)
@click.argument('output_directory', type=click.Path())
def games(api_base, page_size, max_pages, start_date, duration_weeks, sleep_interval, dry_run, output_directory):
    first_url = build_url(api_base, page_size, max_pages, 1, start_date, duration_weeks)
    print('Fetching 1st page...')
    first_response = requests.get(first_url).json()
    write_response(output_directory, 1, first_response)
    total_pages = first_response['meta']['page']['totalPages']
    will_fetch = total_pages if max_pages is None else min(total_pages, max_pages)
    with click.progressbar(range(1, will_fetch+1), label='Fetching %d of %d pages' % (will_fetch, total_pages)) as bar:
        bar.next() # account for first page already fetched
        for page_number in bar:
            if max_pages and page_number > max_pages:
                break
            time.sleep(sleep_interval)
            url = build_url(api_base, page_size, max_pages, page_number, start_date, duration_weeks)
            if dry_run:
                continue
            write_response(output_directory, page_number, requests.get(url).json())

@fetch.command()
def replays():
    raise NotImplementedError()
