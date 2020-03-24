#!/usr/bin/env python2

import collections
import copy
import datetime
import json
import re

import bs4
import requests
from word2number import w2n

def main():
  timeseries_data = get_recent_timeseries_data()
  timeseries_data = add_historical_timeseries_data(timeseries_data)
  timeseries_data = add_manual_data(timeseries_data)

  # Muck with the data to get it into the format that's expected
  # Fill in the blanks
  dates = sorted(timeseries_data.keys())

  start_time = min([datetime.datetime.strptime(d, '%Y-%m-%d') for d in dates])
  end_time = max([datetime.datetime.strptime(d, '%Y-%m-%d') for d in dates])

  curr_time = start_time
  prev_time = None
  while curr_time <= end_time:
    key = curr_time.strftime('%Y-%m-%d')

    if key not in timeseries_data:
      timeseries_data[key] = timeseries_data[prev_time.strftime('%Y-%m-%d')]
    else:
      # Patch over the gaps...
      if 'recovered' not in timeseries_data[key]:
        timeseries_data[key]['recovered'] = timeseries_data[prev_time.strftime('%Y-%m-%d')]['recovered']
      if 'hospitalized' not in timeseries_data[key]:
        timeseries_data[key]['hospitalized'] = timeseries_data[prev_time.strftime('%Y-%m-%d')]['hospitalized']
      if 'tested' not in timeseries_data[key]:
        timeseries_data[key]['tested'] = timeseries_data[prev_time.strftime('%Y-%m-%d')]['tested']

    prev_time = curr_time
    curr_time = curr_time + datetime.timedelta(days=1)

  dates = sorted(timeseries_data.keys())
  values = [timeseries_data[d] for d in dates]

  # Muck with the age groups and sources data to do the right things
  age_group_data = munge_data_to_output(timeseries_data, dates, 'age_groups')
  source_data = munge_data_to_output(timeseries_data, dates, 'sources')

  formatted_data = {
    'timeseries_dates': dates,
    'total': {
      'confirmed': [timeseries_data[d]['confirmed'] for d in dates],
      'tested': [timeseries_data[d]['tested'] for d in dates],
      'hospitalized': [timeseries_data[d]['hospitalized'] for d in dates],
      'recovered': [timeseries_data[d]['recovered'] for d in dates],
    },
    'age_groups': age_group_data,
    'sources': source_data,
  }

  with open('by_state/vic.json', 'w') as f:
    json.dump(formatted_data, f, indent=2)

def get_recent_timeseries_data():
  timeseries_data = {}

  recent_releases = bs4.BeautifulSoup(requests.get('https://www.dhhs.vic.gov.au/media-hub-coronavirus-disease-covid-19').text, 'html.parser')
  page_body = recent_releases.select_one('div.page-content')

  for li in page_body.select('li'):
    # We only care about media releases that will eventually end up on the Vic Health site
    if 'Department of Health and Human Services media release' not in li.text:
      continue
    # If the href is to another site, it'll be covered by adding historical data, below, and we
    # should skip it
    if li.select_one('a').attrs['href'][0] != '/':
      continue

    href = 'https://www.dhhs.vic.gov.au' + li.select_one('a').attrs['href']
    release = bs4.BeautifulSoup(requests.get(href).text, 'html.parser')
    layout_region = release.select_one('div.layout__region')

    # The date is on the second line of this div
    date_text = layout_region.select_one('div.first-line').text.strip().split('\n')[1]
    # And sometimes it has the day first, sometimes not
    date_text = date_text.split(',')[-1].strip()
    date = datetime.datetime.strptime(date_text, '%d %B %Y')
    body = layout_region.select_one('div.page-content').text.strip()

    total_cases, community_contact, hospital, tested, recovered = parse_fulltext_post(body)

    if total_cases is not None:
      timeseries_data[date.strftime('%Y-%m-%d')] = {
        'tested': tested,
        'confirmed': total_cases,
        'hospitalized': hospital,
        'sources': {
          'Locally acquired - contact not identified': community_contact,
        }
      }

      if recovered:
        timeseries_data[date.strftime('%Y-%m-%d')]['recovered'] = recovered
    else:
      import code
      code.interact(local=locals())
      raise 'Trouble parsing!', date

  return timeseries_data

def add_historical_timeseries_data(timeseries_data):
  historical_releases = bs4.BeautifulSoup(
    requests.get('https://www2.health.vic.gov.au/about/media-centre/mediareleases/?ps=10000&s=relevance&pn=1').text,
    'html.parser'
  )

  release_list = historical_releases.select_one('ol.listing')
  for li in release_list.select('li'):
    href = li.select_one('a').attrs['href']
    if href[0] == '/':
      href = 'https://www2.health.vic.gov.au' + href

    title = li.select_one('h3').text

    # We do not care about "Poisonous Mushrooms sprouting early" this time
    if 'COVID-19' not in title and 'coronavirus' not in title.lower():
      continue

    release = bs4.BeautifulSoup(requests.get(href).text, 'html.parser')
    date = datetime.datetime.strptime(release.select_one('div.page-date').text, '%d %b %Y')
    body = release.select_one('div#main').text.strip()

    total_cases, community_contact, hospital, tested, recovered = parse_fulltext_post(body)
    if total_cases is not None:
      timeseries_data[date.strftime('%Y-%m-%d')] = {
        'tested': tested,
        'confirmed': total_cases,
        'hospitalized': hospital,
        'sources': {
          'Locally acquired - contact not identified': community_contact,
        }
      }

      if recovered:
        timeseries_data[date.strftime('%Y-%m-%d')]['recovered'] = recovered

    # Releases from before this date aren't easily machine-parseable
    elif date <= datetime.datetime(year=2020, month=3, day=15):
      continue
    else:
      raise 'Unparseable post!', href

  return timeseries_data

def parse_fulltext_post(body):
  m = re.match(r'.*bringing the total number of cases in Victoria to (?P<total_cases>\d+)\..*At (the )?present( time)?, there are (?P<community_contact>\w+) confirmed cases of COVID-19 in Victoria that may have been acquired through community transmission\..*Currently (?P<hospital>\w+) people are (recovering )?in hospital( .*(?P<recovered>\d+) people have recovered)?.*More than (?P<tested>[\d,]+) Victorians have been tested to date.*', body, re.MULTILINE | re.DOTALL)
  if m:
    total_cases = parse_num(m.group('total_cases'))
    community_contact = parse_num(m.group('community_contact'))
    hospital = parse_num(m.group('hospital'))
    tested = parse_num(m.group('tested'))
    if m.group('recovered'):
      recovered = parse_num(m.group('recovered'))
    else:
      recovered = None

    if community_contact is None:
      print 'wtf?', m.group('community_contact')

    return (total_cases, community_contact, hospital, tested, recovered)
  else:
    return (None, None, None, None, None)


def parse_num(num):
  if re.match(r'^[\d,]+$', num):
    return int(num.replace(',', ''))
  else:
    return w2n.word_to_num(num)

def add_manual_data(timeseries_data):
  events = {
    # https://www2.health.vic.gov.au/about/media-centre/MediaReleases/first-novel-coronavirus-case-in-victoria
    '2020-01-25': {
      'tested': 1,
      'confirmed': 1,
      'hospitalized': 1,
      'sources': {
        'Overseas acquired': 1, # Wuhan
      },
      'age_groups': {
        '50-59': 1,
      }
    },
    # https://www2.health.vic.gov.au/about/media-centre/MediaReleases/second-novel-coronavirus-case-victoria
    '2020-01-29': {
      'tested': 1,
      'confirmed': 1,
      # Not hospitalized
      'sources': {
        'Overseas acquired': 1, # Wuhan
      },
      'age_groups': {
        '60-69': 1,
      }
    },
    # https://www2.health.vic.gov.au/about/media-centre/MediaReleases/third-novel-coronavirus-case-victoria
    '2020-01-30': {
      'tested': 69,
      'confirmed': 1,
      'hospitalized': 1,
      'sources': {
        'Overseas acquired': 1, # Hubei
      },
      'age_groups': {
        '40-49': 1,
      }
    },
    # https://www2.health.vic.gov.au/about/media-centre/MediaReleases/fourth-novel-coronavirus-case-victoria
    '2020-02-01': {
      'tested': 149 - 71,
      'confirmed': 1,
      # Not hospitalized
      'sources': {
        'Overseas acquired': 1, # Wuhan
      },
      'age_groups': {
        '20-29': 1,
      }
    },
    # https://www2.health.vic.gov.au/about/media-centre/MediaReleases/ninth-covid-19-case-victoria
    # Yay, data gaps -_-
    '2020-03-02': {
      'confirmed': 5,
      'recovered': 7,
      'hospitalized': 1, # One at home, one in hospital, three mysteries
      'sources': {
        'Overseas acquired': 5, # Iran, Diamond Princess, ??, ??, ??
      },
      'age_groups': {
        '30-39': 1,
        '70-79': 1,
      }
    },
    # https://www2.health.vic.gov.au/about/media-centre/MediaReleases/tenth-covid-19-case-victoria
    '2020-03-04': {
      'confirmed': 1,
      # Not hospitalized
      'sources': {
        'Overseas acquired': 1, # Iran
      },
      'age_groups': {
        '30-39': 1,
      }
    },
    # https://www2.health.vic.gov.au/about/media-centre/MediaReleases/eleventh-case-coronavirus-victoria
    '2020-03-07': {
      'confirmed': 1,
      # Not hospitalized
      'sources': {
        'Overseas acquired': 1, # US
      },
      'age_groups': {
        '70-79': 1,
      }
    },
    # https://www2.health.vic.gov.au/about/media-centre/MediaReleases/new-case-covid-19-victoria
    '2020-03-08': {
      'confirmed': 1,
      # Not hospitalized
      'sources': {
        'Overseas acquired': 1, # Indonesia
      },
      'age_groups': {
        '50-59': 1,
      }
    },
    # https://www2.health.vic.gov.au/about/media-centre/MediaReleases/three-new-cases-covid-19-in-vic-10-march-2020
    '2020-03-10': {
      'confirmed': 3,
      # None hospitalized
      'sources': {
        'Overseas acquired': 2, # Israel, Jordon, Egypt, Singapore; US
        'Locally acquired - contact of a confirmed case': 1,
      },
      'age_groups': {
        '50-59': 1,
        '70-79': 2,
      }
    },
    # https://www2.health.vic.gov.au/about/media-centre/MediaReleases/three-more-cases-covid-19-victoria
    '2020-03-11': {
      'confirmed': 3,
      # None hospitalized
      'sources': {
        'Overseas acquired': 3, # US; US; US
      },
      'age_groups': {
        '20-29': 1,
        '50-59': 2,
      }
    },
    # https://www2.health.vic.gov.au/about/media-centre/MediaReleases/more-covid-19-cases-confirmed-in-victoria
    '2020-03-12': {
      'confirmed': 6,
      # None hospitalized
      'sources': {
        'Overseas acquired': 5, # Not listed where they flew from
        'Locally acquired - contact of a confirmed case': 1,
      },
      # Here ends our detailed age data :(
    },
    # https://www2.health.vic.gov.au/about/media-centre/MediaReleases/more-covid-19-cases-confirmed-in-victoria-13-march-2020
    '2020-03-13': {
      'confirmed': 9,
      # None hospitalized
      'sources': {
        'Overseas acquired': 7, # Not listed where they flew from
        'Locally acquired - contact of a confirmed case': 1,
        'Locally acquired - contact not identified': 1,
      },
    },
    # https://www2.health.vic.gov.au/about/media-centre/MediaReleases/more-covid-19-cases-confirmed-in-victoria-14-march-2020
    '2020-03-14': {
      'confirmed': 13,
      'hospitalized': 1,
      # One person hospitalized, others in home isolation
      # No detailed source data :(
    },
    # https://www2.health.vic.gov.au/about/media-centre/MediaReleases/more-covid19-cases-confirmed-victoria-15-march
    '2020-03-15': {
      'confirmed': 8,
      # None hospitalized
    },
  }

  confirmed = 0
  hospitalized = 0
  recovered = 0
  tested = 0
  age_groups = collections.defaultdict(lambda: 0)
  sources = collections.defaultdict(lambda: 0)
  for date in sorted(events.keys()):
    event_data = events[date]
    confirmed = event_data.get('confirmed', confirmed)
    hospitalized = event_data.get('hospitalized', hospitalized)
    recovered = event_data.get('recovered', recovered)
    tested = event_data.get('tested', tested)

    for k, v in event_data.get('age_groups', {}).iteritems():
      age_groups[k] += v
    for k, v in event_data.get('sources', {}).iteritems():
      sources[k] += v

    # If there's already data for this date, not sure what happened - just
    # override with this info
    if date not in timeseries_data:
      timeseries_data[date] = {
        'confirmed': confirmed,
        'hospitalized': hospitalized,
        'recovered': recovered,
        'tested': tested,
        'age_groups': copy.deepcopy(age_groups),
        'sources': copy.deepcopy(sources),
      }
    else:
      raise 'Date already existed?', date

  return timeseries_data

def munge_data_to_output(timeseries_data, dates, data_key):
  dates = sorted(timeseries_data.keys())
  values = [timeseries_data[d] for d in dates]

  # Generate a list of all keys for the given data series
  # There's probably a way to do this with a Python one liner, but I think this
  # is clearer
  keyset = set()
  for v in values:
    for k in v.get(data_key, {}).keys():
      keyset.add(k)
  keys = sorted(keyset)

  munged_data = {}
  for k in keys:
    munged_data[k] = []
    for d in dates:
      munged_data[k].append(timeseries_data[d].get(data_key, {}).get(k, 0))

  return {
    'keys': keys,
    'subseries': munged_data,
  }

if __name__ == '__main__':
  main()

