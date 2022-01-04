#!/usr/bin/env python3

import collections
import copy
import datetime
import json
import re
import os

import bs4
import requests
from word2number import w2n

# Whether or not you should cache the requests of a Power BI request.
# This is strongly recommended if developing locally, or the server will rate
# limit you.
CACHE_POWERBI = False

def main():
  timeseries_data = get_timeseries_data_from_power_bi()

  # Fill in the test, hospitalization, icu, and recovery data from the media
  # releases - these are harder to generate from the PowerBI point in time
  # patient snapshot (though we should be able to get this, starting now!)
  timeseries_data = add_recent_data(timeseries_data)
  timeseries_data = add_historical_data(timeseries_data)
  timeseries_data = add_manual_data(timeseries_data)
  timeseries_data = fill_in_blank_data(timeseries_data)

  dates = sorted(timeseries_data.keys())

  # Muck with the age groups and sources data to do the right things
  age_group_data = munge_data_to_output(timeseries_data, dates, 'age_groups')
  source_data = munge_data_to_output(timeseries_data, dates, 'sources')

  formatted_data = {
    'timeseries_dates': dates,
    'total': {
      'tested': [timeseries_data[d]['tested'] for d in dates],
      'confirmed': trim_trailing_nones([timeseries_data[d].get('confirmed', None) for d in dates]),
      'current_icu': [timeseries_data[d]['icu'] for d in dates],
      'current_hospitalized': [timeseries_data[d]['hospitalized'] for d in dates],
      'deaths': [timeseries_data[d]['deaths'] for d in dates],
      'recovered': [timeseries_data[d]['recovered'] for d in dates],
    },
    'age_groups': age_group_data,
    'sources': source_data,
  }

  with open('by_state_partial/vic.json', 'w') as f:
    json.dump(formatted_data, f, indent=2, sort_keys=True)

# If we have a timeseries that ends with None entries, we should remove them, leaving the last known as the last element
def trim_trailing_nones(ts):
  while len(ts) > 0 and ts[-1] is None:
    ts.pop()
  return ts

# Fetch the current case status from PowerBI, Vic Health's live dashboard
def get_timeseries_data_from_power_bi():
  if CACHE_POWERBI:
    data = json.loads(cache_request('data_cache/vic/powerbi.json', powerbi_request))
  else:
    data = json.loads(powerbi_request())

    # If we got back no data, we're likely being 401'd (rate limited) - fall
    # back to the cache file
    if data == '':
      print('Empty response received - falling back to cache')
      data = json.loads(cache_request('data_cache/vic/powerbi.json', powerbi_request, force_cache=True))

    # Otherwise, if we successfully pulled data, update the day's cache file
    else:
      # This will be wrong for the next week, and it's not the right way to
      # handle timezones, but it's not going to be off by enough to actually
      # matter
      day = datetime.datetime.now() + datetime.timedelta(hours=10)
      with open('data_cache/vic/%s_powerbi.snapshot.json' % day.strftime('%Y-%m-%d'), 'w') as f:
        f.write(json.dumps(data))

  cases = uncompress_powerbi_response(data)

  timeseries_data = collections.defaultdict(lambda: {
    'age_groups': collections.defaultdict(lambda: 0),
    'sources': collections.defaultdict(lambda: 0),
  })

  for c in cases:
    age_group, _gender, _case_num, _clinician_status, acquired, _acquired_country, event_date, _clinician_status_n, _acquired_n, _acquired_country_n, _count, _lga = c

    if event_date < datetime.datetime(year=2018, month=6, day=1):
      continue # data entry is even harder

    if event_date < datetime.datetime(year=2019, month=6, day=1):
      # lol data entry is hard
      event_date = datetime.datetime(year=event_date.year + 1, month=event_date.month, day=event_date.day, hour=event_date.hour, minute=event_date.minute)
    event_date_key = event_date.strftime('%Y-%m-%d')

    # Normalize age groups into bunches of 10, as NSW does
    if 'confirmed' not in timeseries_data[event_date_key]:
      timeseries_data[event_date_key]['confirmed'] = 0
    timeseries_data[event_date_key]['confirmed'] += 1

    age_group = normalize_age_group(age_group)
    if age_group is not None:
      timeseries_data[event_date_key]['age_groups'][age_group] += 1
    source = normalize_source(acquired)
    if source is not None:
      timeseries_data[event_date_key]['sources'][source] += 1

    # We can use clinician status over time to determine hospitalized,
    # recovered, icu, and deaths. For now though, let's keep using media
    # briefings for that

  # All our data so far has been "per-day", but we actually need cumulative, so
  # convert to that
  start_time = min([datetime.datetime.strptime(d, '%Y-%m-%d') for d in timeseries_data.keys()])
  end_time = max([datetime.datetime.strptime(d, '%Y-%m-%d') for d in timeseries_data.keys()])
  
  curr_time = start_time + datetime.timedelta(days=1)
  prev_time = start_time
  while curr_time <= end_time:
    curr_key = curr_time.strftime('%Y-%m-%d')
    prev_key = prev_time.strftime('%Y-%m-%d')

    if 'confirmed' not in timeseries_data[curr_key]:
      timeseries_data[curr_key]['confirmed'] = 0
    timeseries_data[curr_key]['confirmed'] += timeseries_data[prev_key].get('confirmed', 0)

    for k in set(timeseries_data[curr_key]['age_groups'].keys()) | set(timeseries_data[prev_key]['age_groups'].keys()):
      timeseries_data[curr_key]['age_groups'][k] += timeseries_data[prev_key]['age_groups'][k]

    for s in set(timeseries_data[curr_key]['sources'].keys()) | set(timeseries_data[prev_key]['sources'].keys()):
      timeseries_data[curr_key]['sources'][s] += timeseries_data[prev_key]['sources'][s]

    prev_time = curr_time
    curr_time += datetime.timedelta(days=1)
  
  return timeseries_data

# Uncompresses the PowerBI response back to a normal set of Python tuples
def uncompress_powerbi_response(data):
  value_lookup = data['results'][0]['result']['data']['dsr']['DS'][0]['ValueDicts']
  header = data['results'][0]['result']['data']['dsr']['DS'][0]['PH'][0]['DM0'][0]
  # There's an implicit None here from the group by, not sure how this is meant to be flagged
  if len(header['C']) != 12:
    header['C'] = [None] + header['C']

  results = []
  for case in data['results'][0]['result']['data']['dsr']['DS'][0]['PH'][0]['DM0']:
    # not sure what this flags, but it indicates something we don't currently handle so skip it.
    if u'\xd8' in case:
      print('FIXME: Skipping over field with special key we don\'t understand')
      continue

    case_num = None
    event_date = None

    ci = 0

    row = []
    for i in range(0, 12):
      mapping = header['S'][i]
      dn = mapping.get('DN', None)

      if 'R' not in case or (case['R'] >> i) & 1 == 0:
        value = case['C'][ci]

        if dn is not None and isinstance(value, int):
          value = value_lookup[dn][value]

        row.append(value)
        ci += 1
      else:
        if len(results) > 0:
          row.append(results[-1][i])
        else:
          row.append(None)

    if row[0] is not None:
      row[0] = row[0].replace(u'\u2013', '-')

    if row[6] is not None and isinstance(row[6], int):
      row[6] = datetime.datetime.fromtimestamp(row[6]/1000.0)
    results.append(tuple(row))

  return results

# Normalized the age groups to buckets of 10 years, as NSW does
def normalize_age_group(age_group):
  if age_group is None or age_group == 'Unknown':
    return None
  elif age_group == '80-84' or age_group == '85+':
    return '80+'
  else:
    age_start = int(age_group.split('-')[0])
    if age_start % 10 != 0:
      age_start -= 5
    return '%d-%d' % (age_start, age_start + 9)

def normalize_source(source):
  return {
    'Contact with a confirmed case': 'Locally acquired - contact of a confirmed case',
    'Acquired in Australia, unknown source': 'Locally acquired - contact not identified',
    'Travel overseas': 'Overseas acquired',
    'Under investigation': 'Under investigation',
  }.get(source, None)

def add_recent_data(timeseries_data):
  timeseries_data = add_health_news_feed(timeseries_data)

  recent_releases = bs4.BeautifulSoup(requests.get('https://www.dhhs.vic.gov.au/media-hub-coronavirus-disease-covid-19').text, 'html.parser')
  page_body = recent_releases.select_one('div.page-content')

  for li in page_body.select('li'):
    # We only care about media releases that will eventually end up on the Vic Health site
    if 'Department of Health and Human Services media release' not in li.text:
      continue
    if 'repatriation flights' in li.text:
      continue
    # If the href is to another site, it'll be covered by adding historical data, below, and we
    # should skip it
    uri = li.select_one('a').attrs['href'].replace('https://www.dhhs.vic.gov.au/', '/')
    if uri[0] != '/':
      continue

    # this almost looks like a real one, but isn't
    if 'cho-victoria-2-april-2020' in uri: continue
    
    timeseries_data = add_dhhs_release(timeseries_data, uri)
  
  timeseries_data = add_dhhs_release(timeseries_data, '/coronavirus-update-victoria-24-april-2020')
  timeseries_data = add_dhhs_release(timeseries_data, '/coronavirus-update-victoria-21-may-2020')

  return timeseries_data

def add_health_news_feed(timeseries_data):
  data = '{"from":0,"size":200,"_source":[],"query":{"bool":{"filter":[{"terms":{"field_node_site":["4"]}},{"terms":{"type":["news"]}},{"terms":{"field_topic":["Media Releases"]}}],"must_not":[{"match":{"nid":11743}}]}},"sort":[{"field_news_date":"desc"}],"aggs":{"field_node_year":{"terms":{"field":"field_node_year","order":{"_key":"desc"},"size":30}}}}'
  new_items = requests.post('https://www.health.vic.gov.au/search-api/v2/dsl', data=data, headers={
    'Content-Type': 'application/json',
  })

  for item in new_items.json()['hits']['hits']:
    date = item['_source']['field_news_date'][0].split('T')[0]
    url = item['_source']['url'][0]
    body = item['_source']['body'][0]
    print(date, url, body[:100])
    if 'coronavirus' not in url: continue
    timeseries_data = add_health_with_date_body(timeseries_data, date, body)
  
  return timeseries_data

def add_health_with_date_body(timeseries_data, date, body):
  confirmed, tested, deaths, recovered, hospitalized, icu = parse_fulltext_post(body)

  date_key = date
  date_keys = [date_key]
  if date_key == '2020-04-15':
    date_keys.append('2020-04-16') # they missed this date, copy it.

  for date_key in date_keys:
    print('{}: confirmed={}, tested={}, deaths={}, recovered={}, hospitalized={}, icu={}'.format(date_key, confirmed, tested, deaths, recovered, hospitalized, icu))
    
    # We should always be able to get the number of people confirmed and tested
    if (tested is not None or date_key in ['2020-06-06', '2020-06-07', '2020-08-02']) and confirmed is not None:
      timeseries_data[date_key]['tested'] = tested
      # We overwrite the summed individual cases here, if we have an official
      # media release
      timeseries_data[date_key]['confirmed'] = confirmed
    else:
      print('WARNING: Trouble parsing! (confirmed={}, tested={}, deaths={}, recovered={}, hospitalized={}, icu={})'.format(confirmed, tested, deaths, recovered, hospitalized, icu))
      print(repr(body))
      return timeseries_data

    if deaths is not None:
      timeseries_data[date_key]['deaths'] = deaths
    if recovered is not None:
      timeseries_data[date_key]['recovered'] = recovered
    if hospitalized is not None:
      timeseries_data[date_key]['hospitalized'] = hospitalized
    if icu is not None:
      timeseries_data[date_key]['icu'] = icu

  return timeseries_data


def add_dhhs_release(timeseries_data, uri):
  href = 'https://www.dhhs.vic.gov.au' + uri
  response_body = cache_request(
    'data_cache/vic/%s.html' % uri.replace('/', '_'),
    lambda: requests.get(href).text
  )

  print('Processing: {}'.format(uri))

  release = bs4.BeautifulSoup(response_body, 'html.parser')
  layout_region = release.select_one('div.layout__region')

  # The date is on the second line of this div
  date_text = None
  try:
    first_line = layout_region.select_one('div.first-line')
    if first_line:
      date_text = first_line.text.strip().split('\n')[1]
  except IndexError:
    pass # this is fine, we just try again with the h1
  if date_text is None:
    date_text = release.select_one('h1').text.strip().split(' - ')[-1]
  if date_text is None:
    print('WARNING: {} was not parseable, please check if it is intended to be a parseable release'.format(href))
    return timeseries_data
  # And sometimes it has the day first, sometimes not
  date_text = date_text.split(',')[-1].strip()
  if date_text.count(' ') < 2 or ' 202' not in date_text:
    date_text = date_text + ' 2020'
  try:
    date = datetime.datetime.strptime(date_text, '%d %B %Y')
  except ValueError:
    date = datetime.datetime.strptime(date_text, '%A %d %B %Y')
  body = layout_region.select_one('div.page-content').text.strip()

  confirmed, tested, deaths, recovered, hospitalized, icu = parse_fulltext_post(body)

  meta = {}
  for table in layout_region.select('table'):
    headers = list(th.text.strip() for th in table.select('th'))
    values = list(td.text.strip() for td in table.select('td'))
    meta.update(dict(zip(headers, values)))

  if tested is None and 'Total tests since pandemic began' in meta:
    tested = parse_num(meta['Total tests since pandemic began'])
  
  if confirmed is None:
    values = []
    for k, v in meta.items():
      if k.startswith('Cases acquired'):
        values.append(parse_num(v))
    if len(values) > 0:
      confirmed = sum(values)

  if deaths is None and 'Lives lost' in meta:
    deaths = parse_num(meta['Lives lost'])

  date_key = date.strftime('%Y-%m-%d')
  date_keys = [date_key]
  if date_key == '2020-04-15':
    date_keys.append('2020-04-16') # they missed this date, copy it.

  for date_key in date_keys:
    print('{}: confirmed={}, tested={}, deaths={}, recovered={}, hospitalized={}, icu={}'.format(date_key, confirmed, tested, deaths, recovered, hospitalized, icu))
    
    # We should always be able to get the number of people confirmed and tested
    if (tested is not None or date_key in ['2020-06-06', '2020-06-07', '2020-08-02']) and confirmed is not None:
      timeseries_data[date_key]['tested'] = tested
      # We overwrite the summed individual cases here, if we have an official
      # media release
      timeseries_data[date_key]['confirmed'] = confirmed
    else:
      print('WARNING: Trouble parsing! {} (confirmed={}, tested={}, deaths={}, recovered={}, hospitalized={}, icu={})'.format(uri, confirmed, tested, deaths, recovered, hospitalized, icu))
      return timeseries_data

    if deaths is not None:
      timeseries_data[date_key]['deaths'] = deaths
    if recovered is not None:
      timeseries_data[date_key]['recovered'] = recovered
    if hospitalized is not None:
      timeseries_data[date_key]['hospitalized'] = hospitalized
    if icu is not None:
      timeseries_data[date_key]['icu'] = icu

  return timeseries_data

def add_historical_data(timeseries_data):
  for basename in os.listdir('data_cache/vic/historical'):
    response_body = cache_request(
      'data_cache/vic/historical/' + basename,
      lambda: False,
      force_cache=True,
    )

    release = bs4.BeautifulSoup(response_body, 'html.parser')
    date = datetime.datetime.strptime(release.select_one('div.page-date').text, '%d %b %Y')
    body = release.select_one('div#main').text.strip()

    confirmed, tested, deaths, recovered, hospitalized, icu = parse_fulltext_post(body)

    # We should always be able to get the number of people tested and confirmed
    if tested is not None and confirmed is not None:
      timeseries_data[date.strftime('%Y-%m-%d')]['tested'] = tested
      timeseries_data[date.strftime('%Y-%m-%d')]['confirmed'] = confirmed
    # Releases from before this date aren't easily machine-parseable, but we
    # know that
    elif date <= datetime.datetime(year=2020, month=3, day=15):
      continue
    else:
      raise Exception('Trouble parsing! %s' % date)

    if deaths is not None:
      timeseries_data[date.strftime('%Y-%m-%d')]['deaths'] = deaths
    if recovered is not None:
      timeseries_data[date.strftime('%Y-%m-%d')]['recovered'] = recovered
    if hospitalized is not None:
      timeseries_data[date.strftime('%Y-%m-%d')]['hospitalized'] = hospitalized
    if icu is not None:
      timeseries_data[date.strftime('%Y-%m-%d')]['icu'] = icu

  return timeseries_data

def fill_in_blank_data(timeseries_data):
  start_time = min([datetime.datetime.strptime(d, '%Y-%m-%d') for d in timeseries_data.keys()])
  end_time = max([datetime.datetime.strptime(d, '%Y-%m-%d') for d in timeseries_data.keys()])

  curr_time = start_time + datetime.timedelta(days=1)
  prev_time = start_time
  while curr_time <= end_time:
    curr_key = curr_time.strftime('%Y-%m-%d')
    prev_key = prev_time.strftime('%Y-%m-%d')

    for k in ['tested', 'deaths', 'recovered', 'hospitalized', 'icu']:
      if k not in timeseries_data[curr_key]:
        timeseries_data[curr_key][k] = timeseries_data[prev_key][k]

    prev_time = curr_time
    curr_time = curr_time + datetime.timedelta(days=1)

  return timeseries_data

def match_first(body, patterns):
  for p in patterns:
    m = re.match(p, body, re.MULTILINE | re.DOTALL)
    if m:
      return m

def parse_fulltext_post(body):
  body = body.replace(u'\xa0', ' ')
  
  confirmed = None
  m = re.match(r'.*confirmed cases in Victoria since the beginning of the pandemic is (?P<confirmed>[\d,]+).*', body, re.MULTILINE | re.DOTALL)
  if not m:
    m = re.match(r'.*total number of [a-zA-Z ]*cases[a-zA-Z ]* (is|to|at) (?P<confirmed>[\d,]+).*', body, re.MULTILINE | re.DOTALL)
  if not m:
    m = re.match(r'.*Of the total (?P<confirmed>[\d,]+) cases.*', body, re.MULTILINE | re.DOTALL)
  if m:
    confirmed = parse_num(m.group('confirmed'))

  tested = None
  m = match_first(body, [
    r'.*Total tests since pandemic began (?P<tested>[\d,]+).*',
    r'.*The total number of tests performed in Victoria since the pandemic began is (?P<tested>[\d,]+).*',
    r'.* (?P<tested>[\d,]+) (Victorians have been tested to date|(swabs|tests|test results) have been (conducted|processed|completed|undertaken|taken|received)).*',
  ])
  if m:
    tested = parse_num(m.group('tested'))

  deaths = None
  m = re.match(r'This brings the total number of deaths in Victoria since the pandemic began to (?P<deaths>\w+)', body, re.MULTILINE | re.DOTALL)
  if not m:
    m = re.match(r'.*Victoria has(?: now)? recorded(?: its first)? (?P<deaths>\w+) deaths related to (?:coronavirus|COVID-19).*', body, re.MULTILINE | re.DOTALL)
  if m:
    deaths = parse_num(m.group('deaths'))
  else:
    m = re.match(r'.*To date, (?P<deaths>\w+) people have died from coronavirus in Victoria.*', body, re.MULTILINE | re.DOTALL)
    if m:
      deaths = parse_num(m.group('deaths'))
    else:
      m = re.match(r'.*taking the number of people who have died in Victoria from coronavirus to (?P<deaths>\w+).*', body, re.MULTILINE | re.DOTALL)
      if m:
        deaths = parse_num(m.group('deaths'))

  recovered = None
  m = re.match(r'.* (?P<recovered>[\d,]+) people have recovered.*', body, re.MULTILINE | re.DOTALL)
  if m:
    recovered = parse_num(m.group('recovered'))

  # Hospitalizations
  hospital = None
  m = re.match(r'.*Currently (?P<hospital>\w+) people are (recovering )?in hospital.*', body, re.MULTILINE | re.DOTALL)
  if m:
    hospital = parse_num(m.group('hospital'))

  # ICU
  icu = None
  m = re.match(r'.*including (?P<icu>\w+) patients in intensive care.*', body, re.MULTILINE | re.DOTALL)
  if m:
    icu = parse_num(m.group('icu'))
  
  return (confirmed, tested, deaths, recovered, hospital, icu)

def parse_num(num):
  overrides = {
    'eleven': 11,
    'twelve': 12,
    'thirteen': 13,
    'fourteen': 14,
    'fifteen': 15,
    'sixteen': 16,
    'seventeen': 17,
    'eighteen': 18,
    'nineteen': 19,
  }
  if re.match(r'^[\d,]+$', num):
    return int(num.replace(',', ''))
  elif num in overrides:
    return overrides[num]
  else:
    return w2n.word_to_num(num)

def add_manual_data(timeseries_data):
  events = {
    # Case data starts here, which causes weirdness if we don't have a tested
    # figure
    '2020-01-24': {
      'tested': 0,
      'deaths': 0,
      'recovered': 0,
      'hospitalized': 0,
      'icu': 0,
    },
    # https://www2.health.vic.gov.au/about/media-centre/MediaReleases/first-novel-coronavirus-case-in-victoria
    '2020-01-25': {
      'tested': 1,
    },
    # https://www2.health.vic.gov.au/about/media-centre/MediaReleases/second-novel-coronavirus-case-victoria
    '2020-01-29': {
      'tested': 1,
    },
    # https://www2.health.vic.gov.au/about/media-centre/MediaReleases/third-novel-coronavirus-case-victoria
    '2020-01-30': {
      'tested': 69
    },
    # https://www2.health.vic.gov.au/about/media-centre/MediaReleases/fourth-novel-coronavirus-case-victoria
    '2020-02-01': {
      'tested': 149 - 71,
    },
  }

  for e in events:
    timeseries_data[e]['tested'] = events[e]['tested']
    if 'deaths' in events[e]:
      timeseries_data[e]['deaths'] = events[e]['deaths']
    if 'recovered' in events[e]:
      timeseries_data[e]['recovered'] = events[e]['recovered']
    if 'hospitalized' in events[e]:
      timeseries_data[e]['hospitalized'] = events[e]['hospitalized']
    if 'icu' in events[e]:
      timeseries_data[e]['icu'] = events[e]['icu']

  return timeseries_data

def powerbi_request():
  q = '{"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"d","Entity":"dimAgeGroup"},{"Name":"l","Entity":"Linelist"}],"Select":[{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"AgeGroup"},"Name":"dimAgeGroup.AgeGroup"},{"Column":{"Expression":{"SourceRef":{"Source":"l"}},"Property":"Sex"},"Name":"Linelist.Sex"},{"Column":{"Expression":{"SourceRef":{"Source":"l"}},"Property":"PHESSID"},"Name":"CountNonNull(Linelist.PHESSID)"},{"Column":{"Expression":{"SourceRef":{"Source":"l"}},"Property":"clin_status"},"Name":"Linelist.clin_status"},{"Column":{"Expression":{"SourceRef":{"Source":"l"}},"Property":"acquired"},"Name":"Linelist.acquired"},{"Column":{"Expression":{"SourceRef":{"Source":"l"}},"Property":"acquired_country"},"Name":"Linelist.acquired_country"},{"Column":{"Expression":{"SourceRef":{"Source":"l"}},"Property":"Eventdate"},"Name":"Linelist.Eventdate"},{"Column":{"Expression":{"SourceRef":{"Source":"l"}},"Property":"clin_status_n"},"Name":"Linelist.clin_status_n"},{"Column":{"Expression":{"SourceRef":{"Source":"l"}},"Property":"acquired_n"},"Name":"Linelist.acquired_n"},{"Column":{"Expression":{"SourceRef":{"Source":"l"}},"Property":"acquired_country_n"},"Name":"Linelist.acquired_country_n"},{"Column":{"Expression":{"SourceRef":{"Source":"l"}},"Property":"CountValue"},"Name":"Linelist.CountValue"},{"Column":{"Expression":{"SourceRef":{"Source":"l"}},"Property":"Localgovernmentarea"},"Name":"Linelist.Localgovernmentarea"},{"Measure":{"Expression":{"SourceRef":{"Source":"l"}},"Property":"M_Age_MedianANDRange"},"Name":"Linelist.M_Age_MedianANDRange"}],"OrderBy":[{"Direction":1,"Expression":{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"AgeGroup"}}}],"GroupBy":[{"SourceRef":{"Source":"l"},"Name":"Linelist"}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,1,2,3,4,5,6,7,8,9,10,11],"ShowItemsWithNoData":[0,1,2,3,4,5,6,7,8,9,10,11],"GroupBy":[0]}]},"Projections":[12],"DataReduction":{"Primary":{"Top":{"Count":1000}}},"Version":1}}}]},"QueryId":"","ApplicationContext":{"DatasetId":"5b547437-24c9-4b22-92de-900b3b3f4785","Sources":[{"ReportId":"964ef513-8ff4-407c-8068-ade1e7f64ca5"}]}}],"cancelQueries":[],"modelId":1959902}'
  r = requests.post(
    'https://wabi-australia-southeast-api.analysis.windows.net/public/reports/querydata?synchronous=true',
    data=q,
    headers={
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
      'Origin': 'https://app.powerbi.com',
      'ActivityId': '6fcca753-9700-4b48-b587-353d4ecdde8d',
      'RequestId': 'ef1fdb86-a106-946a-01c4-c51cec865e73',
      'X-PowerBI-ResourceKey': '80f2a75d-ece4-49dd-9566-236a6522677c',
      'Content-Type': 'application/json',
    })

  return r.text


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

def cache_request(cache_filename, request, force_cache=False):
  if os.path.exists(cache_filename) or force_cache:
    with open(cache_filename, 'rb') as f:
      return f.read()
  else:
    result = request()
    with open(cache_filename, 'wb') as f:
      f.write(result.encode('utf-8'))
    return result

if __name__ == '__main__':
  main()

