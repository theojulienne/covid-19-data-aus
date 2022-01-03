#!/usr/bin/env python3

import collections
import datetime
import json
import os
import re

import bs4
import requests
from word2number import w2n

def main():
  timeseries_data = get_timeseries_data('https://www.health.qld.gov.au/news-events/doh-media-releases')
  timeseries_data = add_test_data(timeseries_data)
  timeseries_data = add_manual_data(timeseries_data)

  # Muck with the data to get it into the format that's expected
  # Fill in the blanks
  dates = sorted(timeseries_data.keys())

  start_time = min([datetime.datetime.strptime(d, '%Y-%m-%d') for d in dates])
  end_time = max([datetime.datetime.strptime(d, '%Y-%m-%d') for d in dates])

  curr_time = start_time + datetime.timedelta(days=1)
  prev_time = start_time
  while curr_time <= end_time:
    curr_key = curr_time.strftime('%Y-%m-%d')
    prev_key = prev_time.strftime('%Y-%m-%d')

    if curr_key not in timeseries_data:
      timeseries_data[curr_key] = timeseries_data[prev_key]
    else:
      timeseries_data[curr_key]['confirmed'] = timeseries_data[curr_key].get('confirmed', timeseries_data[prev_key]['confirmed'])
      timeseries_data[curr_key]['deaths'] = timeseries_data[curr_key].get('deaths', timeseries_data[prev_key]['deaths'])
      timeseries_data[curr_key]['tested'] = timeseries_data[curr_key].get('tested', timeseries_data[prev_key]['tested'])
      timeseries_data[curr_key]['lga'] = timeseries_data[curr_key].get('lga', timeseries_data[prev_key]['lga'])

    prev_time = curr_time
    curr_time = curr_time + datetime.timedelta(days=1)

  dates = sorted(timeseries_data.keys())
  values = [timeseries_data[d] for d in dates]

  # Muck with the LGA data to do the right thing
  lga_data = munge_data_to_output(timeseries_data, dates, 'lga')

  formatted_data = {
    'timeseries_dates': dates,
    'total': {
      'confirmed': [timeseries_data[d]['confirmed'] for d in dates],
      'tested': [timeseries_data[d]['tested'] for d in dates],
      'deaths': [timeseries_data[d]['deaths'] for d in dates],
    },
    'lga': lga_data,
  }

  with open('by_state_partial/qld.json', 'w') as f:
    json.dump(formatted_data, f, indent=2, sort_keys=True)

def get_timeseries_data(url):
  timeseries_data = collections.defaultdict(lambda: {})

  for post_href in get_posts(url):
    print("Parsing post: {}".format(post_href))
    cache_fn = 'data_cache/qld/%s.html' % ('_' + '_'.join(post_href.split('/')[3:]))
    body = cache_request(
      cache_fn,
      lambda: requests.get(post_href).text,
    )

    soup = bs4.BeautifulSoup(body, 'html.parser')
    content = soup.select_one('div#content')

    should_exclude = False
    for excl in ['Safeguards in place to minimise Cairns COVID-19 risk', 'Tests negative following Bundaberg', 'New COVID-19 case prompts reminder for vigilance', 'Update on Queensland COVID-19 testing regime', 'list of', 'warning', 'additional', 'Alert', 'Ipswich', 'strain', 'identified']:
      if excl in soup.select_one('title').text:
        should_exclude = True
    if should_exclude: continue

    date_space = content.select_one('h2,h4')
    if date_space and 'Statement from' in date_space.text: continue
    if not date_space:
      date_space = content.select_one('#last-updated')
    date_text = date_space.text.strip()
    if date_text.startswith('Last updated: '):
      date_text = date_text[14:]
    try:
      date = datetime.datetime.strptime(date_text, '%d %B %Y')
    except ValueError:
      print("Invalid date '{}', skipping".format(date_text))
      continue
    body = re.sub(r'[^\x00-\x7F]+', ' ', content.text)

    # Confirmed count
    confirmed = None
    confirmed_regexes = [
      r'.*Queensland Health has today announced ([\d\w]+) new COVID-19 cases, bringing total cases to (?P<confirmed>[\d,]+).*',
      r'.*Queensland has ([\d\w]+) new (overseas-acquired |confirmed )?cases? of novel coronavirus \(COVID-19\) to report today, bringing (the )?total cases to (?P<confirmed>[\d,]+).*',
      r'.*Queensland has ([\d\w]+) new confirmed cases of novel coronavirus \(COVID-19\) raising the state total to (?P<confirmed>[\d,]+)[\.,].*',
      r'.*Queensland has ([\d\w]+) new confirmed cases of coronavirus \(COVID-19\)(?:,[^,]+,)? raising the state total to (?P<confirmed>[\d,]+)[\.,].*',
      r'.*state total ((to)|(remains at)) (?P<confirmed>[\d,]+)[^\d,].*',
      r'.*There are (?P<confirmed>\d+) confirmed cases of novel coronavirus \(COVID-19\) in Queensland.*',
      r'.*A total of (?P<confirmed>[\w-]+) people in Queensland have been confirmed with COVID-19.*',
      r'.*There have now been (?P<confirmed>[\w-]+) people in Queensland(?: confirmed)? with COVID-19.*',
      r'.*The Queensland Government has reported ([\d\w]+) new cases of COVID-19 in the past 24 hours - in addition to the ([\d\w]+) new cases announced yesterday - bringing the total case number to (?P<confirmed>[\d,]+).*',
    ]
    for r in confirmed_regexes:
      m = re.match(r, body, re.MULTILINE | re.DOTALL)
      if m:
        confirmed = parse_num(m.group('confirmed'))
        break

    # Death count
    m = re.match(r'.*Queensland Health can confirm a (?P<deaths>\w+) Queenslander has passed away.*', body, re.MULTILINE | re.DOTALL)
    deaths = None
    if m:
      deaths = parse_ordinal(m.group('deaths'))

    # LGAs
    lga_data = None
    lga_table = content.select_one('table')
    if lga_table:
      lga_data = {}
      header = None
      first = True

      for tr in lga_table.select('tr'):
        tds = tr.select('th,td')

        # If it's the header row (or the weirdly malformed footer), skip it
        if first:
          header = tds
          first = False
          continue
        if len(tds) == 0 or len(tds) == 1:
          continue

        lga_name = clean_whitespace(tds[0].text.strip()).replace('*', '')
        lga_count = parse_num(tds[-1].text.strip())

        if lga_name == 'Total':
          # If it's the footer row, count it for totals
          if confirmed is None:
            confirmed = lga_count
          if deaths is None and header is not None and header[-2].text.strip() == 'Deaths':
            deaths = parse_num(tds[-2].text.strip())
        else:
          # otherwise it's an LGA
          lga_data[lga_name] = lga_count

    # We don't attempt to parse posts prior to Feb 25 - those we add manually, because they're too
    # variable. We also exclude a single March 26 post that includes no new information
    if confirmed is None and deaths is None and date.strftime('%Y-%m-%d') not in ('2020-03-26', '2020-03-31') and date > datetime.datetime(year=2020, month=2, day=25):
      print('WARNING: Unparseable post! %s (%s)' % (date.strftime('%Y-%m-%d'), cache_fn))
      continue

    date_key = date.strftime('%Y-%m-%d')
    if confirmed is not None:
      timeseries_data[date_key]['confirmed'] = confirmed
    if deaths is not None:
      timeseries_data[date_key]['deaths'] = deaths
    if lga_table is not None:
      timeseries_data[date_key]['lga'] = lga_data

  return timeseries_data

def clean_whitespace(txt):
  return re.sub(r'\s+', ' ', txt.replace('&nbsp;', ' '))

def add_test_data(timeseries_data):
  poll_and_update_test_page()

  test_data_cache_dir = 'data_cache/qld/status-tracing/'

  files = os.listdir(test_data_cache_dir)
  for filename in files:
    print("Processing tracing data: {}".format(filename))
    body = None
    with open(os.path.join(test_data_cache_dir, filename), 'rb') as f:
      body = f.read()
    
    soup = bs4.BeautifulSoup(body, 'html.parser')
    content = soup.select_one('div#qg-primary-content')

    m = re.match(r'.*Testing update as at (?P<date>[^<]+)', str(content).strip(), re.MULTILINE | re.DOTALL)
    if m:
      # new format!
      sm = re.match(r'.*Total samples tested: .*?(?P<samples>[\d,]+)', content.text.strip(), re.MULTILINE | re.DOTALL)
      date = datetime.datetime.strptime(m.group('date') + ' 2020', '%d %B %Y')
      samples = parse_num(sm.group('samples'))
      timeseries_data[date.strftime('%Y-%m-%d')]['tested'] = samples
      continue

    m = None
    h2 = content.select_one('h2')
    if h2:
      m = re.match(r'.*Status as at (?P<date>\d+ \w+ \d+)$', h2.text.strip(), re.MULTILINE | re.DOTALL)
    if m is None:
      m = re.match(r'.*Last updated: .* (?P<date>\d+ \w+ \d+)$', content.select_one('.qh-facts-header p').text.strip(), re.MULTILINE | re.DOTALL)
    date = datetime.datetime.strptime(m.group('date'), '%d %B %Y')

    tables = content.select('table')
    if len(tables) > 0:
      testing_table = tables[-1]

      # Only process the table if we can be sure that we're looking at the right
      # thing
      if testing_table.select_one('tr').select('th')[-1].text.strip() == 'Samples tested':
        for tr in testing_table.select('tr'):
          tds = tr.select('td')

          # Skip the header row
          if len(tds) == 0:
            continue

          if tds[0].text.strip() == 'Total':
            timeseries_data[date.strftime('%Y-%m-%d')]['tested'] = parse_num(tds[1].text.strip())

    # Otherwise, if tests are specified by hand
    sm = re.match(r'.*Total samples tested: .*?(?P<samples>[\d,]+)', content.text.strip(), re.MULTILINE | re.DOTALL)
    if sm:
      timeseries_data[date.strftime('%Y-%m-%d')]['tested'] = parse_num(sm.group('samples'))

    # And finally, the shiny new fact page that has useful information
    fact_bar = content.select_one('.qh-fact-wrapper')
    if fact_bar:
      cases_bits = fact_bar.select('.cases span') + fact_bar.select('.local span') + fact_bar.select('.new span')
      confirmed = parse_num(cases_bits[0].text.strip())
      tested = parse_num(fact_bar.select('.tested span')[0].text.strip())
      timeseries_data[date.strftime('%Y-%m-%d')]['confirmed'] = confirmed
      timeseries_data[date.strftime('%Y-%m-%d')]['tested'] = tested

      deaths_span = fact_bar.select('.lost span')
      if deaths_span:
        deaths = parse_num(deaths_span[0].text.strip())
        timeseries_data[date.strftime('%Y-%m-%d')]['deaths'] = deaths

  return timeseries_data

# unfortunately QLD doesn't have a history of testing data. so instead, every
# poll, we check this page, and save it as the "status as at" date :'(
def poll_and_update_test_page():
  status_url = 'https://www.qld.gov.au/health/conditions/health-alerts/coronavirus-covid-19/current-status/statistics'
  response_body = requests.get(status_url).text

  # Extract the date
  soup = bs4.BeautifulSoup(response_body, 'html.parser')
  content = soup.select_one('div#qg-primary-content')

  m = None
  h2 = content.select_one('h2')
  if h2:
    m = re.match(r'.*Status as at (?P<date>\d+ \w+ \d+)$', h2.text.strip(), re.MULTILINE | re.DOTALL)
  if m is None:
    m = re.match(r'.*Last updated: .* (?P<date>\d+ \w+ \d+)$', content.select_one('.qh-facts-header p').text.strip(), re.MULTILINE | re.DOTALL)
  if m is None:
    raise Exception('Unable to pull QLD status page date!')
  date = datetime.datetime.strptime(m.group('date'), '%d %B %Y')

  # Save the current status page
  status_file = 'data_cache/qld/status-tracing/' + date.strftime('%Y-%m-%d') + '.html'
  with open(status_file, 'wb') as f:
    f.write(response_body.encode('utf-8'))

def add_manual_data(timeseries_data):
  events = {
    # https://www.health.qld.gov.au/news-events/doh-media-releases/releases/queensland-coronavirus-update
    '2020-01-28': {
      'confirmed': 0,
      'deaths': 0,
      'tested': 6,
      'lga': {},
    },
    # https://www.health.qld.gov.au/news-events/doh-media-releases/releases/queensland-coronavirus-update-290120
    '2020-01-29': {
      'confirmed': 1,
    },
    # https://www.health.qld.gov.au/news-events/doh-media-releases/releases/queensland-coronavirus-update2
    '2020-01-30': {
      'confirmed': 2,
    },
    # https://www.health.qld.gov.au/news-events/doh-media-releases/releases/queensland-coronavirus-update3
    '2020-02-04': {
      'confirmed': 3,
    },
    # https://www.health.qld.gov.au/news-events/doh-media-releases/releases/queensland-coronavirus-update4
    '2020-02-05': {
      'confirmed': 4,
    },
    # https://www.health.qld.gov.au/news-events/doh-media-releases/releases/queensland-coronavirus-update5
    '2020-02-06': {
      'confirmed': 5,
    },
    # https://www.health.qld.gov.au/news-events/doh-media-releases/releases/queensland-coronavirus-update8
    '2020-02-22': {
      'confirmed': 7,
    },
    # https://www.abc.net.au/news/2020-03-25/queensland-man-dies-from-coronavirus-covid-19/12090804
    # The first Queenslander died in NSW, shortly after getting off a flight to Sydney
    '2020-03-15': {
      'deaths': 1,
    },
    # https://web.archive.org/web/20200319063836/https://www.qld.gov.au/health/conditions/health-alerts/coronavirus-covid-19/current-status/current-status-and-contact-tracing-alerts
    '2020-03-19': {
      'tested': 27064,
    },
    # https://web.archive.org/web/20200320094233/https://www.qld.gov.au/health/conditions/health-alerts/coronavirus-covid-19/current-status/current-status-and-contact-tracing-alerts
    '2020-03-20': {
      'tested': 28386,
    },
    # https://web.archive.org/web/20200321112923/https://www.qld.gov.au/health/conditions/health-alerts/coronavirus-covid-19/current-status/current-status-and-contact-tracing-alerts
    '2020-03-21': {
      'tested': 29867,
    },
    # https://web.archive.org/web/20200323042338/https://www.qld.gov.au/health/conditions/health-alerts/coronavirus-covid-19/current-status/current-status-and-contact-tracing-alerts
    '2020-03-22': {
      'tested': 32394,
    },
    # https://web.archive.org/web/20200324041724/https://www.qld.gov.au/health/conditions/health-alerts/coronavirus-covid-19/current-status/current-status-and-contact-tracing-alerts
    '2020-03-23': {
      'tested': 37334,
    },
    # No Wayback machine entry for March 24 data
    # https://web.archive.org/web/20200325173429/https://www.qld.gov.au/health/conditions/health-alerts/coronavirus-covid-19/current-status/current-status-and-contact-tracing-alerts
    '2020-03-25': {
      'tested': 38860,
    },
    # No Wayback machine entry for March 26 data
    # https://web.archive.org/web/20200327031520/https://www.qld.gov.au/health/conditions/health-alerts/coronavirus-covid-19/current-status/current-status-and-contact-tracing-alerts
    '2020-03-27': {
      'tested': 42965,
    },
    # No Wayback machine entry for March 28 data
    # https://web.archive.org/web/20200329052305/https://www.qld.gov.au/health/conditions/health-alerts/coronavirus-covid-19/current-status/current-status-and-contact-tracing-alerts
    '2020-03-29': {
      'tested': 49769,
    },
  }

  for date in sorted(events.keys()):
    event_data = events[date]

    for k in event_data:
      timeseries_data[date][k] = event_data[k]

  return timeseries_data

def parse_ordinal(ordinal):
  simple_nums = {
    'fifth': 5,
  }
  if ordinal in simple_nums:
    return simple_nums[ordinal]

  ordinal = ordinal.replace('first', 'one').replace('second', 'two').replace('third', 'three').replace('ieth', 'y')
  ordinal = re.sub(r'th$', '', ordinal)

  return parse_num(ordinal)

def parse_num(num):
  num_match = re.match(r'^[\d,]+', num)
  if num_match:
    return int(num_match.group(0).replace(',', ''))
  else:
    lookups = {
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

    # I can't believe I have to do this, but w2n apparently doesn't support
    # the teen numbers?! (and silently drops them if the teen num is after a
    # number it can parse, e.g. "one hundred and eighteen")
    if num in lookups:
      return lookups[num]
    for k in lookups.keys():
      if k in num:
        raise Exception('w2n is going to handle this wrong, aborting: %s' % num)
    return w2n.word_to_num(num.replace('-', ' '))

def get_posts(url):
  posts = []

  page_num = 1
  curr_year = 2020
  while curr_year == 2020:
    page_url = url + '?result_707098_result_page=%d' % page_num

    post_list_soup = bs4.BeautifulSoup(requests.get(page_url).text, 'html.parser')
    press_zebra = post_list_soup.select_one('div.presszebra')

    for div in press_zebra.select('div'):
      post_date = datetime.datetime.strptime(div.select_one('span').text.strip(), '%d %B %Y')
      post_title = div.select_one('a').text
      post_href = div.select_one('a').attrs['href']

      if 'COVID-19' in post_title or 'coronavirus' in post_title:
        posts.append(post_href)

      curr_year = post_date.year

    page_num += 1

  return posts

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
