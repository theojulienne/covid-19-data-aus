import datetime
import json
import re

import bs4
import requests
import feedparser

def main():
  # Parse the NSW Health media RSS feed, pulling out
  nsw_feed = feedparser.parse('https://www.health.nsw.gov.au/_layouts/feed.aspx?xsl=1&web=/news&page=4ac47e14-04a9-4016-b501-65a23280e841&wp=baabf81e-a904-44f1-8d59-5f6d56519965')
  timeseries_data = get_timeseries_data(nsw_feed)

  dates = sorted(timeseries_data.keys())
  values = [timeseries_data[d] for d in dates]

  age_group_keyset = set()
  for v in values:
    for k in v.get('age_groups', {}).keys():
      age_group_keyset.add(k)
  age_group_keys = sorted(age_group_keyset)

  age_group_data = {}
  for a in age_group_keys:
    age_group_data[a] = []
    for d in dates:
      age_group_data[a].append(timeseries_data[d].get('age_groups', {}).get(a, 0))

  source_keyset = set()
  for v in values:
    for k in v.get('sources', {}).keys():
      source_keyset.add(k)
  source_keys = sorted(source_keyset)

  source_data = {}
  for s in source_keys:
    source_data[s] = [timeseries_data[d].get('sources', {}).get(s, 0) for d in dates]

  formatted_data = {
    'timeseries_dates': dates,
    'total': {
      'confirmed': [timeseries_data[d]['confirmed'] for d in dates],
      'tested': [timeseries_data[d]['tested'] for d in dates],
    },
    'age_groups': {
      'keys': age_group_keys,
      'subseries': age_group_data,
    },
    'sources': {
      'keys': source_keys,
      'subseries': source_data,
    }
  }

  with open('by_state/nsw.json', 'w') as f:
    json.dump(formatted_data, f, indent=2)

def get_timeseries_data(nsw_feed):
  timeseries_data = {}

  for entry in nsw_feed['entries']:
    if ('COVID-19' in entry['title'] or 'coronavirus' in entry['title']) and 'stat' in entry['title']:
      href = entry['links'][0]['href']
      soup = bs4.BeautifulSoup(requests.get(href).text, 'html.parser')

      date = datetime.datetime.strptime(soup.select_one('div.newsdate').text.strip(), '%d %B %Y')
      tables = soup.select('table.moh-rteTable-6')

      age_groups = None
      sources = None

      for t in tables:
        parsed_table = parse_table(t)

        if parsed_table['headers'][0] == 'Cases':
          confirmed, tested = process_overall_table(parsed_table)

        elif parsed_table['headers'][0] == 'Age group':
          age_groups = process_age_table(parsed_table)

        elif parsed_table['headers'][0] == 'Source':
          sources = process_source_table(parsed_table)
        else:
          raise 'Unknown table! %s' % repr(parsed_table['headers'])

      date_data = {
        'confirmed': confirmed,
        # Exclude in progress tests from the total tested, since we want
        # this to indicate completed tests
        'tested': tested,
      }

      if age_groups is not None:
        date_data['age_groups'] = age_groups

      if sources is not None:
        date_data['sources'] = sources

      date_key = date.strftime('%Y-%m-%d')

      # If there's more than one update for a day, only use the first (most
      # recent) one
      if date_key not in timeseries_data:
        timeseries_data[date_key] = date_data

  return timeseries_data

def process_overall_table(table):
  confirmed = [r[1] for r in table['data'] if 'Confirmed' in r[0]][0]

  potential_in_progress = [r[1] for r in table['data'] if 'investigation' in r[0]]
  if len(potential_in_progress) > 0:
    in_progress = potential_in_progress[0]
  else:
    in_progress = 0
  total = [r[1] for r in table['data'] if 'Total' in r[0]][0]

  return [confirmed, total - in_progress]

def process_age_table(table):
  age_groups = {}

  for r in table['data']:
    # We don't consistently add this row, because that would be way too helpful
    if r[0] == 'Total':
      continue

    # If this is a 20-30, rather than 20-29 style header, adjust it
    key = r[0]
    if key.endswith('0'):
      base_num = int(r[0].split('-')[0])
      key = '%d-%d' % (base_num, base_num + 9)
    age_groups[key] = r[-1]

  return age_groups

def process_source_table(table):
  overseas = 0
  contact = 0
  community = 0
  investigation = 0

  for r in table['data'][:-1]:
    if 'Overseas' in r[0]:
      overseas = r[1]
    elif 'contact of a confirmed case' in r[0] or 'Epi link' in r[0]:
      contact = r[1]
    elif 'not identified' in r[0] or 'Unknown' in r[0]:
      community = r[1]
    elif 'investigation' in r[0]:
      investigation = r[1]
    else:
      raise 'Unknown source', r[0]

  return {
    'Overseas acquired': overseas,
    'Locally acquired - contact of a confirmed case': contact,
    'Locally acquired - contact not identified': community,
    'Under investigation': investigation,
  }

def parse_table(to_parse):
  rows = to_parse.select('tr')
  headers = [clean_text(th.text) for th in rows[0].select('th')]
  data = [[parse_datum(clean_text(td.text)) for td in r.select('th') + r.select('td')] for r in rows[1:]]
  return {
    'headers': headers,
    'data': data
  }

def clean_text(text):
  text = text.replace(u'\u200b', '')
  return text.split('*')[0].strip()

def parse_datum(datum):
  if re.match(r'^[\d,]+$', datum):
    return int(datum.replace(',', ''))
  else:
    return datum

if __name__ == '__main__':
  main()

