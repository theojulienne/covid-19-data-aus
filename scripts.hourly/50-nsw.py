#!/usr/bin/env python3

import collections
import copy
import datetime
import json
import re
import os

import bs4
import requests
import certifi

SSL_CERT_PATH=os.getcwd() + '/ssl/digicert-nswhealth-chain.pem'

def main():
  # The NSW Health RSS feed only goes back a few weeks, so we have to scrape this page instead :(
  timeseries_data = get_timeseries_data([
    'https://www.health.nsw.gov.au/news/Pages/2020-nsw-health.aspx',
    'https://www.health.nsw.gov.au/news/Pages/2021-nsw-health.aspx',
    'https://www.health.nsw.gov.au/news/Pages/2022-nsw-health.aspx',
  ])
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
      'deaths': [timeseries_data[d]['deaths'] for d in dates],
      'tested': [timeseries_data[d]['tested'] for d in dates],
      'recovered': [timeseries_data[d]['recovered'] for d in dates],
      'current_hospitalized': [timeseries_data[d]['hospitalized'] for d in dates],
      'current_icu': [timeseries_data[d]['icu'] for d in dates],
      'current_ventilators': [timeseries_data[d]['ventilators'] for d in dates],
    },
    'age_groups': age_group_data,
    'sources': source_data,
  }

  with open('by_state_partial/nsw.json', 'w') as f:
    json.dump(formatted_data, f, indent=2, sort_keys=True)

def get_timeseries_data(urls):
  print('Debugging IP: {}'.format(requests.get('http://icanhazip.com/').text))
  timeseries_data = {}
  for url in urls:
    post_list_soup = bs4.BeautifulSoup(requests.get(url, verify=SSL_CERT_PATH).text, 'html.parser')

    for li in post_list_soup.select('div#ContentHtml1Zone2 li li'):
      # There are other media releases in the list - we only care about those that
      # talk about COVID-19/coronavirus statistics
      if ('COVID-19' in li.text or 'coronavirus' in li.text) and 'stat' in li.text:
        href = li.select_one('a').attrs['href']
        if not href.startswith('https://') and not href.startswith('/'):
          href = 'https://www.health.nsw.gov.au/news/Pages/' + href
        uri = href.replace('https://www.health.nsw.gov.au/', '')
        cache_filename = 'data_cache/nsw/'+uri.replace('/', '_')+'.html'
        if os.path.exists(cache_filename):
          with open(cache_filename, 'rb') as f:
            response_body = f.read()
        else:
          response_body = requests.get(href, verify=SSL_CERT_PATH).text
          with open(cache_filename, 'wb') as f:
            f.write(response_body.encode('utf-8'))
        soup = bs4.BeautifulSoup(response_body, 'html.parser')

        date = datetime.datetime.strptime(soup.select_one('div.newsdate').text.strip(), '%d %B %Y')
        tables = soup.select('table.moh-rteTable-6')

        age_groups = None
        sources = None

        confirmed = tested = deaths = recovered = None
        print("Processing: {}".format(cache_filename))
        for t in tables:
          parsed_table = parse_table(t)

          if len(parsed_table['headers']) == 0:
            print("WARNING: invalid table")
            continue

          if 'Confirmed cases' in parsed_table['headers'][0]:
            parsed_table['data'] = [parsed_table['headers']] + parsed_table['data']
            parsed_table['data'][0][-1] = parse_datum(clean_text(parsed_table['data'][0][-1]))
            parsed_table['headers'] = ['Cases', 'Count']

          if parsed_table['headers'][0] in ('', 'Cases', 'Updates', 'Status'):
            confirmed, tested, deaths, recovered = process_overall_table(parsed_table)

          elif parsed_table['headers'][0].lower() == 'age group':
            age_groups = process_age_table(parsed_table)

          elif re.sub(r'\s+', ' ', parsed_table['headers'][0]) in ('By likely source of infection', 'Source', 'Likely source of infection'):
            sources = process_source_table(parsed_table)

          elif parsed_table['headers'][0] == 'Outcome':
            maybe_confirmed, maybe_recovered = process_outcome_table(parsed_table)
            if maybe_confirmed and not confirmed:
              confirmed = maybe_confirmed
            if maybe_recovered and not recovered:
              recovered = maybe_recovered

          elif parsed_table['headers'][0].startswith('Since') or parsed_table['headers'][0].startswith('Asymptomatic') or parsed_table['headers'][0].startswith('From ') or parsed_table['headers'][0].startswith('Location') or parsed_table['headers'][0].startswith('Suburb') or parsed_table['headers'][0].startswith('Route') or 'vaccination' in parsed_table['headers'][0].lower():
            pass

          else:
            print('WARNING: Unknown table in %s! %s' % (cache_filename, repr(parsed_table['headers'])))
            continue

        body = soup.select_one('div.maincontent').text
        hospitalized, icu, ventilators = parse_full_body(body)

        date_data = {
          'confirmed': confirmed,
          # Exclude in progress tests from the total tested, since we want
          # this to indicate completed tests
          'tested': tested,
          'deaths': deaths,
          'recovered': recovered,
          'hospitalized': hospitalized,
          'icu': icu,
          'ventilators': ventilators,
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

def process_outcome_table(table):
  confirmed = None
  confirmed_row = [r[1] for r in table['data'] if clean_whitespace(r[0]).lower() == 'total']
  if len(confirmed_row) > 0:
    confirmed = confirmed_row[0]

  recovered = [r[1] for r in table['data'] if clean_whitespace(r[0]).lower() == 'recovered'][0]
  not_recovered = [r[1] for r in table['data'] if clean_whitespace(r[0]).lower() in ['not recovered', 'not yet recovered']][0]
  too_soon = [r[1] for r in table['data'] if 'data not available' in clean_whitespace(r[0]).lower()][0]
  active = not_recovered + too_soon

  return confirmed, recovered

def process_overall_table(table):
  confirmed = [r[1] for r in table['data'] if 'confirmed' in clean_whitespace(r[0]).lower()][0]
  deaths = ([r[1] for r in table['data'] if 'deaths' in r[0].lower() or 'died' in r[0].lower()] + [None])[0] # only available in new pages
  recovered = ([r[1] for r in table['data'] if 'recovered' in r[0].lower()] + [None])[0] # only available in even newer pages

  potential_in_progress = [r[1] for r in table['data'] if 'investigation' in clean_whitespace(r[0])]
  if len(potential_in_progress) > 0:
    in_progress = potential_in_progress[0]
  else:
    in_progress = 0

  # If there's a handy "total" row, use that
  if len([r[1] for r in table['data'] if clean_whitespace(r[0]) in ['Total', 'Total persons tested', 'Total tests carried out']]) > 0:
    total = [r[1] for r in table['data'] if clean_whitespace(r[0]) in ['Total', 'Total persons tested', 'Total tests carried out']][0]
  else:
    total = sum([r[1] for r in table['data']])

  return [confirmed, total - in_progress, deaths, recovered]

def clean_whitespace(txt):
  return re.sub(r'\s+', ' ', txt.replace('&nbsp;', ' '))

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
  interstate = 0

  for r in table['data'][:-1]:
    cleaned_r0 = re.sub('[^a-zA-z0-9\-\/\s]', '', r[0])
    cleaned_r0 = re.sub('\s+', ' ', cleaned_r0)
    if 'Overseas' in cleaned_r0:
      overseas = r[-1]
    elif 'contact of a confirmed case' in cleaned_r0 or 'contact of aconfirmed case' in cleaned_r0 or 'Epi link' in cleaned_r0 or 'Locally acquired linked' in cleaned_r0:
      contact = r[-1]
    elif 'not identified' in cleaned_r0 or 'Unknown' in cleaned_r0 or 'Locally acquired no links' in cleaned_r0 or cleaned_r0 == 'Locally acquired':
      community = r[-1]
    elif 'investigation' in cleaned_r0:
      investigation = r[-1]
    elif 'Interstate' in cleaned_r0:
      interstate = r[-1]
    else:
      raise Exception('Unknown source: {}'.format(cleaned_r0))

  return {
    'Overseas acquired': overseas,
    'Interstate acquired': interstate,
    'Locally acquired - contact of a confirmed case': contact,
    'Locally acquired - contact not identified': community,
    'Under investigation': investigation,
  }

def parse_full_body(text):
  m = re.match(r'.*There are(?: currently)? (?P<hospitalized>\d+) COVID-19 cases being treated in NSW.* (?P<icu>\d+) cases in our Intensive Care Units and, of those, (?P<ventilators>\d+) require ventilators at this stage.*', text, re.MULTILINE | re.DOTALL)

  hospitalized = None
  icu = None
  ventilators = None
  if m:
    hospitalized = int(m.group('hospitalized'))
    icu = int(m.group('icu'))
    ventilators = int(m.group('ventilators'))

  return (hospitalized, icu, ventilators)

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
  text = text.replace('(see', '')
  text = text.replace('below)', '')
  # This is not traditionally how one uses an asterisk -_-
  if text.startswith('*'):
    text = text[1:]
  return text.split('*')[0].strip()

def parse_datum(datum):
  if re.match(r'^[\d,]+$', datum):
    return int(datum.replace(',', ''))
  else:
    return datum

def add_manual_data(timeseries_data):
  events = {
    # https://www.health.nsw.gov.au/news/Pages/20200125_02.aspx,
    # https://www.health.nsw.gov.au/news/Pages/20200125_03.aspx
    '2020-01-25': {
      'age_groups': {
        '30-39': 1,
        '40-49': 1,
        '50-59': 1,
      },
      'sources': {
        'Overseas acquired': 3,
      }
    },
    '2020-01-26': {}, # https://www.health.nsw.gov.au/news/Pages/20200126_03.aspx
    # https://www.health.nsw.gov.au/news/Pages/20200127_03.aspx
    '2020-01-27': {
      'age_groups': {
        '20-29': 1, # Age later confirmed in https://www.health.nsw.gov.au/news/Pages/20200130_02.aspx
      },
      'sources': {
        'Overseas acquired': 4, # I assume this case was also overseas acquired? Can't find hard evidence of that though
      }
    },
    '2020-01-28': {}, # https://www.health.nsw.gov.au/news/Pages/20200128_01.aspx
    '2020-01-29': {}, # https://www.health.nsw.gov.au/news/Pages/20200129_00.aspx
    '2020-01-30': {}, # https://www.health.nsw.gov.au/news/Pages/20200130_02.aspx
    '2020-01-31': {}, # https://www.health.nsw.gov.au/news/Pages/20200131_02.aspx
    '2020-02-01': {}, # https://www.health.nsw.gov.au/news/Pages/20200201_00.aspx
    '2020-02-02': {}, # https://www.health.nsw.gov.au/news/Pages/20200202_02.aspx
    '2020-02-03': {}, # https://www.health.nsw.gov.au/news/Pages/20200203_01.aspx
    '2020-02-04': {}, # https://www.health.nsw.gov.au/news/Pages/20200204_02.aspx
    '2020-02-05': {}, # https://www.health.nsw.gov.au/news/Pages/20200205_01.aspx
    '2020-02-06': {}, # https://www.health.nsw.gov.au/news/Pages/20200206_01.aspx
    '2020-02-07': {}, # https://www.health.nsw.gov.au/news/Pages/20200207_01.aspx
    '2020-02-08': {}, # https://www.health.nsw.gov.au/news/Pages/20200208_00.aspx
    '2020-02-09': {}, # https://www.health.nsw.gov.au/news/Pages/20200209_00.aspx
    '2020-02-10': {}, # https://www.health.nsw.gov.au/news/Pages/20200210_03.aspx
    '2020-02-11': {}, # https://www.health.nsw.gov.au/news/Pages/20200211_00.aspx
    '2020-02-12': {}, # https://www.health.nsw.gov.au/news/Pages/20200212_00.aspx
    '2020-02-13': {}, # https://www.health.nsw.gov.au/news/Pages/20200213_01.aspx
    '2020-02-14': {}, # https://www.health.nsw.gov.au/news/Pages/20200214_01.aspx
    '2020-02-15': {}, # No media release this day
    '2020-02-16': {}, # https://www.health.nsw.gov.au/news/Pages/20200216_00.aspx
    '2020-02-17': {}, # https://www.health.nsw.gov.au/news/Pages/20200217_00.aspx
    '2020-02-18': {}, # https://www.health.nsw.gov.au/news/Pages/20200218_00.aspx
    '2020-02-19': {}, # https://www.health.nsw.gov.au/news/Pages/20200219_02.aspx
    '2020-02-20': {}, # https://www.health.nsw.gov.au/news/Pages/20200220_00.aspx
    '2020-02-21': {}, # https://www.health.nsw.gov.au/news/Pages/20200221_00.aspx
    '2020-02-22': {}, # https://www.health.nsw.gov.au/news/Pages/20200222_01.aspx
    '2020-02-23': {}, # https://www.health.nsw.gov.au/news/Pages/20200223_00.aspx
    '2020-02-24': {}, # https://www.health.nsw.gov.au/news/Pages/20200224_00.aspx
    '2020-02-25': {}, # https://www.health.nsw.gov.au/news/Pages/20200225_00.aspx
    '2020-02-26': {}, # https://www.health.nsw.gov.au/news/Pages/20200226_00.aspx
    '2020-02-27': {}, # No media release this day
    '2020-02-28': {}, # No media release this day
    '2020-02-29': {}, # No media release this day
    # https://www.health.nsw.gov.au/news/Pages/20200301_00.aspx
    # https://www.health.nsw.gov.au/news/Pages/20200301_01.aspx
    '2020-03-01': {
      'age_groups': {
        '40-49': 1,
        '50-59': 1,
      },
      'sources': {
        'Overseas acquired': 2,
      }
    },
    # This media release is date the 3rd of March, but the way the subsequent
    # one is written suggests that this should have been on the 2nd
    # https://www.health.nsw.gov.au/news/Pages/20200303_00.aspx
    '2020-03-02': {
      'age_groups': {
        '30-39': 1,
        '40-49': 1,
        '50-59': 1,
      },
      'sources': {
        'Overseas acquired': 1,
        'Locally acquired - contact of a confirmed case': 1,
        'Locally acquired - contact not identified': 1,
      }
    },
    # https://www.health.nsw.gov.au/news/Pages/20200303_04.aspx
    '2020-03-03': {
      'age_groups': {
        '30-39': 2,
        '50-59': 2,
        '60-69': 2,
      },
      'sources': {
        'Overseas acquired': 5,
        'Locally acquired - contact not identified': 1,
      }
    },
    # https://www.health.nsw.gov.au/news/Pages/20200304_06.aspx
    '2020-03-04': {
      'age_groups': {
        '20-29': 1, # Doctor who works at Liverpool hospital (age confirmed the day aften)
        '30-39': 1, # Lives on the Northern Beaches, returned from Iran (travel history confirmed the day after)
        '50-59': 1, # Lives in Cronulla, nurse at Dorothy Henderson Lodge
        '60-69': 1, # Returned from Philippines
        '70-79': 1, # Resident of Dorothy Henderson Lodge
        '90-99': 1, # Resident of Dorothy Henderson Lodge
      },
      'sources': {
        'Overseas acquired': 2,
        'Locally acquired - contact of a confirmed case': 4, # Counting the Dorothy Henderson Lodge as epi links, and
                       # the female doctor an epi link of the other doctor,
                       # since they attended the same conference
      }
    },
    # https://www.health.nsw.gov.au/news/Pages/20200305_01.aspx
    # https://www.health.nsw.gov.au/news/Pages/20200305_02.aspx
    '2020-03-05': {
      'age_groups': {
        # Missing age for traveler returned from Singapore, went to Goulburn, went to Darwin
        '10-19': 1, # Mother worked at Ryde hospital
        '50-59': 1, # Traveller returned from Italy
        '90-99': 1, # Resident of Dorothy Henderson Lodge
      },
      'sources': {
        'Overseas acquired': 2,
        'Locally acquired - contact of a confirmed case': 2,
      }
    },
    # https://www.health.nsw.gov.au/news/Pages/20200306_00.aspx
    '2020-03-06': {
      'age_groups': {
        '10-19': 1, # Household contact of a case in Western Sydney
        '20-29': 2, # Two staff members at Dorothy Henderson Lodge
      },
      'sources': {
        'Locally acquired - contact of a confirmed case': 3,
      }
    },
    # https://www.health.nsw.gov.au/news/Pages/20200307_00.aspx
    '2020-03-07': {
      'age_groups': {
        '20-29': 1, # Close contact of confirmed case
        '40-49': 3, # Two family members of confirmed case, close contact of confirmed case
        '50-59': 1, # Close contact of confirmed case
        '70-79': 1, # Returned from Italy
      },
      'sources': {
        'Overseas acquired': 1,
        'Locally acquired - contact of a confirmed case': 5,
      }
    },
    # https://www.health.nsw.gov.au/news/Pages/20200308_00.aspx
    # https://www.health.nsw.gov.au/news/Pages/20200308_01.aspx
    '2020-03-08': {
      'age_groups': {
        '30-39': 1, # Health care worker at Ryde, contact of Dorothy Henderson case
        '40-49': 1, # Known to have travelled overseas recently
        '50-59': 1, # Contact of confirmed case
        '70-79': 1, # Under investigation
      },
      'sources': {
        'Overseas acquired': 1,
        'Locally acquired - contact of a confirmed case': 2,
        'Under investigation': 1,
      }
    },
    # https://www.health.nsw.gov.au/news/Pages/20200309_01.aspx
    '2020-03-09': {
      'age_groups': {
        '10-19': 3, # Two students at St Patrick's Marist College, Dundas (epi links to other cases);
                    # Year 7 Willoughby Girls student, under investigation
        '30-39': 1, # Returned from Philippines
        '50-59': 3, # Fathers of the two Marist students (epi links to other cases);
                    # Mother of the Willoughby student (counting as epi link to daughter)
      },
      'sources': {
        'Overseas acquired': 1,
        'Locally acquired - contact of a confirmed case': 5,
        'Under investigation': 1,
      }
    },
    # https://www.health.nsw.gov.au/news/Pages/20200310_00.aspx
    # https://www.health.nsw.gov.au/news/Pages/20200310_01.aspx
    '2020-03-10': {
      'age_groups': {
        '20-29': 2, # Contact of Ryde Hospital case; returned from Hong Kong
        '30-39': 2, # Under investigation; contact of Dorothy Henderson Lodge case
        '40-49': 4, # Returned from South Korea; under investigation; related to Dorothy Henderson Lodge case;
                    # Under investigation
        '50-59': 2, # Returned from the US, another returned from US
        '60-69': 2, # Returned from Switzerland; close contact
        '70-79': 1, # Under investigation
        '80-89': 1, # Dorothy Henderson Lodge resident
      },
      'sources': {
        'Overseas acquired': 5,
        'Locally acquired - contact of a confirmed case': 5,
        'Under investigation': 4,
      }
    },
    # https://www.health.nsw.gov.au/news/Pages/20200311_00.aspx
    '2020-03-11': {
      'age_groups': {
        '20-29': 3, # Three returned from Italy together
        '70-79': 1, # Dorothy Henderson Lodge contact
      },
      'sources': {
        'Overseas acquired': 3,
        'Locally acquired - contact of a confirmed case': 1,
      }
    },
    # https://www.health.nsw.gov.au/news/Pages/20200312_00.aspx
    '2020-03-12': {
      'age_groups': {
        '10-19': 2, # Close contact of Ryde case; no overseas travel
        '20-29': 2, # No overseas travel; returned from Malaysia
        '30-39': 2, # Under investigation; under investigation
        '40-49': 1, # No overseas travel
        '50-59': 2, # Returned from UK via Dubai; under investigation
        '60-69': 3, # Two returned from Italy; under investigation
        '80-89': 1, # No overseas travel
      },
      'sources': {
        'Overseas acquired': 4,
        'Locally acquired - contact of a confirmed case': 1,
        'Locally acquired - contact not identified': 4,
        'Under investigation': 4,
      }
    },
    # https://www.health.nsw.gov.au/news/Pages/20200313_00.aspx
    '2020-03-13': {
      'age_groups': {
        '20-29': 3, # Returned from UK via Perth; returned from Switzerland via Dubai; under investigation
        '30-39': 4, # Under investigation; under investigation; returned from US; two returned from Philippines
        '40-49': 4, # Known contact; returned from US; community; under investigation
        '50-59': 2, # Returned from Italy; under investigation
      },
      'sources': {
        'Overseas acquired': 7,
        'Locally acquired - contact of a confirmed case': 1,
        'Locally acquired - contact not identified': 1,
        'Under investigation': 5,
      }
    },
    # https://www.health.nsw.gov.au/news/Pages/20200314_00.aspx
    '2020-03-14': {
      'age_groups': {
        '20-29': 2, # Investigation; Investigation
        '30-39': 7, # Investigation; Investigation; Investigation; Investigation;
                    # returned from Finland and UK; Investigation; Investigation
        '40-49': 4, # Investigation; investigation; Investigation; returned from France and UK
        '50-59': 1, # Investigation
        '60-69': 6, # Returned from Germany; returned from Italy; Investigation; two returned from Italy; returned from US
      },
      'sources': {
        'Overseas acquired': 7,
        'Locally acquired - contact of a confirmed case': 0,
        'Locally acquired - contact not identified': 0,
        'Under investigation': 13,
      }
    },
    # https://www.health.nsw.gov.au/news/Pages/20200315_00.aspx
    '2020-03-15': {
      'age_groups': {
        '10-19': 2, # Investigation; investigation
        '20-29': 4, # Investigation; investigation; close contact; investigation
        '30-39': 5, # close contact; close contact; returned from US; investigation; investigation
        '40-49': 5, # close contact; investigation; investigation; investigation; returned from US
        '50-59': 3, # returned from US; returned from US; returned from Philippines
        '60-69': 3, # returned from SG; investigation; close contact
      },
      'sources': {
        'Overseas acquired': 6,
        'Locally acquired - contact of a confirmed case': 5,
        'Locally acquired - contact not identified': 0,
        'Under investigation': 11,
      }
    },
    # https://www.health.nsw.gov.au/news/Pages/20200324_00.aspx
    '2020-03-24': {
      'icu': 12,
      'ventilators': 8,
    },
    # https://www.health.nsw.gov.au/news/Pages/20200325_00.aspx
    '2020-03-25': {
      'icu': 10,
      'ventilators': 4,
    },
    # https://www.health.nsw.gov.au/news/Pages/20200326_00.aspx
    '2020-03-26': {
      'icu': 16,
      'ventilators': 10,
    },
  }

  age_groups = collections.defaultdict(lambda: 0)
  sources = collections.defaultdict(lambda: 0)
  for date in sorted(events.keys()):
    event_data = events[date]
    for k, v in event_data.get('age_groups', {}).items():
      age_groups[k] += v
    for k, v in event_data.get('sources', {}).items():
      sources[k] += v

    # If there's no date in the timeseries data, there's no data for that day,
    # and it probably doesn't super matter?
    if date in timeseries_data:
      # Only overwrite these values if they're not present
      if 'age_groups' not in timeseries_data[date]:
        timeseries_data[date]['age_groups'] = copy.deepcopy(age_groups)
      if 'sources' not in timeseries_data[date]:
        timeseries_data[date]['sources'] = copy.deepcopy(sources)

      # Always overwrite these ones - we know that they'll be missing
      if 'icu' in event_data:
        timeseries_data[date]['icu'] = event_data['icu']
      if 'ventilators' in event_data:
        timeseries_data[date]['ventilators'] = event_data['ventilators']

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
    last_value = 0
    for d in dates:
      value = timeseries_data[d].get(data_key, {}).get(k, last_value)
      munged_data[k].append(value)
      last_value = value

  return {
    'keys': keys,
    'subseries': munged_data,
  }

if __name__ == '__main__':
  main()

