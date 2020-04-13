import collections
import datetime
import io
import json
import os
import re

from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
import requests

EARLY_FORMAT = [
  ('state_case', 'WA'),
  ('state_case', 'NT'),
  ('state_case', 'SA'),
  ('state_case', 'QLD'),
  ('state_case', 'NSW'),
  ('state_case', 'ACT'),
  ('state_case', 'TAS'),
  ('state_case', 'VIC'),
  ('total', 'count'),
  ('total', 'deaths'),
  ('total', 'recovered'),
  ('icu', 'total'),
  ('icu', 'ACT'),
  ('icu', 'NSW'),
  ('icu', 'NT'),
  ('icu', 'QLD'),
  ('icu', 'SA'),
  ('icu', 'TAS'),
  ('icu', 'VIC'),
  ('icu', 'WA'),
  ('hospitalized', 'total'),
  ('hospitalized', 'ACT'),
  ('hospitalized', 'NSW'),
  ('hospitalized', 'NT'),
  ('hospitalized', 'QLD'),
  ('hospitalized', 'SA'),
  ('hospitalized', 'TAS'),
  ('hospitalized', 'VIC'),
  ('hospitalized', 'WA'),
  ('tests', 'total'),
  ('test_pos_perc', 'total'),
  ('tests', 'ACT'),
  ('tests', 'NSW'),
  ('tests', 'NT'),
  ('tests', 'QLD'),
  ('test_pos_perc', 'ACT'),
  ('test_pos_perc', 'NSW'),
  ('test_pos_perc', 'NT'),
  ('test_pos_perc', 'QLD'),
  ('tests', 'SA'),
  ('tests', 'TAS'),
  ('tests', 'VIC'),
  ('tests', 'WA'),
  ('test_pos_perc', 'SA'),
  ('test_pos_perc', 'TAS'),
  ('test_pos_perc', 'VIC'),
  ('test_pos_perc', 'WA'),
  ('update_time', None),
  ('info', None),
]

FORMAT = [
  ('state_case', 'WA'),
  ('state_case', 'NT'),
  ('state_case', 'QLD'),
  ('state_case', 'SA'),
  ('state_case', 'VIC'),
  ('state_case', 'NSW'),
  ('state_case', 'ACT'),
  ('state_case', 'TAS'),
  ('total', 'count'),
  ('total', 'deaths'),
  ('total', 'recovered'),
  ('icu', 'total'),
  ('icu', 'ACT'),
  ('icu', 'NSW'),
  ('icu', 'NT'),
  ('icu', 'QLD'),
  ('icu', 'SA'),
  ('icu', 'TAS'),
  ('icu', 'VIC'),
  ('icu', 'WA'),
  ('hospitalized', 'total'),
  ('hospitalized', 'ACT'),
  ('hospitalized', 'NSW'),
  ('hospitalized', 'NT'),
  ('hospitalized', 'QLD'),
  ('hospitalized', 'SA'),
  ('hospitalized', 'TAS'),
  ('hospitalized', 'VIC'),
  ('hospitalized', 'WA'),
  ('tests', 'total'),
  ('test_pos_perc', 'total'),
  ('tests', 'ACT'),
  ('test_pos_perc', 'ACT'),
  ('tests', 'NSW'),
  ('tests', 'NT'),
  ('tests', 'QLD'),
  ('test_pos_perc', 'NSW'),
  ('test_pos_perc', 'NT'),
  ('test_pos_perc', 'QLD'),
  ('tests', 'SA'),
  ('tests', 'TAS'),
  ('tests', 'VIC'),
  ('tests', 'WA'),
  ('test_pos_perc', 'SA'),
  ('test_pos_perc', 'TAS'),
  ('test_pos_perc', 'VIC'),
  ('test_pos_perc', 'WA'),
  ('update_time', None),
  ('info', None),
]

class MissingPdfException(Exception):
  pass


def main():
  # Pre-fetch all the PDFs we'll need to use (or read from cache)
  get_pdfs()
  # Parse each of the PDFs, so that we have the national view of the data
  data = parse_pdfs(os.path.join('data_cache', 'national'))

  for state in ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']:
    state_specific_data_file = os.path.join('by_state_partial', '%s.json' % state.lower())

    if os.path.exists(state_specific_data_file):
      with open(state_specific_data_file, 'rb') as f:
        state_specific_data = json.load(f)

      # Update the timeseries data accordingly
      state_dates = state_specific_data['timeseries_dates']

      # null-fill data series that are missing
      for k in ['confirmed', 'tested', 'deaths', 'current_hospitalized', 'current_icu']:
        if k not in state_specific_data['total']:
          state_specific_data['total'][k] = [None for s in state_dates]

      for date in sorted(data.keys()):
        state_index = state_dates.index(date)

        if state_specific_data['total']['confirmed'][state_index] is None:
          state_specific_data['total']['confirmed'][state_index] = data[date][state]['total']

        if state_specific_data['total']['deaths'][state_index] is None:
          state_specific_data['total']['deaths'][state_index] = data[date][state]['deaths']

        if state_specific_data['total']['tested'][state_index] is None:
          state_specific_data['total']['tested'][state_index] = data[date][state]['tests']

        if state_specific_data['total']['current_hospitalized'][state_index] is None:
          state_specific_data['total']['current_hospitalized'][state_index] = data[date][state]['hospitalized']

        if state_specific_data['total']['current_icu'][state_index] is None:
          state_specific_data['total']['current_icu'][state_index] = data[date][state]['icu']

    # We have no state-specific data, so just use the national data raw
    else:
      dates = sorted(data.keys())

      state_specific_data = {
        'timeseries_dates': dates,
        'total': {
          'confirmed': [data[d][state]['total'] for d in dates],
          'deaths': [data[d][state]['deaths'] for d in dates],
          'tested': [data[d][state]['tests'] for d in dates],
          'current_hospitalized': [data[d][state]['hospitalized'] for d in dates],
          'current_icu': [data[d][state]['icu'] for d in dates],
        },
      }

    with open(os.path.join('by_state', '%s.json' % state.lower()), 'w') as f:
      json.dump(state_specific_data, f, indent=2, sort_keys=True)

def get_pdfs():
  # There's actually a 0 pdf, but it's from the same day as 1, so we skip it
  i = 1
  month = 4
  while True:
    # If this scraper survives past June, we can update this
    if month > 6:
      break

    path = 'https://www.health.gov.au/sites/default/files/documents/2020/%s/coronavirus-covid-19-at-a-glance-coronavirus-covid-19-at-a-glance-infographic_%d.pdf' % (str(month).zfill(2), i)
    cache_filename = os.path.join('data_cache', 'national', os.path.basename(path))

    try:
      cache_request(
        cache_filename,
        lambda: request_pdf(path),
      )
      i += 1

    except MissingPdfException:
      month += 1

def request_pdf(href):
  r = requests.get(href)

  if r.status_code == 404:
    raise MissingPdfException()
  else:
    return r.content

def parse_pdfs(path):
  data = {}

  for basename in os.listdir(path):
    curr_day, curr_day_data = parse_pdf(os.path.join(path, basename))
    data[curr_day.strftime('%Y-%m-%d')] = curr_day_data

  return data

def parse_pdf(filename):
  print filename

  pdf_text = None
  with open(filename, 'rb') as f:
    pdf_text = extract_pdf_text(f)

  lines = [l.strip() for l in pdf_text.split('\n') if l.strip() != '']

  # I'm sad about this too, but I'm hoping that now that we've explicitly added
  # deaths to every state, we'll stop mucking with the layout of the state map
  infographic_num = int(filename.split('.')[-2].split('_')[-1])

  if infographic_num <= 6:
    fmt = EARLY_FORMAT
  else:
    fmt = FORMAT

  if len(lines) != len(fmt):
    raise Exception('Uh oh, the format has changed')

  data = collections.defaultdict(dict)
  update_time = None

  for ((format_type, format_detail), line) in zip(fmt, lines):
    if format_type == 'state_case':
      parsed = re.match(r'^(?P<total>[\d,]+)(?: \((?P<deaths>\d+)\))?$', line).groupdict()
      data[format_detail]['total'] = parse_num(parsed['total'])
      data[format_detail]['deaths'] = parse_num(parsed['deaths'] or '0')
    elif format_type == 'total':
      pass
    elif format_type in ['icu', 'hospitalized', 'tests']:
      if format_detail != 'total':
        data[format_detail][format_type] = parse_num(line)
    elif format_type == 'test_pos_perc':
      if format_detail != 'total':
        data[format_detail]['test_pos_perc'] = parse_perc(line)
    elif format_type == 'update_time':
      update_time = datetime.datetime.strptime(line, 'Last updated %d %B %Y')
    elif format_type == 'info':
      pass
    else:
      raise Exception('Unknown format type %s' % format_type)

  return (update_time, data)


def extract_pdf_text(file):
  with io.BytesIO() as return_string:
    resource_manager = PDFResourceManager()

    device = TextConverter(resource_manager, return_string, laparams=LAParams())
    interpreter = PDFPageInterpreter(resource_manager, device)

    for page in PDFPage.get_pages(file, set([0])):
      interpreter.process_page(page)
      import code
      code.interact(local=locals())

    device.close()

    return return_string.getvalue()

def cache_request(cache_filename, request, force_cache=False):
  if os.path.exists(cache_filename) or force_cache:
    with open(cache_filename, 'rb') as f:
      return f.read()
  else:
    result = request()
    with open(cache_filename, 'wb') as f:
      f.write(result)
    return result

def parse_perc(perc):
  return float(perc.replace('%', '')) / 100.0

def parse_num(num):
  return int(num.replace(',', ''))

if __name__ == '__main__':
  main()