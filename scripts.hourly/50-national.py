import collections
import datetime
import json
import os
import re

from pdfminer.converter import PDFPageAggregator
from pdfminer.pdfdocument import PDFDocument
from pdfminer.layout import LAParams
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser
import pdfminer
import requests


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
  pdf_text_data = None
  with open(filename, 'rb') as f:
    pdf_text_data = extract_pdf_text(f)

  data = collections.defaultdict(dict)
  update_time = None

  icu_values = []
  hospitalized_values = []
  top_row_test_values = []
  bottom_row_test_values = []
  top_row_test_percs = []
  bottom_row_test_percs = []

  for left_coord, bottom_coord, text in pdf_text_data:
    # If we're in the state map
    if left_coord > 600 and bottom_coord > 350:
      state = None

      # WA is the farthest left state
      if left_coord < 650:
        state = 'WA'
      # NT is pretty far left, but also high
      elif left_coord < 675 and bottom_coord > 450:
        state = 'NT'
      # SA is also pretty far left, but lower
      elif left_coord < 675 and bottom_coord > 400:
        state = 'SA'
      # Otherwise, if we're still pretty far left, this is Victoria's very
      # left-floating label
      elif left_coord < 675:
        state = 'VIC'
      # If we're over to the right, the top label is QLD
      elif bottom_coord > 425:
        state = 'QLD'
      # Then NSW
      elif bottom_coord > 400:
        state = 'NSW'
      # Then the ACT
      elif bottom_coord > 375:
        state = 'ACT'
      else:
        state = 'TAS'

      parsed = re.match(r'^(?P<total>[\d,]+)(?: \((?P<deaths>\d+)\))?$', text).groupdict()
      data[state]['total'] = parse_num(parsed['total'])
      data[state]['deaths'] = parse_num(parsed['deaths'] or '0')

    # If this is the national totals panel, skip! We get this same information
    # from state-specific numbers
    elif bottom_coord > 470:
      pass

    # If this is the current ICU cases panel
    elif bottom_coord > 300:
      # Again, we don't care about the national total callout
      if bottom_coord > 350:
        pass
      # Otherwise, append all ICU values to a list - we'll order these by their
      # left coordinates, and use this to determine the state
      else:
        icu_values.append((left_coord, parse_num(text)))

    # If this is the current hospitalized cases panel
    elif bottom_coord > 200:
      # We don't care about the national total callout
      if bottom_coord > 250:
        pass
      # Otherwise, append all hospitalized values to a list - we'll order these
      # by their left coordinates, and use this to determine the state
      else:
        hospitalized_values.append((left_coord, parse_num(text)))

    # If this is the testing panel
    elif bottom_coord > 25:
      # We don't care about the totals callouts
      if bottom_coord > 125:
        pass
      # Top row of test numbers
      elif bottom_coord > 100:
        top_row_test_values.append((left_coord, parse_num(text)))
      # Top row of test percentages
      elif bottom_coord > 75:
        top_row_test_percs.append((left_coord, parse_perc(text)))
      # Bottom row of test numbers
      elif bottom_coord > 50:
        bottom_row_test_values.append((left_coord, parse_num(text)))
      # Bottom row of test percentages
      else:
        bottom_row_test_percs.append((left_coord, parse_perc(text)))

    # If this is the "last updated" time
    elif left_coord < 30:
      update_time = datetime.datetime.strptime(text, 'Last updated %d %B %Y')

    # Otherwise it's just the info section
    else:
      pass

  flatten_and_insert_state_data(data, sorted(icu_values), 'icu')
  flatten_and_insert_state_data(data, sorted(hospitalized_values), 'hospitalized')
  flatten_and_insert_state_data(data, sorted(top_row_test_values) + sorted(bottom_row_test_values), 'tests')
  flatten_and_insert_state_data(data, sorted(top_row_test_percs) + sorted(bottom_row_test_percs), 'test_pos_perc')

  return (update_time, data)

def flatten_and_insert_state_data(data, values, value_key):
  states = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']

  if len(states) != len(values):
    raise Exception('Uh oh, missing / extra %s values!' % value_key)

  for state, (_, value) in zip(states, values):
    data[state][value_key] = value

def extract_pdf_text(file):
  parser = PDFParser(file)
  resource_manager = PDFResourceManager()
  device = PDFPageAggregator(resource_manager, laparams=LAParams())
  interpreter = PDFPageInterpreter(resource_manager, device)

  results = []
  for page in PDFPage.create_pages(PDFDocument(parser)):
    interpreter.process_page(page)
    layout = device.get_result()
    results += parse_obj(layout._objs)

  return results

def parse_obj(lt_objs):
  results = []

  for obj in lt_objs:
    # if it's a textbox, print text and location
    if isinstance(obj, pdfminer.layout.LTTextBoxHorizontal):
      if obj.get_text().strip() != '':
        results.append((obj.bbox[0], obj.bbox[1], obj.get_text().strip()))

    # if it's a container, recurse
    elif isinstance(obj, pdfminer.layout.LTFigure):
      results += parse_obj(obj._objs)

  return results

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