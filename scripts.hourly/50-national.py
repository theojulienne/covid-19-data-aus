#!/usr/bin/env python3

import collections
import datetime
import json
import os
import re
import traceback

from pdfminer.converter import PDFPageAggregator
from pdfminer.pdfdocument import PDFDocument
from pdfminer.layout import LAParams
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser
import pdfminer
import requests
import bs4


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

      # If the national data goes farther than the state data
      for date in sorted(data.keys()):
        # If we're missing a date in the state dates, it should be greater
        # than the state dates that we have
        if date not in state_dates:
          assert(date > max(state_dates))
          state_specific_data['timeseries_dates'].append(date)

          # Fill in the data for each key with blanks
          for k in state_specific_data['total']:
            if isinstance(state_specific_data['total'][k][-1], int):
              state_specific_data['total'][k].append(None)

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
  when = datetime.date(2020, 4, 5)
  while when <= datetime.date.today():
    curr = when
    when += datetime.timedelta(days=1)

    path = 'https://www.health.gov.au/resources/publications/coronavirus-covid-19-at-a-glance-{}'.format(curr.strftime('%-d-%B-%Y').lower())
    cache_filename = os.path.join('data_cache', 'national', os.path.basename(path) + '.html')
    try:
      data = cache_request(
        cache_filename,
        lambda: request_pdf(path),
      )
    except MissingPdfException:
      print('No pointer page for {}'.format(curr))
      continue

    # and the actual PDF from that next
    pointer_to_pdf = bs4.BeautifulSoup(data, 'html.parser')
    links = pointer_to_pdf.select('a.health-file__link')
    if len(links) == 0:
      print('No link on page for {}'.format(curr))
      continue
    link = links[0]
    href = link['href']

    cache_filename = os.path.join('data_cache', 'national', os.path.basename(href))
    try:
      data = cache_request(
        cache_filename,
        lambda: request_pdf(href),
      )
    except MissingPdfException:
      print('No PDF for {}'.format(curr))
      continue

    print(href)

def request_pdf(href):
  r = requests.get(href)

  if r.status_code == 404:
    raise MissingPdfException()
  else:
    return r.content

def parse_pdfs(path):
  data = {}

  for basename in os.listdir(path):
    if not basename.endswith('.pdf'): continue
    try:
      curr_day, curr_day_data = parse_pdf(os.path.join(path, basename))
    except Exception as e:
      print('Failed to parse {}: {}'.format(basename, e))
      traceback.print_exc()
    else:
      data[curr_day.strftime('%Y-%m-%d')] = curr_day_data

  return data

def parse_pdf(filename):
  file_date = datetime.datetime(2020, 4, 1)

  if 'coronavirus-covid-19-at-a-glance' in filename:
    if 'coronavirus-covid-19-at-a-glance-coronavirus-covid-19-at-a-glance-infographic' in filename:
      pass
    elif '-coronavirus-covid-19' in filename:
      prefix, _ = filename.split('-coronavirus-covid-19', 1)
      file_date = datetime.datetime.strptime(prefix.replace('_0', '').replace('_1', ''), 'data_cache/national/coronavirus-covid-19-at-a-glance-%d-%B-%Y')
    else:
      file_date = datetime.datetime.strptime(filename.replace('_0', '').replace('_1', ''), 'data_cache/national/coronavirus-covid-19-at-a-glance-%d-%B-%Y.pdf')

  # print "File date", file_date

  print('Processing: {}'.format(filename))
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

  OLD_WIDTH = 841.920
  OLD_HEIGHT = 595.320

  for rel_left_coord, rel_bottom_coord, text in pdf_text_data:
    left_coord = rel_left_coord * OLD_WIDTH
    bottom_coord = rel_bottom_coord * OLD_HEIGHT

    # If someone stuck a new line in and copied the value twice by accident,
    # lets just pretend that didn't happen
    if '\n' in text:
      if len(set([t.strip() for t in text.split('\n')])) == 1:
        text = t.split('\n')[0].strip()

    # if file_date >= datetime.datetime(2020, 5, 26) and file_date <= datetime.datetime(2020, 6, 3):
    #   print file_date.strftime('%Y-%m-%d'), left_coord, bottom_coord, text

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

      SUMMARY_REGEX = r'^(?P<total>[\d,]+)(?: \n?\((?P<deaths>\d+)\))?$'
      match = re.match(SUMMARY_REGEX, text)
      if not match:
        # There really shouldn't be a newline here! But if there is, because
        # Reasons....
        for line in text.split('\n'):
          m = re.match(SUMMARY_REGEX, line)
          if m:
            match = m
      if match:
        parsed = match.groupdict()
        data[state]['total'] = parse_num(parsed['total'])
        data[state]['deaths'] = parse_num(parsed['deaths'] or '0')
      else:
        raise Exception('Uh oh! Couldn\'t parse %s' % repr(text))

    # If this is the national totals panel, skip! We get this same information
    # from state-specific numbers
    elif bottom_coord > 470:
      pass

    # If this is the current ICU cases panel
    elif bottom_coord > 300:
      # Again, we don't care about the national total callout
      if bottom_coord > 350:
        pass
      # end of the table, after this is the age care table
      elif left_coord > 250:
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
      # end of the table, after this is the age care table
      elif left_coord > 240:
        pass
      # Otherwise, append all hospitalized values to a list - we'll order these
      # by their left coordinates, and use this to determine the state
      else:
        hospitalized_values.append((left_coord, parse_num(text)))

    # If this is the testing panel
    elif bottom_coord > 25 or (file_date >= datetime.datetime(2020, 5, 26) and bottom_coord > 12):
      # We don't care about the totals callouts
      if bottom_coord > 125:
        pass
      # end of the table, after this is the age care table
      elif left_coord > 250:
        pass
      # Top row of test numbers
      elif bottom_coord > 100 or (file_date >= datetime.datetime(2020, 5, 26) and bottom_coord > 90):
        top_row_test_values.append((left_coord, parse_num(text)))
      # Top row of test percentages
      elif bottom_coord > 75 or (file_date >= datetime.datetime(2020, 5, 26) and bottom_coord > 67):
        top_row_test_percs.append((left_coord, parse_perc(text)))
      # Bottom row of test numbers
      elif bottom_coord > 50 or (file_date >= datetime.datetime(2020, 5, 26) and bottom_coord > 37):
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

  # This is stupid. However, starting with 2020-04-17, there's an extra,
  # seemingly hidden "11" for Queensland in these values. I assume someone sent
  # a text field to the back, without realizing it. Remove it.
  if update_time >= datetime.datetime(year=2020, month=4, day=17) and (118.22, 11) in icu_values:
    icu_values.remove((118.22, 11))

  flatten_and_insert_state_data(data, sorted(icu_values), 'icu')
  flatten_and_insert_state_data(data, sorted(hospitalized_values), 'hospitalized')
  flatten_and_insert_state_data(data, sorted(top_row_test_values) + sorted(bottom_row_test_values), 'tests')
  flatten_and_insert_state_data(data, sorted(top_row_test_percs) + sorted(bottom_row_test_percs), 'test_pos_perc')

  return (update_time, data)

def flatten_and_insert_state_data(data, values, value_key):
  states = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']

  if len(states) != len(values):
    # print values
    raise Exception('Uh oh, missing / extra %s values! %s' % (value_key, values))

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
    width = layout.bbox[2]
    # we expect the same w/h ratio as the original PDF size
    height = layout.bbox[2] / 841.92 * 595.32
    height_surplus = (layout.bbox[3] - height)
    # weird bars on top/bottom which we'll 'offset' half away
    print('height surplus: {}'.format(height_surplus))
    results += parse_obj(layout._objs, width, height, height_surplus/2)

  return results

def parse_obj(lt_objs, width, height, y_offset):
  results = []

  for obj in lt_objs:
    # if it's a textbox, print text and location
    if isinstance(obj, pdfminer.layout.LTTextBoxHorizontal):
      if obj.get_text().strip() != '':
        results.append((obj.bbox[0] / width, (obj.bbox[1] - y_offset) / height, obj.get_text().strip()))

    # if it's a container, recurse
    elif isinstance(obj, pdfminer.layout.LTFigure):
      results += parse_obj(obj._objs, width, height, y_offset)

  return results

def cache_request(cache_filename, request, force_cache=False):
  if os.path.exists(cache_filename) or force_cache:
    with open(cache_filename, 'rb') as f:
      ret = f.read()
      # don't allow "pretend 404" pages
      if 'We have publications on different health topics for you to access' not in str(ret):
        return ret

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