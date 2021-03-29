#!/usr/bin/env python3

import datetime
import re
import os

import bs4
import requests

# press releases each day show the updates, unfortunately the URLs are human created so not stable.
# we'll pull all press releases containing "COVID" and use that as the marker to cache (and later parse) the page.
releases_url = 'https://ww2.health.wa.gov.au/News/Media-releases-listing-page'
post_list_soup = bs4.BeautifulSoup(requests.get(releases_url).text, 'html.parser')

for post_link in post_list_soup.select('div#contentArea ul li a'):
  if not 'COVID' in post_link.text: continue

  uri = post_link.attrs['href']
  if not uri.startswith('/'): continue # skip non-stats offsite links
  url = 'https://ww2.health.wa.gov.au' + uri
  cache_filename = 'data_cache/wa/'+uri.replace('/', '_')+'.html'
  if not os.path.exists(cache_filename):
    response_body = requests.get(url).text
    with open(cache_filename, 'wb') as f:
      f.write(response_body.encode('utf-8'))

## TODO: parse data from the available cached files in os.listdir('data_cache/wa')
