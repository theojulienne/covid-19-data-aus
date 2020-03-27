#!/usr/bin/env python2

import datetime
import re
import os

import requests

# unfortunately QLD doesn't have a history of testing data. so instead, every poll, we check this page, and save it as the "status as at" date :'(
status_url = 'https://www.qld.gov.au/health/conditions/health-alerts/coronavirus-covid-19/current-status/current-status-and-contact-tracing-alerts'
response_body = requests.get(status_url).text
matches = re.findall(r'Status.*as at ([^<]*)', response_body)
if len(matches) == 1:
    status_date = matches[0]
    parsed_date = datetime.datetime.strptime(status_date, '%d %B %Y').date().isoformat()
    
    status_file = 'data_cache/qld/status-tracing/' + parsed_date + '.html'
    with open(status_file, 'wb') as f:
        f.write(response_body.encode('utf-8'))
else:
    print('WARNING: No "status as at" date was found in the QLD status page, we may be missing data')