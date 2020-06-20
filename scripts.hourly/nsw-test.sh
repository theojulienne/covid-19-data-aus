#!/bin/bash

echo 'Hello, testing!'
echo 'Fetching IP'
curl -iv http://icanhazip.com/

echo 'Fetching page'
curl -iv https://www.health.nsw.gov.au/news/Pages/2020-nsw-health.aspx

echo 'Exiting'
exit 1