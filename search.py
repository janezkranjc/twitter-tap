#!/usr/bin/env python

import sys
import argparse
from time import sleep

try:
    import settings
except:
    sys.stderr.write("Please copy your __settings.py to settings.py and edit it.\n")
    sys.exit(1)

from twython import Twython

parser = argparse.ArgumentParser(description='Search Twitter and return Tweets')
parser.add_argument('-q', '--query', type=unicode, dest='query', required=True, help='A UTF-8 search query of 1,000 characters maximum, including operators. Queries may additionally be limited by complexity.')
parser.add_argument('-g', '--geocode', type=unicode, dest='geocode', help='Returns tweets by users located within a given radius of the given latitude/longitude. The location is preferentially taking from the Geotagging API, but will fall back to their Twitter profile. The parameter value is specified by "latitude,longitude,radius", where radius units must be specified as either "mi" (miles) or "km" (kilometers). Note that you cannot use the near operator via the API to geocode arbitrary locations; however you can use this geocode parameter to search near geocodes directly. A maximum of 1,000 distinct "sub-regions" will be considered when using the radius modifier. Example value: 37.781157,-122.398720,1mi')
parser.add_argument('-l', '--lang', type=unicode, dest='lang', help='Restricts tweets to the given language, given by an ISO 639-1 code. Language detection is best-effort.\nExample value: eu')

args = parser.parse_args()

query = args.query
geocode = args.geocode
lang = args.lang

if settings.ACCESS_TOKEN == '':
    sys.stdout.write("No access token found in settings. Obtaining one now...\n")
    token_getter = Twython(settings.CONSUMER_KEY,settings.CONSUMER_SECRET,oauth_version=2)
    settings.ACCESS_TOKEN = token_getter.obtain_access_token()
    sys.stdout.write("Access token: "+settings.ACCESS_TOKEN+"\n")

twitter = Twython(settings.CONSUMER_KEY, access_token=settings.ACCESS_TOKEN)

results = twitter.search(q=query,geocode=geocode,lang=lang,count=100)

"""
if results.next_results:
            p = urlparse.urlparse(results.next_results)
            results.next_results_max_id = dict(urlparse.parse_qsl(p.query))['max_id']
        for status in json['statuses']:
            results.append(Status.parse(api, status))
"""

for result in results['statuses']:
    print result['text']