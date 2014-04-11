#!/usr/bin/env python

import sys
import argparse
from time import sleep
import signal
import pymongo
import urlparse

def exit_gracefully(signal, frame):
    sys.exit(0)

signal.signal(signal.SIGINT, exit_gracefully)

try:
    import settings
except:
    sys.stderr.write("Please copy your __settings.py to settings.py and edit it.\n")
    sys.exit(1)

from twython import Twython

parser = argparse.ArgumentParser(description='Query the Twitter API and store Tweets in MongoDB.')
parser.add_argument('-q', '--query', type=unicode, dest='query', required=True, help='A UTF-8 search query of 1,000 characters maximum, including operators. Queries may additionally be limited by complexity. Information on how to construct a query is available at https://dev.twitter.com/docs/using-search')
parser.add_argument('-g', '--geocode', type=unicode, dest='geocode', help='Returns tweets by users located within a given radius of the given latitude/longitude. The location is preferentially taking from the Geotagging API, but will fall back to their Twitter profile. The parameter value is specified by "latitude,longitude,radius", where radius units must be specified as either "mi" (miles) or "km" (kilometers). Note that you cannot use the near operator via the API to geocode arbitrary locations; however you can use this geocode parameter to search near geocodes directly. A maximum of 1,000 distinct "sub-regions" will be considered when using the radius modifier. Example value: 37.781157,-122.398720,1mi')
parser.add_argument('-l', '--lang', type=unicode, dest='lang', help='Restricts tweets to the given language, given by an ISO 639-1 code. Language detection is best-effort.\nExample value: eu')
parser.add_argument('-d', '--dbname', type=unicode, dest='dbname', default='twitter', help='Database name. Defaults to \'twitter\'.')

args = parser.parse_args()

query = args.query
geocode = args.geocode
lang = args.lang
dbname = args.dbname

# here we get the access token if it is not written in the settings
if settings.ACCESS_TOKEN == '':
    sys.stdout.write("No access token found in settings. Obtaining one now...\n")
    token_getter = Twython(settings.CONSUMER_KEY,settings.CONSUMER_SECRET,oauth_version=2)
    settings.ACCESS_TOKEN = token_getter.obtain_access_token()
    sys.stdout.write("Access token: "+settings.ACCESS_TOKEN+"\n")

twitter = Twython(settings.CONSUMER_KEY, access_token=settings.ACCESS_TOKEN)

client = pymongo.MongoClient(settings.MONGODB_URI)

db = client[dbname]

queries = db.queries
tweets = db.tweets

queries.ensure_index("query",direction=pymongo.ASCENDING,unique=True)
tweets.ensure_index("id",direction=pymongo.DESCENDING,unique=True)

current_query = queries.find_one({'query':query})
if current_query:
    since_id = current_query['since_id']
else:
    since_id = None

results = twitter.search(q=query,geocode=geocode,lang=lang,count=100,since_id=since_id)

refresh_url = results['search_metadata'].get('refresh_url')
p = urlparse.urlparse(refresh_url)
since_id = dict(urlparse.parse_qsl(p.query))['since_id']
queries.update({'query':query},{"$set":{'since_id':since_id}},upsert=True)

"""
if results.next_results:
            p = urlparse.urlparse(results.next_results)
            results.next_results_max_id = dict(urlparse.parse_qsl(p.query))['max_id']
        for status in json['statuses']:
            results.append(Status.parse(api, status))
"""

for result in results['statuses']:
    print result['text']
