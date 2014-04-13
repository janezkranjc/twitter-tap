#!/usr/bin/env python

import logging
import sys
import argparse
from time import sleep
import signal
import urlparse
from datetime import datetime
from email.utils import parsedate

FORMAT = '[%(asctime)-15s] %(levelname)s: %(message)s'

try:
    import pymongo
    from twython.exceptions import TwythonRateLimitError, TwythonError
    from twython import Twython
except ImportError:
    logging.basicConfig(format=FORMAT)
    logger = logging.getLogger('twitter')
    logger.fatal("Could not import, try running pip install -r requirements.txt")
    sys.exit(1)



def parse_datetime(string):
    return datetime(*(parsedate(string)[:6]))

logging_dict = {
    "DEBUG":logging.DEBUG,
    "INFO":logging.INFO,
    "WARN":logging.WARN,
    "ERROR":logging.ERROR,
    "CRITICAL":logging.CRITICAL,
    "FATAL":logging.FATAL,
}


def exit_gracefully(signal, frame):
    logger.warn("Shutdown signal received! Shutting down.")
    sys.exit(0)

signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)

try:
    import settings
except ImportError:
    logging.basicConfig(format=FORMAT)
    logger = logging.getLogger('twitter')
    logger.fatal("Please copy your __settings.py to settings.py and edit it.")
    sys.exit(1)

parser = argparse.ArgumentParser(description='Twitter acquisition pipeline: Query the Twitter API and store Tweets in MongoDB.')
parser.add_argument('-q', '--query', type=unicode, dest='query', required=True, help='A UTF-8 search query of 1,000 characters maximum, including operators. Queries may additionally be limited by complexity. Information on how to construct a query is available at https://dev.twitter.com/docs/using-search')
parser.add_argument('-g', '--geocode', type=unicode, dest='geocode', help='Returns tweets by users located within a given radius of the given latitude/longitude. The location is preferentially taking from the Geotagging API, but will fall back to their Twitter profile. The parameter value is specified by "latitude,longitude,radius", where radius units must be specified as either "mi" (miles) or "km" (kilometers). Note that you cannot use the near operator via the API to geocode arbitrary locations; however you can use this geocode parameter to search near geocodes directly. A maximum of 1,000 distinct "sub-regions" will be considered when using the radius modifier. Example value: 37.781157,-122.398720,1mi')
parser.add_argument('-l', '--lang', type=unicode, dest='lang', help='Restricts tweets to the given language, given by an ISO 639-1 code. Language detection is best-effort.\nExample value: eu')
parser.add_argument('-d', '--dbname', type=unicode, dest='dbname', default='twitter', help='Database name. Defaults to \'twitter\'.')
parser.add_argument('-v', '--verbosity', type=unicode, dest='loglevel', default='WARN', choices=["DEBUG","INFO","WARN","ERROR","CRITICAL","FATAL"], help='The level of verbosity.')

args = parser.parse_args()

query = args.query
geocode = args.geocode
lang = args.lang
dbname = args.dbname
loglevel = args.loglevel

logging.basicConfig(format=FORMAT,level=logging_dict[loglevel],stream=sys.stdout)
logger = logging.getLogger('twitter')

# here we get the access token if it is not written in the settings
if settings.ACCESS_TOKEN == '':
    logger.warn("No access token found in settings. Obtaining one now...")
    token_getter = Twython(settings.CONSUMER_KEY,settings.CONSUMER_SECRET,oauth_version=2)
    settings.ACCESS_TOKEN = token_getter.obtain_access_token()
    logger.warn("Access token: "+settings.ACCESS_TOKEN)

twitter = Twython(settings.CONSUMER_KEY, access_token=settings.ACCESS_TOKEN)

try:
    client = pymongo.MongoClient(settings.MONGODB_URI)
except:
    logger.fatal("Couldn't connect to MongoDB. Please check your settings.")
    sys.exit(1)

db = client[dbname]

queries = db.queries
tweets = db.tweets

queries.ensure_index([("query",pymongo.ASCENDING),("geocode",pymongo.ASCENDING),("lang",pymongo.ASCENDING)],unique=True)
tweets.ensure_index("id",direction=pymongo.DESCENDING,unique=True)

current_query = queries.find_one({'query':query,'geocode':geocode,'lang':lang})
if current_query:
    since_id = current_query['since_id']
else:
    since_id = None

def perform_query(**kwargs):
    while True:
        sleep(1.5)
        try:
            results = twitter.search(**kwargs)
        except TwythonRateLimitError:
            logger.warn("Rate limit reached, taking a break for a minute...\n")
            sleep(60)
            continue
        except TwythonError, err:
            logger.error("Some other error occured, taking a break for half a minute: "+str(err))
            sleep(30)
            continue
        return results

def save_tweets(statuses):
    for status in statuses:
        status['created_at']=parse_datetime(status['created_at'])
        tweets.update({'id':status['id']},status,upsert=True)
    if len(statuses)==0:
        logger.debug("No new tweets. Taking a break for 10 seconds...")
        sleep(10)
    else:
        logger.debug("Received "+str(len(statuses))+" tweets.")

logger.info("Starting...")

while True:
    results = perform_query(q=query,geocode=geocode,lang=lang,count=100,since_id=since_id)

    refresh_url = results['search_metadata'].get('refresh_url')
    p = urlparse.urlparse(refresh_url)
    new_since_id = dict(urlparse.parse_qsl(p.query))['since_id']
    queries.update({'query':query,'geocode':geocode,'lang':lang},{"$set":{'since_id':new_since_id}},upsert=True)
    logger.debug("Rate limit for current window: "+str(results['headers']['x-rate-limit-remaining']))
    save_tweets(results['statuses'])

    next_results = results['search_metadata'].get('next_results')
    while next_results:
        p = urlparse.urlparse(next_results)
        next_results_max_id = dict(urlparse.parse_qsl(p.query))['max_id']
        results = perform_query(q=query,geocode=geocode,lang=lang,count=100,since_id=since_id,max_id=next_results_max_id)
        next_results = results['search_metadata'].get('next_results')
        logger.debug("Rate limit for current window: "+str(results['headers']['x-rate-limit-remaining']))
        save_tweets(results['statuses'])


    since_id = new_since_id