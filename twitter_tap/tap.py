#!/usr/bin/env python

import logging
import sys
import argparse
from time import sleep
import signal
import urlparse
from datetime import datetime
from email.utils import parsedate

def main():

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

    parser = argparse.ArgumentParser(description='Twitter acquisition pipeline: Query the Twitter API and store Tweets in MongoDB. In order to run this you must provide a query, the consumer secret and either the consumer key or the access token. Consumer key and secret can be obtained at the http://apps.twitter.com/ website, while the access token will be obtained when first connecting with the key and secret.')
    parser.add_argument('-q', '--query', type=unicode, dest='query', required=True, help='A UTF-8 search query of 1,000 characters maximum, including operators. Queries may additionally be limited by complexity. Information on how to construct a query is available at https://dev.twitter.com/docs/using-search')
    parser.add_argument('-g', '--geocode', type=unicode, dest='geocode', help='Returns tweets by users located within a given radius of the given latitude/longitude. The location is preferentially taking from the Geotagging API, but will fall back to their Twitter profile. The parameter value is specified by "latitude,longitude,radius", where radius units must be specified as either "mi" (miles) or "km" (kilometers). Note that you cannot use the near operator via the API to geocode arbitrary locations; however you can use this geocode parameter to search near geocodes directly. A maximum of 1,000 distinct "sub-regions" will be considered when using the radius modifier. Example value: 37.781157,-122.398720,1mi')
    parser.add_argument('-l', '--lang', type=unicode, dest='lang', help='Restricts tweets to the given language, given by an ISO 639-1 code. Language detection is best-effort.\nExample value: eu')
    parser.add_argument('-r', '--result-type', '--result_type', type=unicode, default='mixed', dest='result_type', choices=["mixed","recent","popular"],help='Specifies what type of search results you would prefer to receive. The current default is "mixed". Valid values include: "mixed" - Include both popular and real time results in the response. "recent" - return only the most recent results in the response. "popular" - return only the most popular results in the response.')
    parser.add_argument('-d', '--db', type=unicode, dest='dburi', default='mongodb://localhost:27017/twitter', help='MongoDB URI, example: mongodb://dbuser:dbpassword@localhost:27017/dbname Defaults to mongodb://localhost:27017/twitter')
    parser.add_argument('-qc', '--queries-collection','--queries_collection', dest='queries_collection', type=unicode, default='queries', help='The name of the collection for storing the highest since_id for each query. Default is queries.')
    parser.add_argument('-tc', '--tweets-collection','--tweets_collection', dest='tweets_collection', type=unicode, default='tweets', help='The name of the collection for storing tweets. Default is tweets.')
    parser.add_argument('-v', '--verbosity', type=unicode, dest='loglevel', default='WARN', choices=["DEBUG","INFO","WARN","ERROR","CRITICAL","FATAL"], help='The level of verbosity.')
    parser.add_argument('-w', '--wait', type=float, dest='waittime', default=2.0, help='Mandatory sleep time before executing a query. The default value is 2, which should ensure that the rate limit of 450 per 15 minutes is never reached.')
    parser.add_argument('-c', '--clean', dest='clean', action='store_true', default=False, help="Set this switch to use a clean since_id.")
    parser.add_argument('-ck', '--consumer-key', '--consumer_key', type=unicode, required=True, dest='consumer_key', help="The consumer key that you obtain when you create an app at https://apps.twitter.com/")
    parser.add_argument('-cs', '--consumer-secret', '--consumer_secret', type=unicode, dest='consumer_secret', help="The consumer secret that you obtain when you create an app at https://apps.twitter.com/")
    parser.add_argument('-at', '--access-token', '--access_token', type=unicode, dest='access_token', help="You can use consumer_key and access_token instead of consumer_key and consumer_secret. This will make authentication faster, as the token will not be fetched. The access token will be printed to the standard output when connecting with the consumer_key and consumer_secret.")

    if len(sys.argv)==1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()

    query = args.query
    geocode = args.geocode
    lang = args.lang
    loglevel = args.loglevel
    waittime = args.waittime
    clean_since_id = args.clean
    result_type = args.result_type

    CONSUMER_KEY = args.consumer_key
    CONSUMER_SECRET = args.consumer_secret
    ACCESS_TOKEN = args.access_token
    MONGODB_URI = args.dburi

    logging.basicConfig(format=FORMAT,level=logging_dict[loglevel],stream=sys.stdout)
    logger = logging.getLogger('twitter')

    if CONSUMER_SECRET is None and ACCESS_TOKEN is None:
        logger.fatal("Consumer secret or access token is required.")
        sys.exit(1)

    # here we get the access token if it is not provided with the options
    if not ACCESS_TOKEN:
        logger.warn("No access token provided in options. Obtaining one now...")
        token_getter = Twython(CONSUMER_KEY,CONSUMER_SECRET,oauth_version=2)
        ACCESS_TOKEN = token_getter.obtain_access_token()
        logger.warn("Access token: "+ACCESS_TOKEN)

    twitter = Twython(CONSUMER_KEY, access_token=ACCESS_TOKEN)

    try:
        client = pymongo.MongoClient(MONGODB_URI)
    except:
        logger.fatal("Couldn't connect to MongoDB. Please check your --db argument settings.")
        sys.exit(1)

    parsed_dburi = pymongo.uri_parser.parse_uri(MONGODB_URI)
    db = client[parsed_dburi['database']]

    queries = db[args.queries_collection]
    tweets = db[args.tweets_collection]

    queries.ensure_index([("query",pymongo.ASCENDING),("geocode",pymongo.ASCENDING),("lang",pymongo.ASCENDING)],unique=True)
    tweets.ensure_index("id",direction=pymongo.DESCENDING,unique=True)
    tweets.ensure_index([("coordinates.coordinates",pymongo.GEO2D),])

    if not clean_since_id:
        current_query = queries.find_one({'query':query,'geocode':geocode,'lang':lang})
    else:
        current_query = None
    if current_query:
        since_id = current_query['since_id']
    else:
        since_id = None

    def perform_query(**kwargs):
        while True:
            sleep(waittime)
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
        results = perform_query(q=query,geocode=geocode,lang=lang,count=100,since_id=since_id,result_type=result_type)

        refresh_url = results['search_metadata'].get('refresh_url')
        p = urlparse.urlparse(refresh_url)
        new_since_id = dict(urlparse.parse_qsl(p.query))['since_id']
        queries.update({'query':query,'geocode':geocode,'lang':lang},{"$set":{'since_id':new_since_id}},upsert=True)
        logger.debug("Rate limit for current window: "+str(twitter.get_lastfunction_header(header="x-rate-limit-remaining")))
        save_tweets(results['statuses'])

        next_results = results['search_metadata'].get('next_results')
        while next_results:
            p = urlparse.urlparse(next_results)
            next_results_max_id = dict(urlparse.parse_qsl(p.query))['max_id']
            results = perform_query(q=query,geocode=geocode,lang=lang,count=100,since_id=since_id,max_id=next_results_max_id,result_type=result_type)
            next_results = results['search_metadata'].get('next_results')
            logger.debug("Rate limit for current window: "+str(twitter.get_lastfunction_header(header="x-rate-limit-remaining")))
            save_tweets(results['statuses'])

        since_id = new_since_id

if __name__ == "__main__":
    main()