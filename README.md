# Twitter Tap #

Collect tweets to a mongoDB using the twitter search API. If you want to save tweets using a graphical interface try [Tweetset](http://github.com/janezkranjc/tweetset).

# About Twitter Tap #

Twitter Tap is a python tool that connects to the Twitter API and issues calls to the **search endpoint** or the **streaming API** using a query that the user has entered.

## Using the search API ##

If using the search API, the tool follows all the **next_results** links (with the corresponding **max_id**) so that all results are collected. When all the **next_results** links are exhausted the query is repeated using the **since_id** of the latest tweet from the results of the first query and follows all the **next_results** links again. The latest **since_id** is also stored in the database for each distinct query (query, geolocation, language), so that when the tool is restarted you will still only receive unique tweets.

Tweets are stored into a mongoDB, which has a unique index on the Tweet ID so that there is no duplication of data if more than 1 query is executed simultaneously.

There is an arbitrary wait time before each API call so that the rate limit is not reached. The default value of 2 seconds makes sure that there are no more than 450 requests per 15 minutes as is the rate limit of the search endpoint for authenticating with the app (not the user).

The tool can be run from the command line or be run as a daemon using supervisor (recommended). A sample supervisord.conf script is included with the tool.

## Using the streaming API ##

You can also use the streaming API to store tweets. Storing tweets from the streaming API is much more straightforward, but you can only get the tweets you are currently streaming. Using the search API will also get you tweets from a couple of days back. Check out the documentation so that you can see what sorts of queries you can make with the search API vs the streaming API.

## Loading data from CSV ##

You can also load your search and streaming keywords using CSV files. Twitter recommends that you only pass 10 keywords and operators to the search API, so create your CSV files in increments of 10. When loading from file with the search API, the OR operator is applied. See the examples and arguments below. Add your files to

```bash
twitter-tap/data
```

# Installation #

Install Twitter Tap using [pip](http://www.pip-installer.org/)

```bash
pip install twitter-tap
```

Or, if you want the current code

```bash
git clone git://github.com/janezkranjc/twitter-tap.git
cd twitter-tap
python setup.py install
```

# Before you start #

Please follow this link https://apps.twitter.com/ and create a twitter app. You will need the consumer key and consumer secret to access the twitter API.

# Using twitter tap #

Run Twitter Tap in the command line like this.

```bash
tap
```

### Show help text ###
```bash
tap -h
```

```bash
tap search -h
```

```bash
tap stream -h
```

### Executing a query with the search API ###

To execute a query you must provide a **query**, the **consumer secret** and either the **consumer key** or the access token. Consumer key and secret can be obtained at the http://apps.twitter.com/ website, while the access token will be obtained when first connecting with the key and secret.

```bash
tap search --consumer-key CONSUMERKEY --consumer-secret CONSUMERSECRET -q "miley cyrus" -v DEBUG
```

```bash
tap search --consumer-key CONSUMERKEY --consumer-secret CONSUMERSECRET -ql "miley_fans" -v DEBUG
```

Search options:

| Option                 | Description                                                                                                                                                                                                                 |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| --query                | A UTF-8 search query of 1,000 characters maximum, including operators. Queries may additionally be limited by complexity. Information on how to construct a query is available at https://dev.twitter.com/docs/using-search |
| --geocode              | Returns tweets by users located within a given radius of the given latitude/longitude. The location is preferentially taking from the Geotagging API, but will fall back to their Twitter profile. The parameter value is specified by "latitude,longitude,radius", where radius units must be specified as either "mi" (miles) or "km" (kilometers). Note that you cannot use the near operator via the API to geocode arbitrary locations; however you can use this geocode parameter to search near geocodes directly. A maximum of 1,000 distinct "sub-regions" will be considered when using the radius modifier. Example value: 37.781157,-122.398720,1mi |
| --lang                 | Restricts tweets to the given language, given by an ISO 639-1 code. Language detection is best-effort. Example value: eu |
| --result-type          | Specifies what type of search results you would prefer to receive. The current default is "mixed". Valid values include: "mixed" - Include both popular and real time results in the response. "recent" - return only the most recent results in the response. "popular" - return only the most popular results in the response. |
| --wait                 | Mandatory sleep time before executing a query. The default value is 2, which should ensure that the rate limit of 450 per 15 minutes is never reached. |
| --clean                | Set this switch to use a clean since_id. |
| --query-load           | Load query terms from filename. Loads csv files, just pass in a filename without the extension|
| --consumer-key         | The consumer key that you obtain when you create an app at https://apps.twitter.com/ |
| --consumer-secret      | The consumer secret that you obtain when you create an app at https://apps.twitter.com/ |
| --access-token         | You can use consumer_key and access_token instead of consumer_key and consumer_secret. This will make authentication faster, as the token will not be fetched. The access token will be printed to the standard output when connecting with the consumer_key and consumer_secret. |
| --db                   | MongoDB URI, example: mongodb://dbuser:dbpassword@localhost:27017/dbname Defaults to mongodb://localhost:27017/twitter |
| --queries-collection   | The name of the collection for storing the highest since_id for each query. Default is queries. |
| --tweets-collection    | The name of the collection for storing tweets. Default is tweets. |
| --verbosity            | The level of verbosity. (DEBUG, INFO, WARN, ERROR, CRITICAL, FATAL) |


### Executing a query with the streaming API ###

To execute a query you must provide a **query**, the **consumer secret** and either the **consumer key** or the access token. Consumer key and secret can be obtained at the http://apps.twitter.com/ website, while the access token will be obtained when first connecting with the key and secret.

```bash
tap stream --consumer-key CONSUMERKEY --consumer-secret CONSUMERSECRET --access-token ACCESSTOKEN --access-token-secret ACCESSTOKENSECRET --track "miley cyrus" -v DEBUG
```

```bash
tap stream --consumer-key CONSUMERKEY --consumer-secret CONSUMERSECRET --access-token ACCESSTOKEN --access-token-secret ACCESSTOKENSECRET --track "miley cyrus" --follow-load "miley_fans" -v DEBUG
```

Streaming options:

| Option | Description
|--------|--------------------|
| --db   | MongoDB URI, example: mongodb://dbuser:dbpassword@localhost:27017/dbname Defaults to mongodb://localhost:27017/twitter |
| --tweets-collection | The name of the collection for storing tweets. Default is tweets. |
| --follow | A comma separated list of user IDs, indicating the users to return statuses for in the stream. More information at https://dev.twitter.com/docs/streaming-apis/parameters#follow |
| --track | A comma separated list of keywords or phrases to track. Phrases of keywords are specified by a comma-separated list. More information at https://dev.twitter.com/docs/streaming-apis/parameters#track |
| --locations | A comma-separated list of longitude,latitude pairs specifying a set of bounding boxes to filter Tweets by. On geolocated Tweets falling within the requested bounding boxes will be included—unlike the Search API, the user\'s location field is not used to filter tweets. Each bounding box should be specified as a pair of longitude and latitude pairs, with the southwest corner of the bounding box coming first. For example: "-122.75,36.8,-121.75,37.8" will track all tweets from San Francisco. NOTE: Bounding boxes do not act as filters for other filter parameters. More information at https://dev.twitter.com/docs/streaming-apis/parameters#locations |
| --track-load | Specify a filename to load and append terms from. Loads csv files, just pass in a filename without the extension |
| --follow-load | Specify a filename to load and append account IDs from. Loads csv files, just pass in a filename without the extension |
| --firehose | Use this option to receive all public tweets if there are no keywords, users or locations to track. This requires special permission from Twitter. Otherwise a sample of 1% of tweets will be returned. |
| --consumer-key | The consumer key that you obtain when you create an app at https://apps.twitter.com/ |
| --consumer-secret | The consumer secret that you obtain when you create an app at https://apps.twitter.com/ |
| --access-token | You can generate your user access token at http://apps.twitter.com by clicking 'Create my access token'. |
| --access-token-secret | You can generate your user access token secret at http://apps.twitter.com by clicking 'Create my access token'. |
| --verbosity | The level of verbosity. (DEBUG, INFO, WARN, ERROR, CRITICAL, FATAL) |

### Where are the tweets stored ###

The tweets are stored in the mongoDB in a collection called **tweets**. This can be changed using the --tweets-collection option. There is also a collection for saving the highest since_id for queries, which is **queries** by default (can be changed using the --queries-collection option).

# Running as a daemon #

To run Tap as a daemon you are encouraged to use supervisor. (Doesn't work natively under windows. You should use cygwin.)

Here is a sample supervisord.conf file for running tap

```bash
; Sample supervisor config file for daemonizing the twitter search to mongodb software

[inet_http_server]          ; inet (TCP) server disabled by default
port=127.0.0.1:9001         ; (ip_address:port specifier, *:port for all iface)
username=manorastroman      ; (default is no username (open server))
password=kingofthedragonmen ; (default is no password (open server))

[supervisord]
stopsignal=INT
logfile=supervisord.log      ; (main log file;default $CWD/supervisord.log)
logfile_maxbytes=50MB        ; (max main logfile bytes b4 rotation;default 50MB)
logfile_backups=10           ; (num of main logfile rotation backups;default 10)
loglevel=info                ; (log level;default info; others: debug,warn,trace)
pidfile=supervisord.pid      ; (supervisord pidfile;default supervisord.pid)
nodaemon=false               ; (start in foreground if true;default false)
minfds=1024                  ; (min. avail startup file descriptors;default 1024)
minprocs=200                 ; (min. avail process descriptors;default 200)

[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

[supervisorctl]
serverurl=http://127.0.0.1:9001 ; use an http:// url to specify an inet socket
username=manorastroman       ; should be same as http_username if set
password=kingofthedragonmen  ; should be same as http_password if set

[program:tapsearch]
command=tap search --consumer-key CONSUMERKEY --consumer-secret CONSUMERSECRET -q "miley cyrus" -v DEBUG
stdout_logfile=tap_search.log
stderr_logfile=tap_err.log
autostart=true
autorestart=true
startsecs=10
stopwaitsecs=10

[program:tapstream]
command=tap stream --consumer-key CONSUMERKEY --consumer-secret CONSUMERSECRET --access-token ACCESSTOKEN --access-token-secret ACCESSTOKENSECRET --track "miley cyrus" -v DEBUG
stdout_logfile=tap_stream.log
stderr_logfile=tap_stream_err.log
autostart=true
autorestart=true
startsecs=10
stopwaitsecs=10

; If you want to use supervisor while loading the keywords from a file you must set the directory as follows

[program:tapstream]
command=tap stream --consumer-key CONSUMERKEY --consumer-secret CONSUMERSECRET --access-token ACCESSTOKEN --access-token-secret ACCESSTOKENSECRET --track-load "miley_fans" -v DEBUG
stdout_logfile=tap_stream.log
stderr_logfile=tap_stream_err.log
autostart=true
autorestart=true
startsecs=10
stopwaitsecs=10
directory=twitter_tap/

```

Afterwards you can start the daemon like this (you must be in the same folder as supervisord.conf or your supervisord.conf must be /etc/)

```bash
supervisord
```

Open your browser to http://127.0.0.1:9001 to see the status of the daemon.
By default the username is manorastroman and the password kingofthedragonmen.

Alternatively you can see the status like this

```bash
supervisorctl status
```

Or see the tail of the logs (log file locations can be setup in supervisord.conf)

```bash
supervisorctl tail tapsearch
```

```bash
supervisorctl tail tapstream
```

Whenever you feel like shutting it down

```bash
supervisorctl shutdown
```

# Useful links #

- **TweetSet** (for saving tweets to a relational database using a web interface) https://github.com/janezkranjc/tweetset
- **MongoDB** https://www.mongodb.org/
- **Twitter developers** https://dev.twitter.com/
- **Supervisor** http://supervisord.org/

# Changes #

v2.0.4:

- Added support for loading keywords from csv files.

v2.0.3:

- Added support for Python3.

v2.0.2:

- More informative errors on saving streaming tweets.

v2.0.1:

- Fixed a bug where search results would fail to retreive tweets due to invalid since_id. The since_id is now manually computed.

v2.0.0:

- This version now uses two commands - **search** and **stream**, to use either with the search API or the streaming API (on version 1.1.0 you could only use the search API).

v1.1.0:

-  Added two options for changing the default collection names for queries and tweets.

v1.0.0:

-  This version no longer reuqires a separate settings.py file as all options can be entered as command line arguments

v0.1.0:

-  Alpha release - this version needs a settings.py to enter credentials.
