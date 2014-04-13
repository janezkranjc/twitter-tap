# Twitter Tap #

Collect tweets to a mongoDB using the twitter search API.

# Getting started #
## Prerequisites ##

- python >= 2.7
- pip
- mongodb
- virtualenvwrapper or virutalenvwrapper-win if on windows (optional but highly recommended)

## Before you start ##
Please follow this link https://apps.twitter.com/ and create a twitter app. You will need the consumer key and consumer secret to access the twitter API.

## Installation ##
### Creating the environment ###
Create a virtual python environment for the project.
If you're not using virtualenv or virtualenvwrapper you may skip this step.

#### For virtualenvwrapper ####
```bash
mkvirtualenv --no-site-packages twitter-tap
```

#### For virtualenv ####
```bash
virtualenv --no-site-packages twitter-tap
cd twitter-tap
source bin/activate
```

### Clone the code ###

```bash
git clone git@github.com:janezkranjc/twitter-tap.git
```

### Install requirements ###

```bash
cd twitter-tap
pip install -r requirements.txt
```

### Copy and edit the settings file ###
```bash
cp __settings.py settings.py
vi settings.py
```

## Show help text ##
```bash
python tap.py -h
```

## Executing a query ##
```bash
python tap.py -q "miley cyrus" -v DEBUG
```

# Running as a daemon #

To run Tap as a daemon you are encouraged to use supervisor.

The supervisord.conf file is there to serve as a sample configuration file for supervisor. You can use it if you find it sufficient. Just edit it to change the query.

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
supervisorctl tail tap
```

Whenever you feel like shutting it down

```bash
supervisorctl shutdown
```

# Useful Links #

- **MongoDB** https://www.mongodb.org/
- **Twitter developers** https://dev.twitter.com/
- **Supervisor** http://supervisord.org/
- **Documentation for virtualenvwrapper** http://virtualenvwrapper.readthedocs.org/en/latest/
