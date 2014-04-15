import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
try:
    README = open(os.path.join(here, 'README.md')).read()
except:
    README = """\
Twitter Tap is a tool for collecting tweets to a mongoDB using the twitter search API. """

CLASSIFIERS = [
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'Intended Audience :: Science/Research',
    'License :: OSI Approved :: MIT License',
    'Topic :: Communications :: Chat',
    'Topic :: Internet',
    'Topic :: Database'
]

dist = setup(
    name='twitter-tap',
    version='1.0.0',
    description='Collect tweets to a mongoDB using the Twitter search API.',
    long_description=README,
    author='Janez Kranjc',
    author_email='janez.kranjc@gmail.com',
    url='http://janezkranjc.github.io/twitter-tap/',
    license = 'MIT',
    install_requires=['pymongo','twython'],
    classifiers=CLASSIFIERS,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'console_scripts': [
         'tap = twitter_tap.tap:main',
        ],
    }
)