from setuptools import setup, find_packages
import os

import os
long_description = 'Twitter Tap is a python tool that connects to the Twitter API and issues calls to the search or the streaming endpoint using a query that the user has entered.'
if os.path.exists('README.rst'):
    long_description = open('README.md').read()

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
    version='2.0.3',
    author='Janez Kranjc',
    description='Collect tweets to a mongoDB using either the Twitter search API or the streaming API.',
    long_description=long_description,
    author_email='janez.kranjc@gmail.com',
    url='http://janezkranjc.github.io/twitter-tap/',
    license = 'MIT',
    install_requires=['pymongo','twython','six'],
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
