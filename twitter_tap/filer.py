import json
import datetime
import os
import errno
import os.path
import time
import gzip
import logging

logger = logging.getLogger('twitter')

class Filer:
    def __init__(self, data_dir, n=10000):
        if not os.path.exists(data_dir):
            os.makedirs(data_dir, mode=0o777, exist_ok=True)
        self.data_dir = data_dir
        self.counter = 0  # number of tweets in a file
        self.n = n  # max number of tweets in a file
        self.flush_every = 100
        self.file = None
        self.new_file()

    def emit(self, dict_entry):
        text_to_wtite = json.dumps(dict_entry) + "\n"
        self.file.write(text_to_wtite)
        self.counter += 1
        if self.counter > self.flush_every:
            self.file.flush()
        if self.counter >= self.n:  # add condition  "if new day"
            self.close_file()
            self.new_file()

    def new_file(self):
        # generate directory name & create it if id does not exist, time in UTC!
        now = datetime.datetime.utcnow()
        directory = os.path.join(self.data_dir, now.strftime("%Y/%m/%d"))
        os.makedirs(directory, mode=0o777, exist_ok=True)  # don't raise an error if the directory already exists

        # generate filename
        file_name = os.path.join(directory, now.strftime("%Y-%m-%d_%H-%M-%S") + ".txt")
        while os.path.exists(file_name):
            file_name = os.path.join(directory, now.strftime("%Y-%m-%d_%H-%M-%S-%f") + ".txt")

        logger.debug("Creating new file: " + file_name)

        # open file
        self.file = open(file_name, 'w')

    def close_file(self):
        try:
            self.file.close()
        except AttributeError:
            pass
        self.counter = 0

    def __del__(self):
        self.close_file()
