import json
import datetime
import os
import errno
import os.path
import time
import gzip

class filer:

    def __init__(self, dataDir, N=10000):
        if not os.path.exists(dataDir):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), dataDir)
        self.dataDir = dataDir
        self.counter = 0    #number of tweets in a file
        self.N = N          #max number of tweets in a file
        self.flushEvery = 100
        self.file = None
        self.newFile()


    def emit(self, dictEntry):
        textToWtite = json.dumps(dictEntry)+ "\n"
        self.file.write(textToWtite)
        self.counter += 1
        if self.counter > self.flushEvery:
            self.file.flush()
        if self.counter >= self.N:   # add condition  "if new day"
            self.closeFile()
            self.newFile()


    def newFile(self):
        #generate directory name & create it if id does not exist, time in UTC!
        now = datetime.datetime.utcnow()
        directory = os.path.join(self.dataDir, now.strftime("%Y/%m/%d"))
        os.makedirs(directory, mode=0o777, exist_ok=True)  # don't raise an error if the directory already exists

        #generate filename
        fileName = now.strftime("%Y-%m-%d_%H-%M-%S")+".txt"
        print(fileName)

        #open file
        self.file = open(os.path.join(directory, fileName), 'w')


    def closeFile(self):
        self.file.close()
        self.counter = 0


    def __del__(self):
        self.closeFile()


if __name__ == '__main__':
    f = filer("./data", N=2)
    f.emit({"First" : 1} )
    time.sleep(1)
    f.emit({"Second": 2,  "this": "tralalal"
                                  "jssjsjsj" })
    time.sleep(1)
    f.emit({"Third": 3, 33: 77})
    time.sleep(1)
    f.emit({"Fourth": 4, 44: 77})
    time.sleep(1)
    f.emit({"Fifth": 5, 55: 77})
    time.sleep(1)
    f.emit({"Sixth": 6, 66: 77})
    time.sleep(1)
    f.emit({"Seventh": 7, 777: 77})




