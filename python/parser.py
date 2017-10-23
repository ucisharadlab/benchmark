import json


class Parser(object):

    def __init__(self, seedFile):
        self.fp = open(seedFile)

    def getNext(self):
        try:
            json.load(self.fp.readline())
        except Exception as e:
            return None
