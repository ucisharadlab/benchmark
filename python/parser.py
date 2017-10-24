import json


class Parser(object):

    def __init__(self, seedFile):
        self.fp = open(seedFile)

    def getNext(self):
        try:
            return json.loads(self.fp.readline())
        except Exception as e:
            return None
