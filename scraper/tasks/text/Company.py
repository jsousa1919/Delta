class Company(object):

    def __init__(self, symbol):
        self.sid = None
        self.symbol = symbol
        self.name = symbol
        self.keywords = set([symbol])

    def set_sid(self, sid):
        self.sid = sid

    def set_name(self, name):
        self.name = name

    def add_keywords(self, keywords):
        self.keywords.update(keywords)