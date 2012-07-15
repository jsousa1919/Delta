import sqlite3

db_file = '/home/justin/deltachi/delta-aggregate/twitter-scraper/db.sqlite'
db = sqlite3.connect(db_file)
cur = db.cursor()

class Company(object):

    def __init__(self, symbol):
        self.sid = None
        self.symbol = symbol
        self.name = symbol
        self.keywords = [symbol]

    def set_sid(self, sid):
        self.sid = sid

    def set_name(self, name):
        self.name = name

    def add_keywords(self, keywords):
        self.keywords.extend(keywords)
        self.keywords = list(set(self.keywords))

def from_symbol(symbol):
    cur.execute('select sid, name from stock where symbol = ?', [symbol])
    row = cur.fetchone()
    if row is None:
        return None
    co = Company(symbol)
    co.set_sid(row[0])
    if row[1]: co.set_name(row[1])

    # set keywords
    cur.execute('select word from keyword2 where sid = ?', [co.sid])
    co.add_keywords([row[0] for row in cur.fetchall()])

    return co
