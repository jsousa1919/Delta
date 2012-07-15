import datetime
import time
import re
import csv
import datetime
from scrapers import Price
from Task import Task, DumpTask
import random
import logging
        
class PHScrapeTask(Task):
    taskName = "PHScrape"
        
    def execute(self):
        logging.info("Scraping stock price history")
        companies = self.followed_stocks() # ['ZIP', 'LNKD', 'GIS', 'MHP', 'NKE', 'RL', 'VZ', 'AAPL', 'CAT']
        for company in companies:
            #collect Google and Yahoo price history
            task = PHQueryStockTask(self.agent, [company])
            task.delta = random.randrange(1,60*len(companies))
            task.schedule() 

class PHQueryStockTask(Task):
    taskName = "PHQueryStock"
    price_col = ("sid", "date", "open", "close", "high", "low", "volume")

    def __init__(self, agent, args):
        super(PHQueryStockTask, self).__init__(agent, args)
        self.symbol = args[0]
            
    def get_info(self):
        logging.info("Retrieving previous data")
        self.now = datetime.datetime.now().date()

        self.id = self.get_stock(self.symbol)
        sql = "select last_pricing from stock where sid=?;"
        self.last = self.db.query(sql, self.id, one=True)
        self.last = datetime.datetime.strptime(self.last, "%Y-%m-%d").date() if self.last else None

        # if checked within last 24 hours, don't
        if self.last is not None and self.now - self.last < datetime.timedelta(1):
            logging.info("Stock checked today, aborting")
            return False

        self.old = []
        sql = "select date, open, close, high, low, volume from price_history where sid=?"
        old = self.db.query(sql, self.id)
        for row in old:
            row = list(row)
            row[0] = datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
            self.old.append(row)
        return True


    def finish(self):
        logging.info("Storing collected data")
        for row in self.data:
            gtrends = dict([(col, row[col]) for col in self.price_col])
            self.db.insert("price_history", gtrends)           
        
        logging.info("Marking stock as updated")
        self.db.update('stock', {'last_pricing': self.now}, {'sid': self.id})
        
    def execute(self):
        if not self.get_info():
            return
        
        logging.info("Collecting stock price history for stock %s", self.symbol)
        self.data = []
        
        g = Price.Collector()
        g.importLol(self.symbol, self.old)
        g.update()
        for k,v in g.updates.items():
            v['sid'] = self.id
            v['date'] = k
            self.data.append(v)

        self.finish()


class PHDumpTask(DumpTask):
    taskName = "PHDump"
    tables = [{ \
        'query': 'select %s from price_history', \
        'drop': None, \
        'file': 'price_history.csv', \
        'columns': ['sid', 'date', 'open', 'close', 'high', 'low', 'volume']}]
