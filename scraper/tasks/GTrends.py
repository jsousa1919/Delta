import datetime
import time
import re
import csv
import datetime
from scrapers import pyGTrends
from Task import Task, DumpTask
import random
import logging
        
class GTScrapeTask(Task):
    taskName = "GTScrape"
        
    def execute(self):
        logging.info("Scraping Google Trends")
        companies = self.followed_stocks() # ['ZIP', 'LNKD', 'GIS', 'MHP', 'NKE', 'RL', 'VZ', 'AAPL', 'CAT']
        for company in companies:
            #collect Google Trends data
            task = GTQueryStockTask(self.agent, [company])
            task.delta = random.randrange(1,60*len(companies))
            task.schedule() 

class GTQueryStockTask(Task):
    taskName = "GTQueryStock"
    gtrends_col = ("sid", "date", "searches", "error")

    def __init__(self, agent, args):
        super(GTQueryStockTask, self).__init__(agent, args)
        self.symbol = args[0]
        if len(args) > 1:
            self.date_string = args[1]
        else:
            self.date_string = None
            
    def get_info(self):
        logging.info("Retrieving previous data")
        self.now = datetime.datetime.now()

        logging.debug("Checking for stock symbol %s", self.symbol)
        sql = "select sid, last_gtrends from stock where symbol=?;"
        res = self.db.query(sql, self.symbol, one=True)
        if res:
            self.id, self.last = res
            self.last = datetime.date(*[int(x) for x in self.last.split("-")]) if self.last else None
        else:
            logging.debug("New stock %s", self.symbol)
            self.db.insert('stock', {'symbol': self.symbol})
            self.id = self.db.last_insert_id()
            self.last = None

        # check global GTrends throttle
        sql = "select last from gtrends_throttle limit 1;"
        res = self.db.query(sql, one=True)
        last_check = datetime.datetime.strptime(res, "%Y-%m-%d %H:%M:%S.%f") if res else None
        if last_check and self.now - last_check < datetime.timedelta(seconds=10):
            # print self.now - last_check
            logging.warning("Global throttle rate for GTrends has been exceeded")
            self.delta = random.randint(0,600)
            self.schedule()

    def finish(self):
        logging.info("Storing collected data")
        for row in self.data:
            gtrends = dict([(col, row[col]) for col in self.gtrends_col])
            self.db.insert("gtrends", gtrends, on_conflict="replace")           
        
        logging.info("Marking stock as updated")
        sql = "update stock set last_gtrends=? where sid=?;"
        self.db.update('stock', {'last_gtrends': self.now.date()}, {'sid': self.id})
    
    def __parse_data(self, data):
        found_data = False
        data.pop(0) # column headers
        for row in data:
            searches = float(row[1])
            if not found_data and searches is not 0:
                found_data = True
            date = datetime.datetime.strptime(row[0], "%b %d %Y").date()
            error = row[2]
            vals = (self.id, date, searches, error)
            self.data.append(dict(zip(self.gtrends_col, vals)))

        return found_data
        
    def execute(self):
        self.get_info()

        # if checked within last 24 hours, don't
        if self.date_string is None and \
           self.last is not None and \
           self.now.date() - self.last < datetime.timedelta(days=1):
            logging.info("Stock checked today, aborting")
            return
        
        logging.info("Connecting to Google Trends for stock %s", self.symbol)
        self.data = []
        
        self.db.update('gtrends_throttle', {'last': self.now})
        self.db.commit()
        g = pyGTrends.pyGTrends()
        if self.last is not None:
            # get month or recent
            date = self.date_string or "mtd"
            logging.info("Checking %s", date)
            g.download_report(self.symbol, date=date, scale=1)
            recent = g.csv(as_list=True)
            self.__parse_data(recent)
        else:
            # get all data
            logging.info("Scheduling history check")
            cur_month = self.now.date()
            while cur_month > datetime.date(2009,1,1):
                date_string = "%d-%d" % (cur_month.year, cur_month.month)
                mtask = GTQueryStockTask(self.agent, [self.symbol, date_string])
                mtask.delta = random.randrange(1, 86400*7) # one week to catch up on history
                mtask.schedule()
                # hack to decrement month
                m = cur_month.month
                while cur_month.month == m: 
                    cur_month = cur_month - datetime.timedelta(27)

        self.finish()


class GTDumpTask(DumpTask):
    taskName = "GTDump"
    tables = [{ \
        'query': 'select %s from stock', \
        'drop': None, \
        'file': 'stock.csv', \
        'columns': ['sid', 'symbol', 'last_gtrends', 'follow'] \
    }, { \
        'query': 'select %s from gtrends', \
        'drop': None, \
        'file': 'gtrends.csv', \
        'columns': ['sid', 'date', 'searches', 'error']}]
