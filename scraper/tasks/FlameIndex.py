import datetime
import re
import csv
import datetime
import random
import BeautifulSoup
import logging
from Task import Task, DumpTask
from dateutil import tz

from_zone = tz.gettz('UTC')
to_zone = tz.gettz('US/Eastern')

class FITask(Task):
    def get_link(self, sid):
        logging.debug("Fetching Flame Index link for stock id %s", sid)
        sql = "select flameindex_link from stock where sid=?;"
        return self.db.query(sql, sid, one=True)
        
    def stock_update(self, sid):
        logging.debug("Checking last Flame Index update for stock id %s", sid)
        sql = "select last_flameindex from stock where sid=?;"
        res = self.db.query(sql, sid, one=True)
        return datetime.datetime.strptime(res, "%Y-%m-%d %H:%M:%S.%f") if res else None

class FIScrapeTask(FITask):
    taskName = "FIScrape"
        
    def execute(self):
        logging.info("Scraping FlameIndex")
        companies = self.followed_stocks() # ['ZIP', 'LNKD', 'GIS', 'MHP', 'NKE', 'RL', 'VZ', 'AAPL', 'CAT']
        for company in companies:
            #collect FlameIndex data
            task = FIQueryStockTask(self.agent, [company])
            task.delta = random.randrange(1,60*len(companies))
            task.schedule() 

class FIQueryStockTask(FITask):
    taskName = "FIQueryStock"

    def __init__(self, agent, args):
        super(FIQueryStockTask, self).__init__(agent, args)
        self.symbol = args[0]
        self.data = {}
            

    def get_info(self):
        logging.info("Retrieving previous data")
        self.now = datetime.datetime.now()
        self.id = self.get_stock(self.symbol)
        self.link = self.get_link(self.id)
        self.last = self.stock_update(self.id)
        

    def finish(self):
        track_ids = ("sid", "date")

        logging.info("Storing collected data")
        for row in self.data['history']:
            self.db.safe_insert("fi_track", {'sid': row[0], 'date': row[1]}, {'rank': row[2]})
        
        data = self.data['current']
        self.db.safe_insert("fi_track", {'sid': data[0], 'date': data[1]}, {'rating': data[2]})

        logging.info("Marking stock as updated")
        sql = "update stock set last_flameindex = ? where sid = ?;"
        self.db.query(sql, [self.now, self.id])
        
    def execute(self):
        self.get_info()

        # if link doesn't exist, get one
        if self.link is None:
            # do something to find one
            return

        # if checked recently, don't
        if self.last is not None and \
           self.now - self.last < datetime.timedelta(hours=1):
            logging.info("Stock checked recently, aborting")
            return

        # get page
        logging.info("Scraping Flame Index for stock %s", self.symbol)
        page = self.get_page(self.link)
        soup = BeautifulSoup.BeautifulSoup(page)

        # parse index
        logging.debug("Parsing index")
        try:
            date = soup.find(attrs={'class': 'currentdate'})
            date = datetime.datetime.strptime(date.text, '%A, %B %d, %Y %I:%M:%S %p %Z')
            details = soup.find(attrs={'class': 'indexdetail'})
            rating = float(details.find(text='CURRENT SCORE:').findNext('td').text)
            self.data['current'] = [self.id, date, rating]

            startLoc = page.find("name: 'Flame Index Rank'")
            endLoc = page[startLoc:].find("}")
            pat = re.compile('\[(\d+), (\d+)\]')
            items = pat.findall(page[startLoc : startLoc + endLoc])
            self.data['history'] = []
            for (date, ranking) in items:
                date = datetime.datetime.utcfromtimestamp(int(date)/1000)
                if self.last is None or date > self.last:
                    self.data['history'].append([self.id, date, float(ranking)])
        except:
            raise Exception("Sentiment not found in page")

        self.finish()

class FIDumpTask(DumpTask):
    taskName = "FIDump"
    tables = [{ \
        'query': 'select %s from fi_track', \
        'drop': None, \
        'file': 'fi_track.csv', \
        'columns': ['sid', 'date', 'rating', 'rank']}]
