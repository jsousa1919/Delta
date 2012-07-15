import datetime
import re
import csv
import datetime
import BeautifulSoup
import logging
import random
from Task import Task, DumpTask

class SoPiTask(Task):
    def stock_update(self, sid):
        logging.debug("Checking last SocialPicks update for stock id %s", sid)
        sql = "select last_socialpicks from stock where sid=?;"
        res = self.db.query(sql, sid, one=True)
        return datetime.datetime.strptime(res, "%Y-%m-%d %H:%M:%S.%f") if res else None

class SoPiScrapeTask(SoPiTask):
    taskName = "SoPiScrape"
        
    def execute(self):
        logging.info("Scraping SocialPicks")
        companies = self.followed_stocks() # ['ZIP', 'LNKD', 'GIS', 'MHP', 'NKE', 'RL', 'VZ', 'AAPL', 'CAT']
        for company in companies:
            #collect SocialPicks data
            task = SoPiQueryStockTask(self.agent, [company])
            task.delta = random.randrange(1,60*len(companies))
            task.schedule() 


class SoPiQueryStockTask(SoPiTask):
    taskName = "SoPiQueryStock"
    url = "http://www.socialpicks.com/stock/%s"

    def __init__(self, agent, args):
        super(SoPiQueryStockTask, self).__init__(agent, args)
        self.symbol = args[0]
        self.link = self.url % self.symbol
            

    def get_info(self):
        logging.info("Retrieving previous data")
        self.now = datetime.datetime.now()
        self.id = self.get_stock(self.symbol)
        self.last = self.stock_update(self.id)
        

    def finish(self):
        # track_col = ("sid", "date", "rating")

        logging.info("Storing collected data")
        self.db.insert("sopi_stock_track", self.data)     
        
        logging.info("Marking stock as updated")
        sql = "update stock set last_socialpicks = ? where sid = ?;"
        self.db.query(sql, [self.now, self.id])
        
    def execute(self):
        self.get_info()

        # if checked recently, don't
        if self.last is not None and \
           self.now - self.last < datetime.timedelta(days=1):
            logging.info("Stock checked recently, aborting")
            return

        # get page
        logging.info("Scraping Social Picks for stock %s", self.symbol)
        page = self.get_page(self.link)

        # parse rating
        logging.debug("Parsing rating")
        try:
            startLoc = page.find("SocialPicks Sentiment:")
            endLoc = page[startLoc:].find("</td>")
            fullPat = re.compile('graphic_star_big.gif')
            halfPat = re.compile('graphic_star_big_half.gif')
            full = fullPat.findall(page[startLoc : startLoc + endLoc])
            half = halfPat.findall(page[startLoc : startLoc + endLoc])
            score = len(full) if full else 0
            score += .5 if half else 0
            self.data = {'sid': self.id, 'date': self.now, 'rating': float(score)}
        except:
            raise Exception("Sentiment not found in page")

        self.finish()

class SoPiDumpTask(DumpTask):
    taskName = "SoPiDump"
    tables = [{ \
        'query':    'select %s from sopi_track', \
        'drop':     None, \
        'file':     'sopi_track.csv', \
        'columns':  ['sid', 'date', 'rating']}]
