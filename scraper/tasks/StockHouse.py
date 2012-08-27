import time
import re
import csv
import datetime
import re
from BeautifulSoup import BeautifulSoup
from Task import Task, DumpTask
import random 
import logging

class SHTask(Task):
    def stock_update(self, sid):
        logging.debug("Checking last StockHouse update for stock id %s", sid)
        sql = "select last_stockhouse from stock where sid="+str(sid)
        res = self.db.query(sql, one=True)
        if res:
            return datetime.datetime.strptime(res, "%Y-%m-%d %H:%M:%S.%f")
        else:
            return None
            
class SHScrapeTask(Task):
    """
    Check StockHouse.com periodically for new news articles
    """

    taskName = "SHScrape"
    def __init__(self, agent, args):
        super(SHScrapeTask, self).__init__(agent, args)
        self.history = (len(args) > 0)
        
    def execute(self):
        logging.info("Scraping StockHouse")
        companies = self.followed_stocks() # ['ZIP', 'LNKD', 'GIS', 'MHP', 'NKE', 'RL', 'VZ', 'AAPL', 'CAT']
        for company in companies:
            #collect all new StockHouse articles
            newArticles = SHQueryStock(self.agent, [company, self.history])
            newArticles.delta = random.randrange(1,86400)
            newArticles.schedule() 
            
class SHQueryStock(SHTask):
    taskName = "SHQueryArticles"
    
    def __init__(self, agent, args):
        super(SHQueryStock, self).__init__(agent, args)
        self.symbol = args[0]
        self.history = (len(args) > 1)
        
    def get_info(self):
        self.now = datetime.datetime.now()
        logging.info("Retrieving previous data")
        self.id = self.get_stock(self.symbol)
        self.last = self.stock_update(self.id)

    def execute(self):
        self.get_info()
        logging.info("Checking new StockHouse for %s", self.symbol)
        
        url = "http://www.stockhouse.com/financialtools/sn_newsreleases.aspx?qm_symbol="+self.symbol
        soup = BeautifulSoup(self.get_page(url))
        dates = soup.findAll("span", attrs={"class": "ft_stat"}) #all dates of articles
        for date in dates:
            sql = "select link from sh_article where headline=?"
            link = date.findPrevious("a", href = re.compile("^sn_newsreleases"))
            dupURL = self.db.query(sql, [link.text], one=True)
            if link.text != "News Releases" and link.text != "Next &gt;&gt;" and not dupURL:
                aurl = link["href"] #article url
                headline = link.text # article headline 
                # format date
                date = date.text[:date.text.index("-")-1].strip()
                try:
                    date = datetime.datetime.strptime(date, "%m/%d/%Y %I:%M %p")
                except ValueError: #date includes seconds
                    date = datetime.datetime.strptime(date, "%m/%d/%Y %I:%M:%S %p")
                #get article
                aurl = "http://www.stockhouse.com/FinancialTools/" + aurl
                newPage = SHQueryArticle(self.agent, [aurl, date, headline, self.id, self.last, self.now, self.history])
                if self.history:
                    newPage.schedule()
                    return
                else:
                    newPage.delta = random.randrange(1, 3600)
                    newPage.schedule()
            
            
class SHQueryArticle(Task):
    taskName = "SHQueryArticle"
    data_col = ('id', 'date', 'headline', 'text', 'refs', 'link')
    
    def __init__(self, agent, args):
        super(SHQueryArticle, self).__init__(agent, args)
        self.url = args[0]
        self.date = args[1]
        self.headline = args[2]
        self.id = args[3]
        self.last = args[4]
        self.now = args[5]
        self.history = args[6]
        self.data = []
        
    def get_article(self, headline):
        sql = "select id from sh_article where headline=?;"
        aid = self.db.query(sql, [headline], one=True)
        if not aid:
            logging.debug("New article")
            self.db.insert('sh_article', {'headline': headline})
            aid = self.db.last_insert_id()
        #print aid
        return aid
        
    def add_article(self, date, headline, article, refs, link):
        sql = "select * from sh_article where headline=?"
        duplicate = self.db.bool_query(sql, [headline])
        if not duplicate:
            aid = self.get_article(headline)
            data = (aid, date, headline, article, refs, link)
            self.data.append(dict(zip(self.data_col, data)))
            
    def finish(self):
        SHArticle_col = ("date", "headline", "text", "link")
        SHArticle_ref_col = ("id", "sid")
        track_col = ("sid", "start", "end", "count")
        
        logging.info("Storing collected data")
        for data in self.data:
            self.db.update('sh_article', dict([(col, data[col]) for col in SHArticle_col]), {"id": data['id']})

            refs = [self.get_stock(symbol) for symbol in data['refs']] #sids of referenced stocks
            # symRefs = [symbol for symbol in data['refs']] #all stocks being referenced  # this does nothing
            for data['sid'] in refs:
                self.db.insert('sh_article_ref', dict([(col, data[col]) for col in SHArticle_ref_col]))

        self.db.insert('sh_track', dict(zip(track_col, (self.id, self.last, self.now, len(self.data)))))
        
        logging.info("Marking stock as updated")
        sql = "update stock set last_stockhouse = ? where sid = ?;"
        cur = self.db.query(sql, [self.now, self.id])
        
    def execute(self):
        logging.info("Retrieving article from StockHouse page")
        ticker_pattern = re.compile("(?:NYSE|Nasdaq|NASDAQ): ?([A-Z]+(?:\.[A-Z]+)?)\W")
        refs = [] #holds all references to other stocks(NYSE and Nasdaq only)
        article = [] #holds all paragraphs of article
        page = self.get_page(self.url)

        if self.history:
            soup = BeautifulSoup(page)
            self.headline = soup.find("div", attrs={"class": "qmnews_headline"}).text
            date = soup.find("div", attrs={"class": "qmnews_datetime"}).text
            self.date = datetime.datetime.strptime(date, "%B %d, %Y - %I:%M %p %Z")
        
        frameSoup = BeautifulSoup(page[page.index('<body', 10000):page.index('</body>')])
        artPiece = frameSoup.findAll("p") #all paragraphs in article
        for piece in artPiece:
            if piece.text[:6] == "About ":
                break
            article.append(' ' + piece.text)
            if ticker_pattern.findall(piece.text):
                refs.extend(ticker_pattern.findall(piece.text))
        self.add_article(self.date, self.headline, ' '.join(article).encode("ascii", "ignore"), refs, self.url)

        if self.history:
            nav = soup.find("td", attrs={"class": "ft_more_link_r"}).find("a")
            if nav.text.find("<<") >= 0:
                t = SHQueryArticle(self.agent, [nav['href'], None, None, self.id, self.last, self.now, self.history])
                t.delta = random.randrange(1, 300)
                t.schedule()

        self.finish()
    
class SHDumpTask(DumpTask):
    taskName = "SHDump"
    tables = [{ \
        'query': 'select %s from sh_article', \
        'drop': None, \
        'file': 'sh_article.csv', \
        'columns': ['id', 'date', 'headline', 'text'] \
    }, { \
        'query': 'select %s from sh_article_ref', \
        'drop': None, \
        'file': 'sh_article_ref.csv', \
        'columns': ['id', 'sid', 'symbol'] \
    }, { \
        'query': 'select %s from sh_track', \
        'drop': None, \
        'file': 'sh_track.csv', \
        'columns': ['sid', 'start', 'end', 'count']}]
