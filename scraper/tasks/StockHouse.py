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
        
    def execute(self):
        logging.info("Scraping StockHouse")
        companies = self.followed_stocks() # ['ZIP', 'LNKD', 'GIS', 'MHP', 'NKE', 'RL', 'VZ', 'AAPL', 'CAT']
        for company in companies:
            #collect all new StockHouse articles
            newArticles = SHQueryStock(self.agent, [company])
            newArticles.delta = random.randrange(1,86400)
            newArticles.schedule() 
            
class SHQueryStock(SHTask):
    taskName = "SHQueryArticles"
    
    def __init__(self, agent, args):
        super(SHQueryStock, self).__init__(agent, args)
        self.symbol = args[0]
        
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
                newPage = SHQueryArticle(self.agent, [aurl, date, headline, self.id, self.last, self.now])
                newPage.delta = random.randrange(1, 3600)
                newPage.schedule()

class SHNewPage(Task):
    taskName = "SHNP"
    
    def __init__(self, agent, args):
        super(SHNewPage, self).__init__(agent, args)
        self.url = args[0]
        self.date = args[1]
        self.headline = args[2]
        self.id = args[3]
        self.last = args[4]
        self.now = args[5]
        
    def execute(self):
        logging.info("Retrieving new StockHouse page")
        htmlNP = self.get_page(self.url)
        soupNP = BeautifulSoup(htmlNP)
        frame = soupNP.find("iframe", id="ctl00_cphMainContent_frmTool") #frame that contains article
        if frame is not None:
            frameLink = "http://www.stockhouse.com"+frame["src"][2:]  
            article = SHQueryArticle(self.agent, [self.url, self.date, self.headline, self.id, self.last, self.now])
            article.schedule()
        else:
            article = SHQueryArticle(self.agent, [self.url, self.date, self.headline, self.id, self.last, self.now])
            article.schedule()
            
            
class SHQueryArticle(Task):
    taskName = "SHQueryArticle"
    data_col = ('aid', 'date', 'headline', 'article', 'refs', 'link')
    
    def __init__(self, agent, args):
        super(SHQueryArticle, self).__init__(agent, args)
        self.url = args[0]
        self.date = args[1]
        self.headline = args[2]
        self.id = args[3]
        self.last = args[4]
        self.now = args[5]
        self.data = []
        
    def get_article(self, headline):
        sql = "select aid from sh_article where headline=?;"
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
        SHArticle_col = ("date", "headline", "article", "link")
        SHArticle_ref_col = ("aid", "sid")
        track_col = ("sid", "start", "end", "count")
        
        logging.info("Storing collected data")
        for data in self.data:
            self.db.update('sh_article', dict([(col, data[col]) for col in SHArticle_col]), {"aid": data['aid']})

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
        end = 0 #used to determine when the end of the article is reached
        frameLinkHTML = self.get_page(self.url)
        frameSoup = BeautifulSoup(frameLinkHTML)
        artPiece = frameSoup.find('div', attrs={"class": "ft_body"}).findAll("p") #all paragraphs in article
        for piece in artPiece:
            if piece.text[:6] != "About " and end == 0:
                article.append(piece.text)
                if ticker_pattern.findall(piece.text):
                    refs.extend(ticker_pattern.findall(piece.text))
            else:
                end = 1
        self.add_article(self.date, self.headline, ' '.join(article).encode("ascii", "ignore"), refs, self.url)
        self.finish()
    
class SHDumpTask(DumpTask):
    taskName = "SHDump"
    tables = [{ \
        'query': 'select %s from sh_article', \
        'drop': None, \
        'file': 'sh_article.csv', \
        'columns': ['aid', 'date', 'headline', 'article'] \
    }, { \
        'query': 'select %s from sh_article_ref', \
        'drop': None, \
        'file': 'sh_article_ref.csv', \
        'columns': ['aid', 'sid', 'symbol'] \
    }, { \
        'query': 'select %s from sh_track', \
        'drop': None, \
        'file': 'sh_track.csv', \
        'columns': ['sid', 'start', 'end', 'count']}]
