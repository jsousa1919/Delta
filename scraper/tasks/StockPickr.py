import datetime
import re
import csv
import datetime
import BeautifulSoup
import logging
import random
from Task import Task, DumpTask

list_user = "LIST"

class SPQueryUserTask(Task):
    taskName = "SPQueryUser"

    def __init__(self, agent, args):
        super(SPQueryUserTask, self).__init__(agent, args)
        self.user = args[0]

    def get_info(self):
        logging.debug("Checking for StockPickr User %s", self.user)
        sql = "select uid from spr_user where name=%s;"
        self.id = self.db.query(sql, self.user, one=True)
        if not self.id:
            self.db.insert('spr_user', {'name': self.user})
            self.id = self.db.last_insert_id()
            
    def execute(self):
        logging.info("Scraping User %s (not implemented)", self.user)
        

class SPQueryPortfolioTask(Task):
    taskName = "SPQueryPortfolio"
    purl = "http://www.stockpickr.com/%s/portfolio/%s"
    lurl = "http://www.stockpickr.com/today/%s"
    grades = {"F":0, "D-":1, "D":2, "D+":3, "C-":4, "C":5, "C+":6, "B-":7, "B":8, "B+":9, "A-":10, "A":11, "A+":12}

    def __init__(self, agent, args):
        super(SPQueryPortfolioTask, self).__init__(agent, args)
        self.user = args[0]
        self.portfolio = args[1]
            

    def get_info(self):
        logging.info("Retrieving previous data")
        self.now = datetime.datetime.now().date()

        logging.debug("Checking for StockPickr User %s", self.user)
        sql = "select uid from spr_user where name=?;"
        uid = self.db.query(sql, self.user, one=True)
        if not uid:
            logging.debug("New User")
            cur = self.db.insert('spr_user', {'name': self.user})
            uid = self.db.last_insert_id()

        logging.debug("Checking for StockPickr Portfolio %s", self.portfolio)
        sql = "select pid, last from spr_portfolio where uid=? and name=?;"
        res = self.db.query(sql, [uid, self.portfolio], one=True)
        if res:
            self.id = res[0]
            self.last = datetime.date(*[int(x) for x in res[1].split("-")])
        else:
            logging.debug("New Portfolio")
            cur = self.db.insert('spr_portfolio', {'uid': uid, 'name': self.portfolio})
            self.id = self.db.last_insert_id()
            self.last = None

    def finish(self):
        port_track_col = ("pid", "date", "description", "views", "user_rating", "user_rating_count", "pro_rating")
        pos_col = ("pid", "date", "stock", "shares", "percentage", "notes")

        logging.info("Storing collected data")
        port_track = dict([(col, self.data[col]) for col in port_track_col])
        self.db.insert("spr_portfolio_track", port_track)
        for position in self.data["positions"]:
            pos = dict([(col, position[col]) for col in pos_col])
            self.db.insert("spr_position", pos)     
        
        logging.info("Marking portfolio as updated")
        sql = "update spr_portfolio set last = ? where pid = ?;"
        self.db.update('spr_portfolio', {'last': self.now}, {'pid': self.id})

    def __parse_positions(self, var):
        listPattern = re.compile("\[[^\[]+?\]")
        tagPattern = re.compile("<.+?>")
        betweenTagPattern = re.compile(">(.+?)<")
        numberPattern = re.compile("-?\d+\.?\d*")
        stringPattern = re.compile("\".*?[^\\\\]\"")

        positions = []
        columns = ('pid', 'date', 'stock', 'percentage', 'shares', 'notes')
        for text in listPattern.findall(var):
            data = stringPattern.findall(text[1:-1])
            stock = betweenTagPattern.findall(data[0])[0]
            if self.user == list_user:
                percentage = shares = 'NULL'
                notes = tagPattern.sub(" ", data[-1][1:-1])
            else:
                comments = tagPattern.split(data[-1][1:-1])
                try:
                    percentage = float(numberPattern.findall(comments[0])[0])
                except:
                    percentage = 0
                try:
                    shares = float(numberPattern.findall(comments[1])[0])
                except:
                    shares = 0
                try:
                    notes = comments[2]
                except:
                    notes = ''
            positions.append(dict(zip(columns, (self.id, self.now, stock, percentage, shares, notes.encode("ascii", "ignore")))))
        return positions
        
    def execute(self):
        self.get_info()

        # if checked within last 24 hours, don't
        if self.last is not None and self.now == self.last:
            logging.info("Portfolio checked today, aborting")
            return

        # get page
        logging.info("Scraping portfolio %s", self.portfolio)
        if self.user == list_user:
            url = self.lurl % self.portfolio
        else:           
            url = self.purl % (self.user, self.portfolio)
        page = self.get_page(url)
        soup = BeautifulSoup.BeautifulSoup(page)

        # parse statistics
        logging.debug("Parsing stats")
        details = soup.find(attrs={"class":"details"})
        if details is None:
            logging.error("No user details on page, is the portfolio name correct?")
            return
        views = int(re.compile("SP.viewCount = '(.+?)'").findall(page)[0].replace(",",""))
        desc = ''.join(line for line in soup.find(id="desc") if type(line) is not BeautifulSoup.Tag)
        try:
            userRatingText = details.find(attrs={"id":"ratingSum"}).text
            userRating, userRatingCount = (float(num) for num in userRatingText.split('/'))
            proRating = self.grades[details.find(attrs={"class":"rating"}).find("h3").nextSibling]
        except Exception as ex:
            userRating = userRatingCount = proRating = 0

        # parse positions
        logging.debug("Parsing positions")
        varPattern = re.compile("aDataSet.+[^\\\\];")
        positions = self.__parse_positions(varPattern.findall(page)[0])

        # wrap up
        columns = ("pid", "date", "description", "views", "user_rating", "user_rating_count", "pro_rating", "positions")
        data = ( \
            self.id, \
            self.now, \
            desc.encode("ascii", "ignore"), \
            views, \
            userRating, \
            userRatingCount, \
            proRating, \
            positions)
        self.data = (dict(zip(columns, data)))
        self.finish()


class SPScrapeUpdatesTask(Task):
    taskName = "SPScrapeUpdates"
    aurl = "http://www.stockpickr.com"

    def __init__(self, agent, args):
        super(SPScrapeUpdatesTask, self).__init__(agent, args)
        self.list = args[0]  
        self.link = None

    def get_info(self):
        logging.info("Retrieving previous data")
        self.now = datetime.datetime.now().date()

        logging.debug("Checking for StockPickr list %s", self.list)
        sql = "select link, last from spr_updates where name=?;"
        res = self.db.query(sql, self.list, one=True)
        if not res:
            return False
        self.link = res[0]
        self.last = datetime.date(*[int(x) for x in res[1].split("-")])
        return True

    def finish(self):
        logging.info("Marking list as updated")
        self.db.update('spr_updates', {'last': self.now}, {'name': self.list})
    
    def execute(self):
        if not self.link and not self.get_info():
            logging.warning("List %s does not exist", self.list)
            return
            
        # get page
        logging.info("Scraping list %s", self.list)
        page = self.get_page(self.link)
        soup = BeautifulSoup.BeautifulSoup(page)

        # parse
        logging.debug("Parsing")
        updates = soup.find(id="listData") or soup.find(id="todays_lists")
        for row in updates.findAll("tr")[1:]:
            try:
                data = row.findAll("td") 
                if len(data) == 1:
                    link = data[0].find("a").get("href").split("/")
                    # daily lists page
                    user = list_user
                    portfolio = link[-2]
                    SPQueryPortfolioTask(self.agent, [user, portfolio]).schedule()
                elif len(data) == 2:
                    # articles page
                    date = datetime.datetime.strptime(data[1].text, "%b %d, %Y").date()
                    if date < self.last:
                        self.finish()
                        return
                    t = SPQueryArticleTask(self.agent, [self.aurl + data[0].find("a").get("href").decode('utf8')])
                    t.delta = random.randrange(1, 86400)
                    t.schedule()
                elif len(data) == 3:
                    link = data[0].find("a").get("href").split("/")
                    # portfolio updates
                    user = link[-4]
                    portfolio = link[-2]
                    rating = data[1].text
                    date = datetime.datetime.strptime(data[2].text, "%B %d, %Y").date()
                    if date < self.last:
                        self.finish()
                        return
                    t = SPQueryPortfolioTask(self.agent, [user, portfolio])
                    t.delta = random.randrange(1, 86400)
                    t.schedule()
            except Exception as ex:
                logging.error("List Parse Exception: %s", str(ex))
                logging.error(str(row))

        # if we got here, it means there may be ungathered list items on the next page, so proceed with the next link
        logging.info("Continuing to next page")
        try:
            self.link = self.aurl + soup.find("img", {"alt": "next page"}).findPrevious("a").get("href")
        except Exception as ex:
            logging.info("No more pages")
            self.finish()
            return
        self.execute()

class SPQueryArticleTask(Task):
    taskName = "SPQueryArticle"
        
    def __init__(self, agent, args):
        super(SPQueryArticleTask, self).__init__(agent, args)
        self.link = args[0]

    def get_info(self):
        logging.info("Retrieving previous data")
        self.now = datetime.datetime.now().date()

        logging.debug("Checking for StockPickr article at %s", self.link)
        sql = "select id, last from spr_article where link=?;"
        res = self.db.query(sql, self.link.encode("ascii", "ignore"), one=True)
        if res:
            self.id = res[0]
            self.last = datetime.date(*[int(x) for x in res[1].split("-")])
        else:
            self.id = self.last = None

    def finish(self):
        art_col = ("id", "link", "title", "author", "date", "text", "notes", "last")
        art_ref_col = ("id", "stock")
        art_track_col = ("id", "last", "views")

        logging.info("Storing collected data")
        art = dict([(col, self.data[col]) for col in art_col])
        if not self.id:
            self.db.insert("spr_article", art)
            self.data["id"] = self.db.last_insert_id()
            for self.data["stock"] in self.data["stocks"]:
                art_ref = dict([(col, self.data[col]) for col in art_ref_col])
                self.db.insert("spr_article_reference", art_ref)

        logging.info("Marking article as updated")
        art_track = dict([(col, self.data[col]) for col in art_track_col])
        self.db.insert("spr_article_track", art_track)

    def execute(self):
        self.get_info()
        
        # if checked within last 24 hours, don't
        if self.last is not None and self.now == self.last:
            logging.info("Article checked today, aborting")
            return

        # get page
        logging.info("Scraping article %s", self.link)
        page = self.get_page(self.link)
        soup = BeautifulSoup.BeautifulSoup(page)
        
        # parse metadata
        logging.debug("Parsing header")
        body = soup.find(id="articleBlock")
        if body is None:
            logging.error("No article on page, is the link correct?")
            return

        temp = body.find("h1").text.split(" - ")
        title = ' - '.join(temp[0:-1])
        views = temp[-1]
        views = re.compile("\d*").findall(views.replace(",",""))[0] or '0'

        byline = body.find(attrs={"class":"byline"})
        byline = [line for line in byline if type(line) is not BeautifulSoup.Tag]
        authorPattern = re.compile("By (.+)")
        date = datetime.datetime.strptime(byline[-1].strip(), "%m/%d/%y - %I:%M %p %Z")
        try:
            author = authorPattern.findall(byline[0])[0]
        except:
            author = ''
        try:
            tickers = [t.text for t in body.find(attrs={"class":"tickers"}).findAll("a")]
        except:
            tickers = []

        # parse body
        logging.debug("Parsing body")
        paras = body.findAll("p")
        notes = text = ''
        ad = False
        for p in paras:
            ital = p.find("em")
            first = p.first()
            if first and ital == first:
                notes = notes + ' ' + p.text
            elif p.text.strip().startswith('----'):
                ad = not ad
            elif not ad:
                text = text + ' ' + p.text

        # wrap up
        columns = ("link", "title", "author", "date", "text", "notes", "id", "views", "stocks", "last")
        data = (\
            self.link, \
            title.encode("ascii", "ignore"), \
            author.encode("ascii", "ignore"), \
            date, \
            text.encode("ascii", "ignore"), \
            notes.encode("ascii", "ignore"), \
            self.id, \
            views, \
            tickers, \
            self.now)
        self.data = dict(zip(columns, data))
        self.finish()

class SPDumpTask(DumpTask):
    taskName = "SPDump"
    tables = [{ \
        'query': 'select %s from spr_user', \
        'drop': None, \
        'file': 'spr_user.csv', \
        'columns': ['uid', 'name'] \
    }, { \
        'query': 'select %s from spr_portfolio', \
        'drop': None, \
        'file': 'spr_portfolio.csv', \
        'columns': ['pid', 'uid', 'name', 'last'] \
    }, { \
        'query': 'select %s from spr_portfolio_track', \
        'drop': None, \
        'file': 'spr_portfolio_track.csv', \
        'columns': ['pid', 'date', 'description', 'views', 'user_rating', 'user_rating_count', 'pro_rating'] \
    }, { \
        'query': 'select %s from spr_position', \
        'drop': None, \
        'file': 'spr_position.csv', \
        'columns': ['pid', 'date', 'stock', 'shares', 'percentage', 'notes'] \
    }, { \
        'query': 'select %s from spr_article', \
        'drop': None, \
        'file': 'spr_article.csv', \
        'columns': ['id', 'link', 'title', 'author', 'date', 'text', 'notes'] \
    }, { \
        'query': 'select %s from spr_article_reference', \
        'drop': None, \
        'file': 'spr_article_reference.csv', \
        'columns': ['id', 'stock'] \
    }, { \
        'query': 'select %s from spr_article_track', \
        'drop': None, \
        'file': 'spr_article_track.csv', \
        'columns': ['id', 'last', 'views']}]
