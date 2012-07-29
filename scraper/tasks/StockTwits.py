import csv
import datetime
import re
from BeautifulSoup import BeautifulSoup
from Task import Task, DumpTask
import random
import logging
import json
import urllib2

class STTask(Task):
    def get_user(self, username):
        logging.debug("Checking for StockTwits User %s", username)
        sql = "select uid from st_user where name=?;"
        uid = self.db.query(sql, username, one=True)
        if not uid:
            logging.debug("New User")
            self.db.insert('st_user', {'name': username})
            uid = self.db.last_insert_id()
        return uid

    def stock_update(self, sid):
        logging.debug("Checking last StockTwits update for stock id %s", sid)
        sql = "select last_stocktwits from stock where sid=?;"
        res = self.db.query(sql, sid, one=True)
        return datetime.datetime.strptime(res, "%Y-%m-%d %H:%M:%S.%f") if res else None

    def user_update(self, uid):
        logging.debug("Checking last StockTwits update for user id %s", uid)
        sql = "select last from st_user where uid=?;"
        res = self.db.query(sql, uid, one=True)
        return datetime.datetime.strptime(res, "%Y-%m-%d").date() if res else None

class STScrapeTask(STTask):
    """
    Scrape StockTwits.com for every company on our list every hour
    and update user followings if they have not yet been updated
    today.  Also update Trending Now if it has not been updated recently.
    """    
    taskName="STScrape"
    
    def __init__(self, agent, args):
        super(STScrapeTask, self).__init__(agent, args)
        self.history = args[0] if len(args) > 0 else None
        
    def execute(self):
        logging.info("Scraping StockTwits")
        companies = self.followed_stocks() # ['ZIP', 'LNKD', 'GIS', 'MHP', 'NKE', 'RL', 'VZ', 'AAPL', 'CAT']
        for company in companies:
            if self.history:
                # collect historical StockTwit information 
                newTweets = STQueryStockTask(self.agent, [company, 'history'])
                newTweets.delta = random.randrange(1,86400)
                newTweets.schedule() 
                
            else:
                # collect all new StockTwit information 
                newTweets = STQueryStockTask(self.agent, [company])
                newTweets.delta = random.randrange(1,3600)
                newTweets.schedule() 

        newTN = STQueryTrendingNowTask(self.agent, [])
        newTN.schedule()
            
class STQueryStockTask(STTask):
    taskName = "STQueryStock"
    turl = "http://stocktwits.com/symbol/%s"
    surl = "http://stocktwits.com/streams/stream?stream=stock&stream_id=%s"
    nurl = "http://stocktwits.com/streams/poll?stream=symbol&max=%s&item_id=%s"
    data_col = ('uid', 'date', 'text', 'refs')
    
    def __init__(self, agent, args):
        super(STQueryStockTask, self).__init__(agent, args)
        self.symbol = args[0]
        self.poll_id = args[1] if len(args) > 1 else None
        self.max = args[2] if len(args) > 2 else None
        self.data = []     

    def get_info(self):
        self.now = datetime.datetime.now()
        logging.info("Retrieving previous data")

        self.id = self.get_stock(self.symbol)
        self.last = self.stock_update(self.id)

    def __add_tweet(self, user, date, msg, refs):
        uid = self.get_user(user)
        
        sql = "select * from st_tweet where uid=? and date=?"
        duplicate = self.db.bool_query(sql, [uid, date])

        if not duplicate:
            data = (uid, date, msg, refs)
            self.data.append(dict(zip(self.data_col, data)))

        last = self.user_update(uid)
        if last and self.now.date() != last:
            #Query User
            newUserInfo = STQueryUserTask(self.agent, [user])
            newUserInfo.delta = random.randrange(1,3600)
            newUserInfo.schedule()

    def finish(self):
        tweet_col = ("uid", "date", "text")
        tweet_ref_col = ("id", "sid")
        track_col = ("sid", "start", "end", "count")

        logging.info("Storing collected data")
        for data in self.data:
            self.db.insert('st_tweet', dict([(col, data[col]) for col in tweet_col]))
            data['id'] = self.db.last_insert_id()

            refs = [self.get_stock(symbol) for symbol in data['refs']]
            for data['sid'] in refs:
                self.db.insert('st_tweet_ref', dict([(col, data[col]) for col in tweet_ref_col]))

        self.db.insert('st_track', dict(zip(track_col, (self.id, self.last, self.now, len(self.data)))))
        
        logging.info("Marking stock as updated")
        sql = "update stock set last_stocktwits = ? where sid = ?;"
        self.db.query(sql, [self.now, self.id])
        
    def execute(self):
        self.get_info()
        logging.info("Checking new StockTwits for %s", self.symbol)
        nextMax = False
        history = (self.poll_id == "history") or bool(self.max)

        # get page
        if not (self.poll_id and self.max):
            url = self.turl % self.symbol
            page = self.get_page(url)
            self.poll_id = re.findall("'symbol', (\d+)\)", page)[0]

        head = {'X-Requested-With': 'XMLHttpRequest'}
        if self.max:
            req = urllib2.Request(self.nurl % (self.max, self.poll_id), headers=head)
        else:
            req = urllib2.Request(self.surl % self.poll_id, headers=head)
        data = urllib2.urlopen(req).read()
        js = json.loads(data)
        content = js['messages']

        if history and js['more']:
            if 'max_id' in js:
                nextMax = js['max_id']
            elif 'max' in js:
                nextMax = js['max']
            else:
                nextMax = js['stream']['max_id']

        # parse
        logging.debug("Parsing")
        for message in content:
            username = message['user']['username'] #get username

            date = message['created_at']
            date = datetime.datetime.strptime(' '.join(date.split(' ')[:-1]), "%a, %d %b %Y %H:%M:%S")

            tweet = message['body']
            ticker_pattern = re.compile("\$([A-Z]+(?:\.[A-Z]+)?)")
            refs = ticker_pattern.findall(tweet)

            self.__add_tweet(username, date, tweet, refs)

        self.finish()
        if nextMax:
            t = STQueryStockTask(self.agent, [self.symbol, self.poll_id, nextMax])
            t.delta = random.randrange(20,60)
            t.schedule()
        

class STQueryUserTask(STTask):
    taskName = "STQueryUser"

    def __init__(self, agent, args):
        super(STQueryUserTask, self).__init__(agent, args)
        self.user = self.args[0]

    def get_info(self):
        self.now = datetime.datetime.now().date()
        logging.info("Retrieving previous data")

        self.id = self.get_user(self.user)
        self.last = self.user_update(self.id)

    def finish(self):
        logging.info("Storing collected data")
        self.db.insert('st_user_track', {"uid": self.id, "date": self.now, "followers": self.followers})

        logging.info("Marking user as updated")
        sql = "update st_user set last = ? where uid = ?;"
        self.db.query(sql, [self.now, self.id])
        
    def execute(self):
        self.get_info()
        if self.last and self.now == self.last:
            logging.info("User checked today, aborting")
            return

        # get page
        logging.info("Scraping user %s", self.user)
        pageURL = "http://stocktwits.com/" + self.user
        soup = BeautifulSoup(self.get_page(pageURL))

        # get user's following
        logging.debug("Parsing")
        followers = soup.find(attrs={"class": "follow-number"})
        if followers is not None:
            self.followers = int(re.sub("\D","", followers.text))
        elif followers is None:
            try:
                self.followers = int(re.sub("\D","", soup.find(id="traderStats").find(href=re.compile("followers$")).find(attrs={"class": "black"}).text))
            except:
                logging.error("No followers on page")
                return

        self.finish()
        
class STQueryTrendingNowTask(STTask):
    taskName="STQueryTrendingNow"
    freq = datetime.timedelta(hours=1)
    url = "http://stocktwits.com/"

    def __init__(self, agent, args):
        super(STQueryTrendingNowTask, self).__init__(agent, args)

    def get_info(self):
        self.now = datetime.datetime.now()
        logging.info("Retrieving previous data")
        return not self.db.bool_query("SELECT * FROM st_trending WHERE date > ?", self.now - self.freq)
        
    def finish(self):
        logging.info("Storing collected data")
        cur = self.db.insert('st_trending', {'date': self.now})
        self.id = self.db.last_insert_id()
        self.data = [self.get_stock(symbol) for symbol in self.data]
        
        for sid in self.data:
            self.db.insert('st_trending_ref', {'trid': self.id, 'sid': sid})

    def execute(self):
        if not self.get_info():
            logging.info("Trending Now checked recently, aborting")
            return

        logging.info("Querying StockTwits Trending Now")
        soup = BeautifulSoup(self.get_page(self.url))

        logging.debug("Parsing")
        self.data = []
        for sym in soup.find(attrs={"class": "scrollableArea"}).findAll("p"):
            if sym.text.startswith("$"):
                self.data.append(sym.text[1:])

        self.finish()
            

class STDumpTask(DumpTask):
    taskName = "STDump"
    tables = [{ \
        'query': 'select %s from st_user', \
        'drop': None, \
        'file': 'st_user.csv', \
        'columns': ['uid', 'name', 'last']
    }, { \
        'query': 'select %s from st_user_track', \
        'drop': None, \
        'file': 'st_user_track.csv', \
        'columns': ['uid', 'date', 'followers'] \
    }, { \
        'query': 'select %s from st_tweet', \
        'drop': None, \
        'file': 'st_tweet.csv', \
        'columns': ['id', 'uid', 'date', 'text'] \
    }, { \
        'query': 'select %s from st_tweet_ref', \
        'drop': None, \
        'file': 'st_tweet_ref.csv', \
        'columns': ['id', 'sid'] \
    }, { \
        'query': 'select %s from st_track', \
        'drop': None, \
        'file': 'st_track.csv', \
        'columns': ['sid', 'start', 'end', 'count'] \
    }, { \
        'query': 'select %s from st_trending', \
        'drop': None, \
        'file': 'st_trending.csv', \
        'columns': ['trid', 'date'] \
    }, { \
        'query': 'select %s from st_trending_ref', \
        'drop': None, \
        'file': 'st_trending_ref.csv', \
        'columns': ['trid', 'sid']}]
