import datetime
import cPickle as pickle
import base64
import urllib2
import logging

class Task(object):
    taskName = "NoOp"

    def __init__(self, agent, args):
        self.agent = agent
        self.args = args
        self.reschedules = False
        self.delta = 0
        self.tid = 0
        self.db = self.agent.db
        
    def schedule(self, go_now = False):
        logging.info("Scheduling Task in %ds: %s %s", 0 if go_now else self.delta, self.taskName, str(self.args))
        sql = "INSERT INTO tasks (task, after, reschedule, delta, args) VALUES (?,?,?,?,?);"
        if go_now:
            thedate = datetime.datetime.now()
        else:
            thedate = datetime.datetime.now() + datetime.timedelta(seconds=self.delta)
        params = [self.taskName, thedate, int(self.reschedules), self.delta, base64.b64encode(pickle.dumps(self.args))]
        self.db.query(sql, params)
        self.db.commit()

    def schedule_if_none_exist(self, check_args = False):
        logging.info("Scheduling Task %s if none exists", self.taskName)
        sql = "SELECT tid FROM tasks WHERE complete = 0 AND task = ? AND tid != ?"
        vals = [self.taskName, self.tid]
        if check_args:
            sql += " AND args = ?"
            vals.append(base64.b64encode(pickle.dumps(self.args)))
        if not self.db.bool_query(sql, vals):
            self.schedule()

    def complete(self, allow_reschedule=True):
        sql = "UPDATE tasks SET complete = ?, completed_on = ? WHERE tid = ?"
        params = [1, datetime.datetime.now(), self.tid]
        self.db.query(sql, params)
        self.db.commit()

        logging.info("Completed Task Successfully")
        # Reschedule
        if allow_reschedule and self.reschedules:
            logging.info("Rescheduling Task")
            self.schedule()

    def get_stock(self, symbol):
        logging.debug("Checking for stock %s", symbol)
        sql = "select sid from stock where symbol=?;"
        sid = self.db.query(sql, symbol, one=True)
        if not sid:
            logging.debug("New stock")
            self.db.insert('stock', {'symbol': symbol})
            sid = self.db.last_insert_id()
        return sid

    def followed_stocks(self):
        sql = "select symbol from stock where follow = 1;"
        stocks = self.db.query(sql)
        logging.info("Following Stocks: %s", str(stocks))
        return stocks

    def get_page(self, url):
        logging.debug("Retrieving URL: %s", url)
        header = {"User-Agent": "Mozilla/5.0 (X11; U; Linux x86_64; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.205 Safari/534.16"}
        request = urllib2.Request(url, headers=header)
        try:
            response = urllib2.urlopen(request, timeout=15)
            htmlSource = response.read()
            response.close()
        except urllib2.HTTPError as ex:
            logging.error("URL Exception: %s", str(ex))
            htmlSource = None
        return htmlSource
                
    def execute(self):
        logging.info("No-Op")


class DieTask(Task):
    taskName = "Die"

    def execute(self):
        logging.info("Stopping Agent...")
        self.agent.expire = True

class DumpTask(Task):
    taskName = "Dump"
    tables = []

    def get_info(self):
        logging.info("Loading database")
        for t in self.tables:
            t['query'] = t['query'] % ','.join(t['columns'])
            t['res'] = self.db.query(t['query'])
    
    def finish(self):
        logging.info("Removing old data")
        for t in self.tables:
            if t['drop']:
                self.db.query(t['drop'])

    def execute(self):
        self.get_info()
        
        logging.info("Writing CSVs")
        for t in self.tables:
            with open(t['file'], 'w') as f:
                writer = csv.writer(f)
                writer.writerow(t['columns'])
                writer.writerows(t['res'])

        self.finish()
