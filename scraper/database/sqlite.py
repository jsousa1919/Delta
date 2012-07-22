#
# Author: Edward Benson <eob@csail.mit.edu>
#

import sqlite3
import os
import logging

class database:
    
    def __init__(self, params):
        self.dbfile = params[0]
        self.db = None
        self.connect()
        self.load_schema()
        
    def connect(self):
        if os.path.exists(self.dbfile):
            self.db = sqlite3.connect(self.dbfile)
        else:
            self.db = sqlite3.connect(self.dbfile)
            self.db.commit()
        self.cur = None

    def ensure_connection(self):
        if not self.cur:
          self.cur = self.db.cursor()
    
    def commit(self):
        self.db.commit()

    def rollback(self):
        self.db.rollback()

    def end(self):
        if self.cur:
            self.cur.close()
            self.cur = None

    def close(self):
        self.db.close()
    
    def bool_query(self, query, params=None):
        self.ensure_connection()
        res = self.query(query, params)
        has_result = bool(res)
        return has_result
        
    def query(self, query, params=None, one=False):
        try:
            self.ensure_connection()
            if type(query) == str:
                if params:
                    if type(params) == list:
                        self.cur.execute(query, params)
                    else:
                        self.cur.execute(query, [params])
                else:
                    self.cur.execute(query)
            elif type(query) == list:
                fields = []
                values = []
                questions = []
                for k,v in params.items():
                    fields.append("%s = ?" % k)
                    values.append(v)

                params = (', '.join(query[1:]), query[0], " AND ".join(fields))
                sql = "SELECT %s FROM %s WHERE %s;" % params
                self.cur.execute(sql, values)

            res = self.cur.fetchall()
            # if only one field was selected, take it out of the tuple
            if res and res[0] and len(res[0]) == 1:
                res = [row[0] for row in res]
            # if only one result requested, take it out of the list
            # return no result as None for one request, empty list otherwise
            if one:
                res = res[0] if res else None
            elif res is None:
                res = []
            return res

        except Exception as ex:
            logging.error("Database Query Exception: %s", str(ex))
            logging.error("Query: %s", str(query))
            logging.error("Params: %s", str(params))
            self.connect()
            return None
        
    def safe_insert(self, table, ids, data):
        if not self.bool_query([table, '*'], ids):
            self.insert(table, dict(ids.items() + data.items()))
        else:
            self.update(table, data, ids)

    def insert(self, table, dic, on_conflict=None):
        #try:
        self.ensure_connection()
        fields = []
        values = []
        for k,v in dic.items():
            fields.append(k)
            values.append(v)
        questions = ['?' for v in values]
        if on_conflict == "replace":
            oncon = "OR REPLACE "
        elif on_conflict == "ignore":
            oncon = "OR IGNORE "
        else:
            oncon = ""
        sql = "INSERT " + oncon + "INTO %s (%s) VALUES (%s)" % (table, ', '.join(fields), ', '.join(questions))
        self.cur.execute(sql, values)
        #except Exception as ex:
        #    logging.error("Database Insert Exception: %s", str(ex))
        #    logging.error("Table: %s", str(table))
        #    logging.error("Params: %s", str(dic))
        #    self.connect()

    def update(self, table, data, ids = {}):
        #try:
        self.ensure_connection()
        set_fields = []
        where_fields = []
        values = []
        for k,v in data.items():
            set_fields.append("%s = ?" % k)
            values.append(v)
        for k,v in ids.items():
            where_fields.append("%s = ?" % k)
            values.append(v)
        sql = "UPDATE %s SET %s" % (table, ', '.join(set_fields))
        if where_fields:
            sql += " WHERE %s" % (' AND '.join(where_fields))
        self.cur.execute(sql, values)
            
    def last_insert_id(self):
        try:
            self.ensure_connection()
            self.cur.execute("SELECT last_insert_rowid() as last;")  
            rowid = self.cur.fetchone()[0]
            return rowid
        except Exception as ex:
            logging.error("Database ID Exception: %s", str(ex))
            self.connect()
            return None
            
    def load_schema(self):
        self.ensure_connection()
        self.cur.execute(self.tasks_schema())
        self.cur.execute(self.stock_schema()) 
        self.cur.execute(self.keyword_schema())
        self.cur.execute(self.keyword_version_schema())
        self.cur.execute(self.representation_schema())
        self.cur.execute(self.word_bank_schema())
        self.cur.execute(self.dictionary_schema())
        self.cur.execute(self.word_representation_schema())
        self.cur.execute(self.text_options_schema())
        self.cur.execute(self.spr_user_schema()) 
        self.cur.execute(self.spr_portfolio_schema()) 
        self.cur.execute(self.spr_portfolio_track_schema()) 
        self.cur.execute(self.spr_position_schema())
        self.cur.execute(self.spr_updates_schema()) 
        self.cur.execute(self.spr_article_schema()) 
        self.cur.execute(self.spr_article_reference_schema()) 
        self.cur.execute(self.spr_article_representation_schema())
        self.cur.execute(self.spr_article_track_schema()) 
        self.cur.execute(self.gtrends_schema()) 
        self.cur.execute(self.st_user_schema()) 
        self.cur.execute(self.st_user_track_schema()) 
        self.cur.execute(self.st_tweet_schema()) 
        self.cur.execute(self.st_tweet_ref_schema()) 
        self.cur.execute(self.st_tweet_representation_schema())
        self.cur.execute(self.st_track_schema()) 
        self.cur.execute(self.st_trending_schema()) 
        self.cur.execute(self.st_trending_ref_schema()) 
        self.cur.execute(self.sh_article_schema())
        self.cur.execute(self.sh_article_ref_schema()) 
        self.cur.execute(self.sh_article_representation_schema())
        self.cur.execute(self.sh_track_schema()) 
        self.cur.execute(self.fi_track_schema())
        self.cur.execute(self.price_history_schema())
        self.cur.execute(self.sopi_stock_track_schema())
        self.cur.execute(self.gtrends_throttle_schema())
        
        self.db_init()
        self.db.commit()
        self.end()

    def db_init(self):
        self.cur.execute("INSERT OR IGNORE INTO spr_updates (name, link, last) VALUES ('DIY', 'http://www.stockpickr.com/list/latestdyi/', '2000-01-01');")
        self.cur.execute("INSERT OR IGNORE INTO spr_updates (name, link, last) VALUES ('PRO', 'http://www.stockpickr.com/list/latestpro/', '2000-01-01');")
        self.cur.execute("INSERT OR IGNORE INTO spr_updates (name, link, last) VALUES ('DAILY', 'http://www.stockpickr.com/list/today/', '2000-01-01');")
        self.cur.execute("INSERT OR IGNORE INTO spr_updates (name, link, last) VALUES ('BLOGS', 'http://www.stockpickr.com/list/problog/', '2000-01-01');")

    def tasks_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS tasks (
            tid integer PRIMARY KEY AUTOINCREMENT,
            task          varchar(255),
            after         datetime,
            complete      integer default 0,
            completed_on  datetime,
            args          text,
            delta         text,
        );
        """

    def spr_user_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS spr_user (
            uid integer PRIMARY KEY AUTOINCREMENT,
            name varchar(32),
            follow boolean default 0
        );
        """

    def spr_portfolio_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS spr_portfolio (
            pid integer PRIMARY KEY AUTOINCREMENT,
            uid integer,
            name varchar(64),
            last date
        );
        """

    def spr_portfolio_track_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS spr_portfolio_track (
            pid integer,
            date date,
            description text,
            views integer,
            user_rating real,
            user_rating_count integer,
            pro_rating integer
        );
        """

    def spr_position_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS spr_position (
            pid integer,
            date date,
            stock varchar(8),
            shares integer,
            percentage real,
            notes text
        );
        """

    def spr_updates_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS spr_updates (
            name varchar(32) UNIQUE,
            link varchar(64) UNIQUE,
            last date
        );
        """

    def spr_article_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS spr_article (
            aid integer PRIMARY KEY AUTOINCREMENT,
            link varchar(128) UNIQUE,
            title varchar(128),
            author varchar(64),
            date date,
            text text,
            notes text,
            last date,
            doc integer
        );
        """

    def spr_article_reference_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS spr_article_reference (
            aid integer,
            stock varchar(8)
        );
        """
    
    def spr_article_representation_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS spr_article_representation (
            rid integer,
            aid integer,
            sid integer,
            term_frequency integer,
            positive integer,
            negative integer,
            subjectivity integer,
            contribution real
        );
        """

    def spr_article_track_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS spr_article_track (
            aid integer,
            last date,
            views integer
        );
        """
            
    def stock_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS stock (
            sid integer PRIMARY KEY AUTOINCREMENT,
            symbol varchar(8),
            name varchar(48), 
            flameindex_link varchar(64),
            last_pricing date,
            last_gtrends date,
            last_stocktwits datetime,
            last_socialpicks datetime,
            last_stockhouse datetime,
            last_flameindex date,
            follow boolean
        );
        """
		

    def gtrends_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS gtrends (
            sid integer,
            date date,
            searches real,
            error varchar(10),
            PRIMARY KEY (sid, date)
        );
        """

    def gtrends_throttle_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS gtrends_throttle (
            last datetime
        );
        """
    
    def st_user_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS st_user (
            uid integer PRIMARY KEY AUTOINCREMENT,
            name varchar(32),
            last date
        );
        """

    def st_user_track_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS st_user_track (
            uid integer,
            date date,
            followers integer
        );
        """

    def st_tweet_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS st_tweet (
            tid integer PRIMARY KEY AUTOINCREMENT,
            uid integer,
            date datetime,
            text text,
            doc integer
        );
        """

    def st_tweet_ref_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS st_tweet_ref (
            tid integer,
            sid integer
        );
        """
    
    def st_tweet_representation_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS st_tweet_representation (
            rid integer,
            tid integer,
            sid integer,
            term_frequency integer,
            positive integer,
            negative integer,
            subjectivity integer,
            contribution real
        );
        """

    def st_track_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS st_track (
            sid integer,
            start datetime,
            end datetime,
            count integer
        );
        """

    def st_trending_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS st_trending (
            trid integer PRIMARY KEY AUTOINCREMENT,
            date datetime
        );
        """

    def st_trending_ref_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS st_trending_ref (
            trid integer,
            sid integer
        );
        """

    def sopi_stock_track_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS sopi_stock_track (
            sid integer,
            date datetime,
            rating integer
        );
        """
    
    def sh_article_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS sh_article (
            aid integer PRIMARY KEY AUTOINCREMENT,
            date datetime,
            headline text,
            article text,
			      link text,
            doc integer
        );
        """
    
    def sh_article_ref_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS sh_article_ref (
            aid integer,
            sid integer
        );
        """
    
    def sh_article_representation_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS sh_article_representation (
            rid integer,
            aid integer,
            sid integer,
            term_frequency integer,
            positive integer,
            negative integer,
            subjectivity integer,
            contribution real
        );
        """
            
    
    def sh_track_schema(self):
		return """
        CREATE TABLE IF NOT EXISTS sh_track (
            sid integer,
            start datetime,
            end datetime,
            count integer
        );
        """

    def fi_track_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS fi_track (
            sid integer,
            date datetime,
            rank integer,
            rating real
        );
        """

    def price_history_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS price_history (
            sid integer,
            date date,
            open real,
            close real,
            high real,
            low real,
            volume integer
        );
        """
    
    def keyword_version_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS keyword_version (
            kvid integer PRIMARY KEY AUTOINCREMENT,
            date datetime,
            description text
        );
        """

    def keyword_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS keyword (
            kvid integer,
            sid integer,
            type varchar(32),
            word varchar(64)
        );
        """
    
    def representation_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS representation (
            rid integer PRIMARY KEY AUTOINCREMENT,
            oid integer,
            did integer,
            kvid integer,
            date datetime
        );
        """
    
    def word_bank_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS word_bank (
            wid integer PRIMARY KEY AUTOINCREMENT,
            word varchar(64),
            form_of integer
        );
        """
    
    def dictionary_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS dictionary (
            did integer PRIMARY KEY AUTOINCREMENT,
            date datetime,
            description text
        );
        """
    
    def word_representation_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS word_representation (
            did integer,
            wid integer,
            subjectivity integer,
            polarity integer,
            PRIMARY KEY (did, wid)
        );
        """
    
    def text_options_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS text_options (
            oid integer PRIMARY KEY AUTOINCREMENT,
            date datetime,
            description text,
            lowercase boolean,
            stemmer varchar(32),
            stem_titles boolean
        );
        """
