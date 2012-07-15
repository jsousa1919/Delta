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
        self.cur.execute(self.events_schema())
        self.cur.execute(self.queries_schema())
        self.cur.execute(self.tasks_schema())
        self.cur.execute(self.people_schema())
        self.cur.execute(self.at_mentions_schema())
        self.cur.execute(self.tweets_schema())
        self.cur.execute(self.places_schema())
        self.cur.execute(self.container_schema())
        self.cur.execute(self.tweet_place_schema())
        self.cur.execute(self.social_graph())
        self.cur.execute(self.feeds_schema()) 
        self.cur.execute(self.spr_user_schema()) 
        self.cur.execute(self.spr_portfolio_schema()) 
        self.cur.execute(self.spr_portfolio_track_schema()) 
        self.cur.execute(self.spr_position_schema())
        self.cur.execute(self.spr_updates_schema()) 
        self.cur.execute(self.spr_article_schema()) 
        self.cur.execute(self.spr_article_reference_schema()) 
        self.cur.execute(self.spr_article_track_schema()) 
        self.cur.execute(self.stock_schema()) 
        self.cur.execute(self.gtrends_schema()) 
        self.cur.execute(self.st_user_schema()) 
        self.cur.execute(self.st_user_track_schema()) 
        self.cur.execute(self.st_tweet_schema()) 
        self.cur.execute(self.st_tweet_ref_schema()) 
        self.cur.execute(self.st_track_schema()) 
        self.cur.execute(self.st_trending_schema()) 
        self.cur.execute(self.st_trending_ref_schema()) 
        self.cur.execute(self.sh_article_schema())
        self.cur.execute(self.sh_article_ref_schema()) 
        self.cur.execute(self.sh_track_schema()) 
        self.cur.execute(self.fi_track_schema())
        self.cur.execute(self.price_history_schema())
        self.cur.execute(self.sopi_stock_track_schema())
        self.cur.execute(self.gtrends_throttle_schema())
        self.cur.execute(self.keyword_schema())
        
        self.db_init()
        self.db.commit()
        self.end()

    def db_init(self):
        self.cur.execute("INSERT OR IGNORE INTO spr_updates (name, link, last) VALUES ('DIY', 'http://www.stockpickr.com/list/latestdyi/', '2000-01-01');")
        self.cur.execute("INSERT OR IGNORE INTO spr_updates (name, link, last) VALUES ('PRO', 'http://www.stockpickr.com/list/latestpro/', '2000-01-01');")
        self.cur.execute("INSERT OR IGNORE INTO spr_updates (name, link, last) VALUES ('DAILY', 'http://www.stockpickr.com/list/today/', '2000-01-01');")
        self.cur.execute("INSERT OR IGNORE INTO spr_updates (name, link, last) VALUES ('BLOGS', 'http://www.stockpickr.com/list/problog/', '2000-01-01');")

    def tweets_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS tweets (
            tid integer PRIMARY KEY AUTOINCREMENT,
            pid            integer,
            qid            integer,
            twitter_id      bigint,
            queried_at     datetime,
            tweet          varchar(255),
            retweet_of     integer default 0,
            reply_of       integer default 0,
            created_at     datetime,
            language      varchar(5),
            geo_lat     double,
            geo_lng     double,
            geo_type    varchar(100),
            geo_id      varchar(100),
            geo_name    varchar(200),
            estimated_loc varchar(200),
            source varchar(255)
        );
        """        

    def at_mentions_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS at_mentions (
            tid integer,
            pid integer
        );
        """

    def people_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS people (
            pid integer PRIMARY KEY AUTOINCREMENT,
            twitter_id integer,
            twitter_name varchar(255),
            user_location varchar(255)
        );
        """

    def tweet_place_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS tweets_places (
            tid integer,
            pid integer
        );
        """
       
    def feeds_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS feeds (
            fid integer,
            feed varchar(255),
            posted datetime,
            title text,
            link text,
            summary text,
            tags text
        );
        """

    # Note: created_at is UTC
    def places_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS places (
            pid integer PRIMARY KEY AUTOINCREMENT,
            name          varchar(255),
            twitter_id    varchar(255),
            country       varchar(255),
            country_code  varchar(5),
            place_type    varchar(255),
            url           varchar(255),
            full_name     varchar(255),
            geo_shape     varchar(255),
            geo_lat       double,
            geo_lng       double,
            geo_box_1_lat double,
            geo_box_2_lat double,
            geo_box_3_lat double,
            geo_box_4_lat double,
            geo_box_1_lng double,
            geo_box_2_lng double,
            geo_box_3_lng double,
            geo_box_4_lng double
        );
        """

    def container_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS spatial_relationships (
            container integer,
            contained integer
        );
        """

    def social_graph(self):
        return """
        CREATE TABLE IF NOT EXISTS graph (
            follower integer,
            followed integer,
            edge     integer default 0
        );
        """

    def events_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS events (
            eid integer PRIMARY KEY AUTOINCREMENT,
            source        varchar(255),
            title         varchar(255),
            venue         varchar(255),
            address1      varchar(255),
            address2      varchar(255),
            description   text,
            url           varchar(255),
            event_time    datetime,
            ticket_price  float default 0
        );
        """
        
    def queries_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS queries (
            qid integer PRIMARY KEY AUTOINCREMENT,
            eid           integer default 0,
            query         varchar(255),
            start_date    datetime,
            stop_date     datetime
        );
        """

    def tasks_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS tasks (
            tid integer PRIMARY KEY AUTOINCREMENT,
            task          varchar(255),
            after         datetime,
            reschedule    integer default 0,
            complete      integer default 0,
            completed_on  datetime,
            args,         text,
            delta         integer
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

    def keyword_schema(self):
        return """
        CREATE TABLE IF NOT EXISTS keyword (
            sid integer,
            type text,
            word text
        );
        """
