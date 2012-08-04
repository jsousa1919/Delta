import datetime
from Task import Task, DumpTask
import logging
from text import Company, Blog, Article


class TextTask(Task):
    def get_or_create_word(self, word):
        sql = "SELECT wid FROM word_bank WHERE word = ?"
        res = self.db.query(sql, word, one=True)
        if not res:
            self.db.insert("word_bank", {"word": word})
            res = self.db.last_insert_id()
        return res
        
    def get_keywords(self, kvid, sid):
        sql = "SELECT MAX(kvid) FROM keyword WHERE sid = ?"
        res = self.db.query(sql, sid, one=True)
        kvid = max(res, kvid)
        
        sql = "SELECT word FROM keyword WHERE kvid = ? AND sid = ?"
        return self.db.query(sql, [kvid, sid])

    def get_options(self):
        sql = "SELECT oid, did, kvid FROM representation WHERE rid = ?"
        (oid, did, kvid) = self.db.query(sql, self.repr, one=True)

        # get text options
        sql = "SELECT lowercase, stemmer, stem_titles FROM text_options WHERE oid = ?"
        (self.lowercase, self.stemmer, self.stem_titles) = self.db.query(sql, oid, one=True)

        # get sentiment dictonary
        sql = "SELECT DISTINCT word.word, rep.subjectivity, rep.polarity FROM word_representation rep, word_bank word WHERE did = ? AND word.wid = rep.wid"
        res = self.db.query(sql, did)
        self.dictionary = dict([(word, (subj, pol)) for (word,subj,pol) in res])

        # get company keywords
        self.companies = [Company.Company(symbol) for symbol in self.followed_stocks()]
        id_sql = "SELECT sid FROM stock WHERE symbol = ?"
        for co in self.companies:
            co.set_sid(self.db.query(id_sql, co.symbol, one=True))
            co.add_keywords(self.get_keywords(kvid, co.sid))

    def sentiment(self, wid):
        entry = self.dictionary.get(wid)
        return entry[1] if entry else 0

    def subjectivity(self, wid):
        entry = self.dictionary.get(wid)
        return entry[0] if entry else 0
    
    def get_text(self, table, rid, mx):
        sql = "SELECT id, date, text FROM %s WHERE id NOT IN (SELECT id FROM %s WHERE rid = ?) LIMIT ?"
        res = self.db.query(sql % (table, table + "_representation"), [rid, mx])
        return [{'id': tid, 'date': date, 'text': text} for (tid, date, text) in res]


class TextAnalysisTask(TextTask):
    taskName="TextAnalysis"

    def __init__(self, agent, args):
        super(TextAnalysisTask, self).__init__(agent, args)
        self.repr = int(args[0]) if len(args) > 0 else None
        if self.repr == "current": self.repr = None
        self.max = int(args[1]) if len(args) > 1 else 100

    def get_info(self):
        if not self.repr:
            sql = "select max(rid) from representation;"
            self.repr = self.db.query(sql, one=True)
        logging.info("Starting analysis with repr %d", self.repr)
        self.get_options()

    def analyze(self, table, text_class):
        corpus = self.get_text(table, self.repr, self.max)
        logging.info("Analyzing %d articles from %s", len(corpus), table)
        for article in corpus:
            t = text_class(None, article['date'], article['text'], bool(self.lowercase), self.stemmer, bool(self.stem_titles))
            t.extract_mentions(self.companies)
            res = t.process((lambda x: self.subjectivity(x)), (lambda x: self.sentiment(x)))
            for co in res:
                data = res[co]
                self.db.insert(table + "_representation", {
                    'rid': self.repr,
                    'id': article['id'],
                    'sid': co.sid,
                    'term_frequency': data['tf'],
                    'positive': data['positive'],
                    'negative': data['negative'],
                    'subjectivity': data['subjectivity'],
                    'contribution': data['contribution']
                })

    def execute(self):
        self.get_info()
        self.analyze('sh_article', Article.Article)
        self.analyze('spr_article', Article.Article)
        self.analyze('st_tweet', Blog.Blog)


