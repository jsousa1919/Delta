import sqlite3
import nltk
import Company
from Article import Article
from Blog import Blog
import KMeans
import datetime
import csv
import math

db_file = '../db.sqlite'
db = sqlite3.connect(db_file)
cur = db.cursor()

class Process:
  word_freq = nltk.FreqDist()
  doc_freq = nltk.FreqDist()
  corpus = {}

  def initialize_companies(self):
    cur.execute('select symbol from stock where follow = 1;')
    companies = cur.fetchall()
    self.companies = [Company.from_symbol(sym[0]) for sym in companies]

  def initialize_text(self, text_list, text_class):
    i = last = 0.0
    corpus = []

    for text in text_list:
      # just some progess display
      perc = 100 * i / len(text_list)
      if perc - last > 5:
        last = perc
        print "%d%% done" % perc
      i += 1

      # initialize article
      text = text_class(*text)
      text.extract_mentions(self.companies)
      text.calculate_metrics()
      corpus.append(text)

      # modify global counts
      for word in text.freq_dist:
        self.word_freq.inc(word, text.freq_dist[word])
        self.doc_freq.inc(word)

    return corpus

  def initialize_corpus(self, from_date = None, to_date = None):
    where = ""
    if to_date and from_date:
      where = " where date > '%s' and date < '%s'" % (from_date, to_date)
    elif from_date:
      where = " where date > '%s'" % (from_date)
    elif to_date:
      where = " where data < '%s'" % (to_date)

    print "Getting StockHouse Articles"
    cur.execute("select headline, date, article from sh_article" + where)
    data = cur.fetchall()
    data = self.initialize_text(data, Article)
    self.corpus['stockhouse'] = data

    # data collection died in october
    print "Getting StockPickr Articles"
    cur.execute("select title, date, text from spr_article" + where)
    data = cur.fetchall()
    data = self.initialize_text(data, Article)
    self.corpus['stockpickr'] = data

    print "Getting Tweets"
    cur.execute("select NULL, date, text from st_tweet" + where)
    data = cur.fetchall()
    data = self.initialize_text(data, Blog)
    self.corpus['twitter'] = data

  def process(self, corpus, subjmod, **args):
    i = last = 0.0
    data = dict([(co, {}) for co in self.companies])
    print "Processing"

    for text in corpus:
      # just some progess display
      perc = 100 * i / len(corpus)
      if perc - last > 5:
        last = perc
        print "%d%% done" % perc
      i += 1

      values = text.process(subjmod, **args)
      date = text.date + datetime.timedelta(hours = 1)
      date = date.replace(minute = 0, second = 0)
      date = str(date)
      
      for co in values:
        if date not in data[co]: data[co][date] = values[co]
        else: data[co][date] = [x + y for x,y in zip(data[co][date], values[co])]
        
    return data

  # TODO modify to accept mode parameter (update or clean start)
  def __init__(self, from_date = None, to_date = None):
    self.initialize_companies()  
    self.initialize_corpus(from_date, to_date)

    i = 0
    for func in [lambda x: 1.0, lambda x: x, lambda x: x + 1, lambda x: math.log(x + 1), lambda x: math.log(x + 2), lambda x: math.sqrt(x), lambda x: math.sqrt(x + 1), lambda x: math.pow(x,2), lambda x: math.pow(x + 1,2)]:
      for const in [0.0, 0.25, 0.5, 0.75, 1.0]:
        print i, const
        data = {}
        for source in self.corpus:
          data[source] = self.process(self.corpus[source], func, glob_const = const)
      
        with open(str(i) + '-' + str(const) + '.csv', 'w') as f:
          out = csv.writer(f)
          out.writerow(['symbol', 'source', 'date', 'tf', 'df', 'pos', 'neg'])
          for source in data:
            for co in data[source]:
              for date in sorted(data[source][co]):
                out.writerow([co.symbol, source, date, str(data[source][co][date][0]), str(data[source][co][date][3]), str(data[source][co][date][1]), str(data[source][co][date][2])])
      i += 1


start = datetime.datetime(2011, 7, 1)
end = datetime.datetime(2012, 4, 1)
Process(start, end)
