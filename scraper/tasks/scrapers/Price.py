import datetime
import csv
import MySQLdb
import GooglePrice
import YahooPrice
import sys

default_start = datetime.datetime(2000, 1, 1)
maximum_gap = datetime.timedelta(7)

class Collector(object):
  
  def __init__(self, start = None):
    self.symbol = None
    self.data = dict()
    self.updates = dict()

    if start is None:
      start = default_start
    self.start = start
    self.today = datetime.datetime.now()

    self.sources = []
    self.sources.append(YahooPrice.Collector())
    self.sources.append(GooglePrice.Collector())


  def reset(self):
    self.data.clear()
    self.updates.clear()


  def importLol(self, symbol, lol):
    self.reset()
    self.symbol = symbol
    names = ('open', 'close', 'high', 'low', 'volume')
    for row in lol:
      self.data[row[0]] = dict(zip(names, row[1:]))   


  def update(self):
    if self.symbol is None:
      return
    self.today = datetime.datetime.now()

    for src in self.sources:
      for (start, end) in self.gaps():
        filler = src.collect(self.symbol, start, end)
        self.data = dict(self.data.items() + filler.items())
        self.updates = dict(self.updates.items() + filler.items())


  def collect(self, symbol=None):
    if self.symbol:
        self.reset()
        self.symbol = symbol
    self.update()


  def gaps(self):
    gaps = list()
    dates = sorted(self.data)
    dates.append(self.today)
    start = self.start

    for end in dates:
      if end - start > maximum_gap:
        gaps.append((start, end))
      start = end
    
    return gaps
