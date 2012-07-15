#! /usr/bin/env python

from csv import DictReader
from datetime import datetime
import os
import argparse

class StrategyData:

  def __init__(self, base, lag=1):
    self.prices = [line for line in DictReader(open(os.path.join(base, 'input.csv')))]
    pos = self.diff([float(line['pos']) for line in self.prices])[lag-1:]
    neg = self.diff([float(line['neg']) for line in self.prices])[lag-1:]
    change = [self.sign(abs(a) + abs(b)) for a,b in zip(pos,neg)]
    self.prices = self.prices[lag:]
    self.data = []
     # list of lists, 1 prediction for each piece of data
    self.predictions = [] 
    for i in range(len(self.prices)):
      self.predictions.append([])

    for pred in os.listdir(os.path.join(base, 'pred')):
      pred = os.path.join(base, 'pred', pred)
      reader = DictReader(open(pred))
      preds = [float(line['Predict']) for line in reader]
      preds = self.diff(preds, lag)
      for i in range(len(self.predictions)):
        self.predictions[i].append(preds[i])
      # self.predictions = [a + b for a,b in zip(self.predictions, preds)] if self.predictions else preds

    preds = iter(self.predictions)
    changes = iter(change)
    for price in self.prices:
      a = dict()
      a['date'] = datetime.strptime(price['date'], "%Y-%m-%d %H:%M:%S")
      #if not (a['date'].hour <= 16 and a['date'].hour >= 10):
      #if a['date'].hour is 16:
      if True:
        a['newSent'] = changes.next()
        a['ask'] = a['bid'] = float(price['open'])
        a['pred'] = preds.next()
        self.data.append(a)

  def diff(self, l, lag=1):
    a = l[lag:]
    b = l[:-lag]
    return [x - y for x,y in zip(a,b)]

  def sign(self, x):
    if x > 0: return 1
    elif x < 0: return -1
    else: return 0

parser = argparse.ArgumentParser()
parser.add_argument('stocks', metavar='S', type=str, nargs='+')
args = parser.parse_args()
stocks = args.stocks

for stock in stocks:
  a = StrategyData('../new_data/lag1-out/' + stock)
  from BasicStrategy import BasicStrategy
  from MomentumStrategy import MomentumStrategy
  from NewSentimentStrategy import NewSentimentStrategy
  from MomentumSentimentStrategy import MomentumSentimentStrategy
  from WeightedStrategy import WeightedStrategy
  b = WeightedStrategy(data=a.data, capital=0, unit=200, fee=0.005, minFee=1, stock=stock)
  b.run()
  print "Final Assets", b.assets[-1]
  print "Maximum Outstanding", b.maxDeficit
  print "Profit Factor", b.assets[-1] / b.maxDeficit
  print "Maximum Running Loss", b.runningLoss
  print "Running Loss Factor", b.runningLoss / b.maxDeficit
  print "Total Fees", b.totalFees
  print "Buys", b.buys
  print "Good Buys", b.goodBuys
  print "Shorts", b.shorts
  print "Good Shorts", b.goodShorts
  raw_input()
