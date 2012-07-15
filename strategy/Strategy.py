from rpy import *
# from pprint import pprint

class Strategy:
  
  def __init__(self, data, capital=0, unit=1, lag=1, fee=0.05, minFee=1, stock='Stock'):
    # initial data
    self.data = sorted(data, key=lambda k: k['date']) # list of dicts holding date, price, and prediction data; make sure list is sorted by date
    self.capital = self.running = capital # starting capital for strategy
    self.unit = unit # minimum number of shares per trade, not mandatory
    self.lag = lag # minumum expiration, not mandatory
    self.fee = fee # pennies per share to fees
    self.minFee = minFee # minimum fees per trade
    self.stock = stock # name of stock

    # tracking data
    self.current = 0  # index of current piece of data
    self.assets = []
    self.runningLoss = self.maxDeficit = 0
    self.buys = 0
    self.goodBuys = 0
    self.shorts = 0
    self.goodShorts = 0
    self.totalFees = 0
    self.limbo = {'buy': [], 'short': []}  # outstanding trades, buys and shorts, with price, amount, and expiration


  # trade a certain amount of stock, incorporating fees
  # amount should be negative for sell
  def trade(self, price, amount):
    transaction = price * amount
    fees = self.fees(amount)
    self.totalFees += fees
    self.running -= (transaction + fees)

  # calculate fees on a transaction given the dollar amount
  def fees(self, shares):
    return max(abs(self.fee * shares), self.minFee)

  # return whether a transaction was profitable
  def good(self, buy, sell, amount):
     return sell * amount > (buy * amount - (self.fees(amount) + self.fees(amount)))

  # perform a buy, set expires if the transaction should be reversed after a certain time
  def buy(self, amount, expires=None):
    self.buys += 1
    price = self.working['ask']
    self.trade(price, amount)
    self.limbo['buy'].append({'price': price, 'amount': amount, 'expires': expires})

  # perform a short, similar to buy()
  def short(self, amount, expires=None):
    self.shorts += 1
    price = self.working['bid']
    self.trade(price, -amount)
    self.limbo['short'].append({'price': price, 'amount': amount, 'expires': expires})

  # sell all long positions
  # must fix 'goodBuys' for consolidated sell if working with small transaction units (< ~$200)
  def sellAll(self):
    amount = sum(trans['amount'] for trans in self.limbo['buy'])
    self.goodBuys += sum(1 for trade in self.limbo['buy'] if self.good(trade['price'], self.working['bid'], trade['amount']))
    self.trade(self.working['bid'], -amount)
    self.limbo['buy'] = []

  # cover all short positions, similar to sellAll()
  def coverAll(self):
    amount = sum(trans['amount'] for trans in self.limbo['short'])
    self.goodShorts += sum(1 for trade in self.limbo['short'] if self.good(self.working['ask'], trade['price'], trade['amount']))
    self.trade(self.working['ask'], amount)
    self.limbo['short'] = []  
  
  # finish expired trades
  def expire(self):
    newLimbo =  {'buy': [], 'short': []}

    for trade in self.limbo['buy']:
      # trade has expired
      if trade['expires'] is 0:
        self.trade(self.working['bid'], -trade['amount'])
        if self.good(trade['price'], self.working['bid'], trade['amount']):
          self.goodBuys += 1
      # trade has not expired
      elif trade['expires']:
        trade['expires'] -= 1
        newLimbo['buy'].append(trade)
      # trade never expires
      else:
        newLimbo['buy'].append(trade)

    for trade in self.limbo['short']:
      if trade['expires'] is 0:
        self.trade(self.working['ask'], trade['amount'])
        if self.good(self.working['ask'], trade['price'], trade['amount']):
          self.goodShorts += 1
      elif trade['expires']:
        trade['expires'] -= 1
        newLimbo['short'].append(trade)
      else:
        newLimbo['short'].append(trade)

    self.limbo = newLimbo
  
  # extend expired trades by 1 period
  # can choose all, buy, or short
  def extend(self, trans='all'):
    if trans is 'all' or trans is 'buy':
      for trade in self.limbo['buy']:
        if trade['expires'] is 0:
          trade ['expires'] = 1
    if trans is 'all' or trans is 'short':
      for trade in self.limbo['short']:
        if trade['expires'] is 0:
          trade ['expires'] = 1

  # strategy function
  def strategize(self):
    pass

  # run strategy
  def run(self, graph=True):
    for each in self.data:
      self.working = self.data[self.current]
      self.strategize()
      self.expire()
      self.calculate()
      self.current += 1

      #if self.current > 80:
      #  pprint(self.working)
      #  pprint(self.limbo)
      #  pprint(self.assets)
      #  raw_input()
    self.finish(graph)

  # calculate running metrics
  def calculate(self):
    # record maximum deficit (money outstanding)
    self.maxDeficit = max(self.maxDeficit, self.capital - self.running)

    # record current assets
    assets = self.running
    for trade in self.limbo['buy']:
      transaction = self.working['bid'] * trade['amount']
      fees = self.fees(trade['amount'])
      assets += (transaction - fees)
    for trade in self.limbo['short']:
      transaction = self.working['ask'] * trade['amount']
      fees = self.fees(trade['amount'])
      assets -= (transaction + fees)
    self.assets.append(assets)

    # record maximum running loss
    self.runningLoss = max(self.runningLoss, self.capital - assets)

  # calculate final metrics and graph
  def finish(self, graph):
    if graph:
      r.plot([asset / (self.capital if self.capital else self.maxDeficit) for asset in self.assets], type='l', xlab='Date', ylab='Profit Factor', main=self.stock + ' Profit over time')

  # return last n predictions (1, 0 or -1)
  def lastPrediction(self, n=1):
    predictions = []
    for i in range(1, n+1):
      predictions.append(self.data[self.current-i]['pred'])
    return predictions

  # amount of money in the market
  def outstanding(self):
    return self.capital - self.running

  # overall prediction
  def pred(self):
    return sum(self.working['pred'])
      
