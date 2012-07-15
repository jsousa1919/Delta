import datetime
import urllib
from csv import DictReader

history_url = "http://ichart.finance.yahoo.com/table.csv?s=%s&a=%d&b=%d&c=%d&d=%d&e=%d&f=%d&g=d&ignore=.csv"

class Collector(object):
  
  def __init__(self):
    pass

  def collect(self, symbol, start, end):
    data = dict()
    url = history_url % (symbol, start.month - 1, start.day, start.year, end.month - 1, end.day, end.year)

    try:    
      fp = urllib.urlopen(url)
    except IOError:
      print "Couldn't open url: %s" % url
      return

    text = fp.read()
    if text.startswith("Date"):
      yahoo = DictReader(text.split('\n'))
    
      for row in yahoo:
        metrics = dict()
        date = datetime.datetime.strptime(row['Date'], '%Y-%m-%d')
        metrics['open'] = float(row['Open'])
        metrics['close'] = float(row['Close'])
        metrics['high'] = float(row['High'])
        metrics['low'] = float(row['Low'])
        metrics['volume'] = float(row['Volume'])
        data[date] = metrics

    return data
