import gdata.spreadsheet.service
import datetime

guser = "deltachi.finance"
gpass = "StevensIT"
spreadsheet_key = "tJlroookfgTzcr-az9IaC-w"

max_range = datetime.timedelta(730) # maximum number of days to collect from google at one time

class Collector(object):
  
  def __init__(self):
    self.google = gdata.spreadsheet.service.SpreadsheetsService()
    self.google.email = guser
    self.google.password = gpass
    self.google.source = 'Script'
    self.google.ProgrammaticLogin()

  def collect(self, symbol, start, end):
    data = dict()
    
    # delete excess rows
    #n = (end - start).days
    #rows = self.google.GetListFeed(spreadsheet_key, 'od6').entry
    #for i in range(n, len(rows)):
    #  self.google.DeleteRow(rows[i])

    while (start < end):
      # current range of dates to query
      td = min(end - start, max_range) 

      # get headers to change dates / symbol
      query = gdata.spreadsheet.service.CellQuery()
      query['min-col'] = '1'
      query['max-col'] = '3'
      query['min-row'] = query['max-row'] = '2'
      feed = self.google.GetCellsFeed(spreadsheet_key, 'od6', query=query)
      batchRequest = gdata.spreadsheet.SpreadsheetsCellsFeed() 

      # change headers
      feed.entry[0].cell.inputValue = symbol
      feed.entry[1].cell.inputValue = start.strftime("%m/%d/%Y")
      feed.entry[2].cell.inputValue = (start + td).strftime("%m/%d/%Y")
      batchRequest.AddUpdate(feed.entry[0])
      batchRequest.AddUpdate(feed.entry[1])
      batchRequest.AddUpdate(feed.entry[2])
      updated = self.google.ExecuteBatch(batchRequest, feed.GetBatchLink().href)

      # check that updates went through correctly (status_code = 200)
      # a status_code of 501 is a rate block
      if (updated.entry[0].batch_status.code != '200'):
        print "Updating spreadsheet failed, status code: %s" % updated.entry[0].batch_status.code
        break

      # retrieve spreadsheet as a list of rows
      feed = self.google.GetListFeed(spreadsheet_key, 'od6')

      # remove headers
      headers = feed.entry.pop(0)
      test = feed.entry.pop(0)

      # found data for this range
      if (test.custom['date'].text != '#N/A'):
        for line in feed.entry:
          try:
            metrics = dict()
            try:
              date = datetime.datetime.strptime(line.custom['date'].text, '%m/%d/%Y %H:%M:%S')
              date.replace(hour=0, minute=0, second=0)
            except ValueError:
              date = datetime.datetime.strptime(line.custom['date'].text, '%m/%d/%Y')
            metrics['open'] = float(line.custom['open'].text)
            metrics['close'] = float(line.custom['close'].text)
            metrics['high'] = float(line.custom['high'].text)
            metrics['low'] = float(line.custom['low'].text)
            metrics['volume'] = float(line.custom['volume'].text)
            data[date] = metrics
          except ValueError:
            break

      # increment start date
      start = start + td

    return data


