from Strategy import Strategy

class WeightedStrategy(Strategy):
  
  def __init__(self, **kw):
    Strategy.__init__(self, **kw)
    self.preds = []

  def strategize(self):
    # add new data about correct predictions 
    if self.current > 0:
      self.preds.append(self.lastCorrectness())
      if len(self.preds) > 7:
        self.preds.pop(0)

    # trade
    if self.pred() > 0:
      self.buy(self.unit, self.lag)
    elif self.pred() < 0:
      self.short(self.unit, self.lag)

  # list of prediction * weight (correctness) for each network
  def pred(self):
    return sum([pred * weight for pred,weight in zip(self.working['pred'], self.weights())])

  # list of number of times correct for each network
  def weights(self):
    weights = [1] * len(self.working['pred'])
    for pred in self.preds:
      weights = [a + b for a,b in zip(weights, pred)]

    return [x - min(weights) for x in weights]
    # return weights
    
  # list of correctness of prediction for each network
  def lastCorrectness(self):
    lastAsk, lastBid = self.data[self.current-1]['ask'], self.data[self.current-1]['bid']
    newAsk, newBid = self.working['ask'], self.working['bid']
    preds = self.lastPrediction()[0]

    correct = []
    for pred in preds:
      if pred > 0:
        correct.append(int(self.good(lastAsk, newBid, self.unit)))
      elif pred < 0:
        correct.append(int(self.good(newAsk, lastBid, self.unit)))
      else:
        correct.append(int(not (self.good(lastAsk, newBid, self.unit) or self.good(newAsk, lastBid, self.unit))))

    return correct
