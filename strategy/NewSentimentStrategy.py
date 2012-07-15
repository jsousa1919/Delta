from Strategy import Strategy

class NewSentimentStrategy(Strategy):
  
  def strategize(self):
    if self.pred() > 0:
      self.buy((self.working['newSent'] + 1) * self.unit, self.lag)
    elif self.pred() < 0:
      self.short((self.working['newSent'] + 1) * self.unit, self.lag)
