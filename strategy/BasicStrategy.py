from Strategy import Strategy

class BasicStrategy(Strategy):
  
  def strategize(self):
    if self.pred() > 0:
      self.buy(self.unit, self.lag)
    elif self.pred() < 0:
      self.short(self.unit, self.lag)
