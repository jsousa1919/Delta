from Strategy import Strategy

class MomentumStrategy(Strategy):
  
  def strategize(self):
    if self.pred() > 0:
      self.coverAll()
      if len(self.limbo['buy']) <= 1: self.extend('buy')
      self.buy(self.unit, self.lag)
    elif self.pred() < 0:
      self.sellAll()
      self.extend('short')
      self.short(self.unit, self.lag)
