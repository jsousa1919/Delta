from datetime import datetime
from Text import Text

class Blog(Text):
    def __init__(self, title, date, text, lower, stemmer, stem_titles):
        super(Blog, self).__init__(text, lower, stemmer, stem_titles)
        self.title = title
        self.date = date

        # extract datetime object if not done already
        if type(self.date) is not datetime:
            try:
                self.date = datetime.strptime(self.date, '%Y-%m-%d %H:%M:%S')
            except Exception:
                self.date = datetime.strptime(art[0], '%Y-%m-%d')

        self.initialize()

    def __str__(self):
        return self.title if self.title else self.raw_text
