from Text import Text

class Sentence(Text):
    def __init__(self, article, text, lower, stemmer, stem_titles):
        super(Sentence, self).__init__(text, lower, stemmer, stem_titles)
        self.article = article
