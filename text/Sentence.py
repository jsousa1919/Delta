from Text import Text

class Sentence(Text):
    def __init__(self, blog, text):
        super(Sentence, self).__init__(text)
        self.blog = blog
