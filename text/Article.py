from Sentence import Sentence
from Blog import Blog
import nltk

tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')

class Article(Blog):
    sentences = []
    glob_const = 0.5

    def __init__(self, title, date, text):
        super(Article, self).__init__(title, date, text)
        self.extract_sentences()

    def extract_sentences(self):
        sentences = tokenizer.tokenize(self.text)
        self.sentences = [Sentence(self, t) for t in sentences]
        for sent in self.sentences:
            sent.initialize(self.lower, self.stem, self.title)

    def extract_mentions(self, companies = None):
        super(Article, self).extract_mentions(companies)
        for sent in self.sentences:
            sent.extract_mentions(self.companies)

    def calculate_metrics(self):
        super(Article, self).calculate_metrics()
        for sent in self.sentences:
            sent.calculate_metrics()

    def process(self, subjmod = lambda x: 1.0, **args):
        if 'glob_const' in args: self.glob_const = args['glob_const']
        data = {}
        for sent in self.sentences:
            #print sent
            values = sent.process(subjmod)
            #print values
            for co in values:
                if co not in data: data[co] = values[co]
                else: data[co] = [x + y for x,y in zip(data[co], values[co])] # pairwise summation of data
        total = sum(self.mentions.values())
        for co in self.mentions:
            frac = float(self.mentions[co]) / total
            if co not in data: data[co] = [self.mentions[co], self.pos * frac * self.glob_const, self.neg * frac * self.glob_const, 1]
            else:
                data[co][0] = self.mentions[co]
                data[co][1] += self.pos * frac * self.glob_const
                data[co][2] += self.neg * frac * self.glob_const
                data[co][3] = 1
        return data
