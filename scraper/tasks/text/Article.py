from Sentence import Sentence
from Blog import Blog
import nltk

tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')

class Article(Blog):
    sentences = []

    def __init__(self, title, date, text, lower, stemmer, stem_titles):
        super(Article, self).__init__(title, date, text, lower, stemmer, stem_titles)
        self.extract_sentences()

    def extract_sentences(self):
        sentences = tokenizer.tokenize(self.text)
        self.sentences = [Sentence(self, t) for t in sentences]
        for sent in self.sentences:
            sent.initialize(self.lower, self.stemmer, self.stem_titles)

    def extract_mentions(self, companies = None):
        super(Article, self).extract_mentions(companies)
        for sent in self.sentences:
            sent.extract_mentions(self.companies)

    def calculate_metrics(self):
        super(Article, self).calculate_metrics()
        for sent in self.sentences:
            sent.calculate_metrics()

    def process(self, subj, pol):
        data = {}
        for sent in self.sentences:
            values = sent.process(subj, pol)
            for co in values:
                if co not in data:
                    data[co] = values[co]
                else: 
                    data[co]['tf'] += values['tf']
                    data[co]['df'] += values['df']
                    data[co]['positive'] += values['positive']
                    data[co]['negative'] += values['negative']
                    data[co]['subjectivity'] += values['subjectivity']

        for co in data:
            data[co]['contribution'] = float(data[co]['df']) / len(self.sententes)
            data[co]['df'] = 1

        return data
