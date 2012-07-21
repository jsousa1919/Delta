import nltk
import re

with open('polarity/subjclueslen1polar.tff', 'r') as f:
    words = f.read().split('\n')

polarity = dict()
subjectivity = dict()
sent_mapping = {'strongneg': -2, 'weakneg': -1, 'neutral': 0, 'both': 0, 'weakpos': 1, 'strongpos': 2}
subj_mapping = {'weaksubj': 1, 'strongsubj': 2}

for each in words:
  prop_strings = each.split(' ')
  if len(prop_strings) < 7: continue
  props = [prop.split('=') for prop in prop_strings]
  props = dict([prop for prop in props if len(prop) is 2])
  polarity[props['word1']] = sent_mapping[props['mpqapolarity']]
  subjectivity[props['word1']] = subj_mapping[props['type']]

wnl = nltk.stem.WordNetLemmatizer()

class Text(object):
    removals = []

    def __init__(self, text, lower, stemmer, stem_titles):
        self.raw_text = text
        self.initialize(lower, stemmer, stem_titles)

    # find all words in the text
    def make_list(self):
        self.word_list = re.findall('\w+', self.text)
    
    # remove stopwords and possessives
    def remove_stopwords(self):
        self.clean_list = [re.sub("'s$", "", word) for word in self.word_list if word.lower() not in nltk.corpus.stopwords.words('english')]

    # remove a specified set of words
    def remove_words(self, remove = None):
        if remove is None:
            remove = self.removals
        if len(remove) > 0:
            self.clean_list = [word for word in self.clean_list if word.lower() not in remove]
        if remove is not self.removals:
            self.removals = list(set(self.removals.extend(remove)))

    # perform word stemming
    # set 'stem_titles' to stem title words since the stemmer doesn't like them (i.e. Apples)
    def stem(self):
        if self.stemmer is 'wnl':
            if self.lower:
                self.clean_list = [wnl.lemmatize(word) for word in self.clean_list]
            else:
                # wnl doesn't lemmatize with uppercase characters
                i = 0
                for word in self.clean_list:
                    if self.stem_titles and word.istitle():
                        self.clean_list[i] = wnl.lemmatize(word.lower()).title()
                    else:
                        self.clean_list[i] = wnl.lemmatize(word)
                    i += 1

    # create frequency distribution from words in text
    def local_freq_dist(self):
        self.freq_dist = nltk.FreqDist(self.clean_list)

    # create frequency distributuion from all words given
    def global_freq_dist(self, words):
        return [self.freq_dist[word] for word in words]

    # return list of unique words in text
    def words(self):
        return self.freq_dist.keys()

    def initialize(self, lower = None, stemmer = None, stem_titles = None):
        if lower is not None:
            self.lower = lower
        if stemmer is 'wnl':
            self.stemmer = wnl
        if stem_titles is not None:
            self.stem_titles = stem_titles
        self.text = self.raw_text.lower() if lower else self.raw_text
        self.make_list()
        self.remove_stopwords()
        self.remove_words()
        if stemmer:
            self.stem()
        self.local_freq_dist()

    # extract number of mentions for each company (symbol, name, keywords, etc...)
    def extract_mentions(self, companies = None):
        self.companies = companies if companies else self.companies
        self.mentions = nltk.FreqDist()
        for co in self.companies:
            for keyword in co.keywords:
                self.mentions.inc(co, self.text.count(keyword if not self.lower else keyword.lower()))

    # company with most mentions
    def top_mention(self):
        return self.mentions.max()

    def mentions(self):
        return self.mentions.keys()

    # return the most mentioned company with the number of mentions higher than cutoff / 1 
    # if no companies make the cutoff, return False
    def paradigm(self, cutoff = 1):
        highest = self.top_mention()
        if self.mentions.freq(highest) > cutoff:
            return highest
        else:
            return False

    def calculate_metrics(self):
        self.subj = self.pos = self.neg = 0
        for word in self.freq_dist:
            word = word.lower()
            if word in subjectivity:
                self.subj += self.freq_dist[word] * subjectivity[word]
                if polarity[word] > 0:
                    self.pos += self.freq_dist[word] * polarity[word]
                elif polarity[word] < 0:
                    self.neg -= self.freq_dist[word] * polarity[word]

    def process(self):
        self.extract_mentions()
        self.calculate_metrics()
        data = {}
        total = sum(self.mentions.values())
        for co in self.mentions:
            count = self.mentions[co]
            frac = float(count) / total
            data[co] = {
                'contribution': frac,
                'tf': count,
                'df': 1,
                'positive': self.pos,
                'negative': self.neg,
                'subjectivity': self.subj,
            }
        return data

    def sentiment(self):
        return self.pos - self.neg
